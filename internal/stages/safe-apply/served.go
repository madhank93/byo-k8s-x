package safeapply

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	admission "k8s.io/api/admission/v1"
	admregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/cert"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// serveTLS serves h over HTTPS on this machine, where the API server can reach
// it, and returns the port and the CA bundle that verifies it. The certificate
// is also valid for names, which is how the aggregator addresses a Service.
func serveTLS(h http.Handler, names ...string) (int, []byte, func(), error) {
	certPEM, keyPEM, err := cert.GenerateSelfSignedCertKey(stages.ExternalHost(), nil, names)
	if err != nil {
		return 0, nil, nil, err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return 0, nil, nil, err
	}
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return 0, nil, nil, err
	}
	srv := &http.Server{Handler: h, TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}},
		ErrorLog: log.New(io.Discard, "", 0)}
	go srv.ServeTLS(l, "", "")
	return l.Addr().(*net.TCPAddr).Port, certPEM, func() { srv.Close() }, nil
}

// unique is a suffix no concurrent run shares, for cluster-scoped names.
func unique() string { return strconv.FormatInt(time.Now().UnixNano(), 36) }

var apiServices = schema.GroupVersionResource{Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"}

// unlistableAPI registers an aggregated API whose one kind, widgets, is in
// discovery but answers 503 to a list by set label, and returns its group.
// Other lists get an empty answer so the namespace controller is not held up.
// A registered API with no backend stalls every namespace deletion on the
// cluster, so stop removes the APIService before the server.
func unlistableAPI(ctx context.Context, env *kube.Env) (string, func(), error) {
	group := "unlistable-" + unique() + ".byok8s.dev"
	gv := group + "/v1"
	port, ca, stopServer, err := serveTLS(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/apis/"+gv:
			json.NewEncoder(rw).Encode(metav1.APIResourceList{
				TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
				GroupVersion: gv,
				APIResources: []metav1.APIResource{{Name: "widgets", SingularName: "widget", Namespaced: true, Kind: "Widget", Verbs: []string{"list", "delete"}}},
			})
		case strings.Contains(r.URL.Query().Get("labelSelector"), setLabel):
			rw.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(rw).Encode(apierrors.NewServiceUnavailable("widgets are down").Status())
		case strings.HasSuffix(r.URL.Path, "/widgets"):
			fmt.Fprintf(rw, `{"apiVersion":%q,"kind":"WidgetList","metadata":{},"items":[]}`, gv)
		default:
			rw.WriteHeader(http.StatusNotFound)
		}
	}), "unlistable."+env.Namespace+".svc")
	if err != nil {
		return "", nil, err
	}
	dyn, err := dynamic.NewForConfig(env.Config)
	if err != nil {
		stopServer()
		return "", nil, err
	}
	name := "v1." + group
	var once sync.Once
	stop := func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancel()
			dyn.Resource(apiServices).Delete(ctx, name, metav1.DeleteOptions{})
			kube.WaitFor(ctx, "APIService "+name+" to leave discovery", func(ctx context.Context) (bool, error) {
				_, err := dyn.Resource(apiServices).Get(ctx, name, metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					return false, nil
				}
				lists, _ := env.Client.Discovery().ServerPreferredNamespacedResources()
				for _, l := range lists {
					if l.GroupVersion == gv {
						return false, nil
					}
				}
				return true, nil
			})
			stopServer()
		})
	}
	// The aggregator reaches a Service, so an ExternalName one points it here.
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "unlistable"},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeExternalName, ExternalName: stages.ExternalHost(),
			Ports: []corev1.ServicePort{{Port: int32(port)}}},
	}
	if _, err := env.Client.CoreV1().Services(env.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		stopServer()
		return "", nil, err
	}
	as := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1", "kind": "APIService",
		"metadata": map[string]any{"name": name, "labels": map[string]any{kube.GraderAPILabel: "safe-apply-unlistable"}},
		"spec": map[string]any{
			"group": group, "version": "v1", "groupPriorityMinimum": int64(1000), "versionPriority": int64(15),
			"caBundle": base64.StdEncoding.EncodeToString(ca),
			"service":  map[string]any{"namespace": env.Namespace, "name": "unlistable", "port": int64(port)},
		},
	}}
	if _, err := dyn.Resource(apiServices).Create(ctx, as, metav1.CreateOptions{}); err != nil {
		stopServer()
		return "", nil, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	err = kube.WaitFor(waitCtx, "discovery to serve "+gv, func(ctx context.Context) (bool, error) {
		lists, _ := env.Client.Discovery().ServerPreferredNamespacedResources()
		for _, l := range lists {
			if l.GroupVersion == gv {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		stop()
		return "", nil, err
	}
	return group, stop, nil
}

// intercept registers a validating webhook on the stage namespace's ConfigMaps
// that, during the first real (not dry-run) op on ConfigMap name, runs act
// and then admits the request: a change that lands while the learner is
// mid-write, after any check it made up front. fired reports act's outcome,
// or that it never ran. failurePolicy Ignore keeps a dead server harmless.
func intercept(ctx context.Context, env *kube.Env, op admission.Operation, name string, act func() error) (fired func() error, stop func(), err error) {
	probe := "webhook-probe-" + unique()
	var (
		mu     sync.Mutex
		ran    bool
		actErr error
		probed bool
	)
	port, ca, stopServer, err := serveTLS(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var review admission.AdmissionReview
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil || review.Request == nil {
			http.Error(rw, "not an AdmissionReview", http.StatusBadRequest)
			return
		}
		req := review.Request
		mu.Lock()
		first := req.Name == name && req.Operation == op && (req.DryRun == nil || !*req.DryRun) && !ran
		ran = ran || first
		probed = probed || req.Name == probe
		mu.Unlock()
		// act writes to this namespace too, so it runs unlocked: its own
		// admission comes back through this handler.
		if first {
			err := act()
			mu.Lock()
			actErr = err
			mu.Unlock()
		}
		review.Response = &admission.AdmissionResponse{UID: req.UID, Allowed: true}
		review.Request = nil
		json.NewEncoder(rw).Encode(review)
	}))
	if err != nil {
		return nil, nil, err
	}
	cfgName := "safe-apply-" + unique()
	url := fmt.Sprintf("https://%s/", net.JoinHostPort(stages.ExternalHost(), strconv.Itoa(port)))
	ignore := admregv1.Ignore
	sideEffects := admregv1.SideEffectClassNoneOnDryRun
	timeout := int32(10)
	adm := env.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	_, err = adm.Create(ctx, &admregv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: cfgName},
		Webhooks: []admregv1.ValidatingWebhook{{
			Name:                    "intercept.safe-apply.byok8s.dev",
			ClientConfig:            admregv1.WebhookClientConfig{URL: &url, CABundle: ca},
			Rules:                   []admregv1.RuleWithOperations{{Operations: []admregv1.OperationType{admregv1.Create, admregv1.Update, admregv1.Delete}, Rule: admregv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"configmaps"}}}},
			NamespaceSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": env.Namespace}},
			FailurePolicy:           &ignore,
			SideEffects:             &sideEffects,
			TimeoutSeconds:          &timeout,
			AdmissionReviewVersions: []string{"v1"},
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		stopServer()
		return nil, nil, err
	}
	stop = func() {
		adm.Delete(context.WithoutCancel(ctx), cfgName, metav1.DeleteOptions{})
		stopServer()
	}
	// The API server picks a new webhook up asynchronously; a dry-run create
	// shows when it is calling this one.
	waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	err = kube.WaitFor(waitCtx, "the API server to call the grader's webhook", func(ctx context.Context) (bool, error) {
		env.Client.CoreV1().ConfigMaps(env.Namespace).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: probe}},
			metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		mu.Lock()
		defer mu.Unlock()
		return probed, nil
	})
	if err != nil {
		stop()
		return nil, nil, err
	}
	fired = func() error {
		mu.Lock()
		defer mu.Unlock()
		if !ran {
			return fmt.Errorf("the program never wrote ConfigMap/%s (%s), so the change meant to land mid-apply never did", name, op)
		}
		return actErr
	}
	return fired, stop, nil
}
