package hpa

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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/cert"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// fakeMetrics is the grader's own metrics pipeline for the autoscaling
// stages: metrics.k8s.io, custom.metrics.k8s.io and external.metrics.k8s.io,
// served from numbers a stage sets, so every replica count it expects is
// arithmetic rather than a guess about load. All values are milli-units.
type fakeMetrics struct {
	env *kube.Env

	mu sync.Mutex
	// cpu answers for one running pod among n that match the request's
	// selector; false leaves the pod out, as a scrape that missed it would.
	cpu func(pod string, n int) (int64, bool)
	// podMetric is the same for a custom per-pod metric, by metric name.
	podMetric map[string]func(pod string) (int64, bool)
	// object is a custom metric describing one object, by "resource/name/metric".
	object map[string]int64
	// external answers a metric name and the selector the client sent.
	external func(metric string, sel labels.Selector) (int64, bool)
}

func (f *fakeMetrics) setCPU(fn func(pod string, n int) (int64, bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cpu = fn
}

func (f *fakeMetrics) setPodMetric(name string, fn func(pod string) (int64, bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.podMetric[name] = fn
}

func (f *fakeMetrics) setObject(key string, v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.object[key] = v
}

func (f *fakeMetrics) setExternal(fn func(metric string, sel labels.Selector) (int64, bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.external = fn
}

// each returns the same value for every pod.
func each(v int64) func(string, int) (int64, bool) {
	return func(string, int) (int64, bool) { return v, true }
}

var fakeGroups = []struct{ group, version string }{
	{"metrics.k8s.io", "v1beta1"},
	{"custom.metrics.k8s.io", "v1beta2"},
	{"external.metrics.k8s.io", "v1beta1"},
}

// startFakeMetrics serves the three metrics APIs and registers them with the
// aggregator, returning once all three are Available. stop removes the
// registrations before the server, so none is ever left unanswered.
func startFakeMetrics(ctx context.Context, env *kube.Env) (*fakeMetrics, func(), error) {
	f := &fakeMetrics{env: env, podMetric: map[string]func(string) (int64, bool){}, object: map[string]int64{}}
	for _, g := range fakeGroups {
		if err := removeAPIService(ctx, env, g.version+"."+g.group); err != nil {
			return nil, nil, err
		}
	}
	const svc = "grader-metrics"
	port, ca, stopServer, err := serveTLS(f, svc+"."+env.Namespace+".svc")
	if err != nil {
		return nil, nil, err
	}
	dyn, err := dynamic.NewForConfig(env.Config)
	if err != nil {
		stopServer()
		return nil, nil, err
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			for _, g := range fakeGroups {
				removeAPIService(context.WithoutCancel(ctx), env, g.version+"."+g.group)
			}
			stopServer()
		})
	}
	// The aggregator reaches a Service, so an ExternalName one points it here.
	_, err = env.Client.CoreV1().Services(env.Namespace).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svc},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeExternalName, ExternalName: stages.ExternalHost(),
			Ports: []corev1.ServicePort{{Port: int32(port)}}},
	}, metav1.CreateOptions{})
	if err != nil {
		stop()
		return nil, nil, err
	}
	for _, g := range fakeGroups {
		as := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiregistration.k8s.io/v1", "kind": "APIService",
			"metadata": map[string]any{"name": g.version + "." + g.group, "labels": map[string]any{kube.GraderAPILabel: "hpa"}},
			"spec": map[string]any{
				"group": g.group, "version": g.version, "groupPriorityMinimum": int64(100), "versionPriority": int64(100),
				"caBundle": base64.StdEncoding.EncodeToString(ca),
				"service":  map[string]any{"namespace": env.Namespace, "name": svc, "port": int64(port)},
			},
		}}
		if _, err := dyn.Resource(apiServicesGVR).Create(ctx, as, metav1.CreateOptions{}); err != nil {
			stop()
			return nil, nil, err
		}
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for _, g := range fakeGroups {
		name := g.version + "." + g.group
		if err := kube.WaitFor(waitCtx, "the grader's "+name+" to be Available", func(ctx context.Context) (bool, error) {
			as, err := apiService(ctx, env, name)
			if err != nil {
				return false, nil
			}
			ok, _ := available(as)
			return ok, nil
		}); err != nil {
			stop()
			return nil, nil, err
		}
	}
	return f, stop, nil
}

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

func (f *fakeMetrics) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "apis" {
		http.NotFound(rw, r)
		return
	}
	gv, rest := parts[1]+"/"+parts[2], parts[3:]
	sel, err := labels.Parse(r.URL.Query().Get("labelSelector"))
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	var body any
	switch {
	case len(rest) == 0:
		body = discovery(gv)
	case gv == "metrics.k8s.io/v1beta1" && len(rest) == 1 && rest[0] == "nodes":
		body = map[string]any{"kind": "NodeMetricsList", "apiVersion": gv, "metadata": map[string]any{}, "items": []any{}}
	case gv == "metrics.k8s.io/v1beta1" && len(rest) >= 3 && rest[0] == "namespaces" && rest[2] == "pods":
		body, err = f.podMetrics(r.Context(), rest[1], sel, rest[3:])
	case gv == "custom.metrics.k8s.io/v1beta2" && len(rest) == 5 && rest[0] == "namespaces" && rest[2] == "pods" && rest[3] == "*":
		body, err = f.customPods(r.Context(), rest[1], rest[4], sel)
	case gv == "custom.metrics.k8s.io/v1beta2" && len(rest) == 5 && rest[0] == "namespaces":
		body, err = f.customObject(rest[1], rest[2], rest[3], rest[4])
	case gv == "external.metrics.k8s.io/v1beta1" && len(rest) == 3 && rest[0] == "namespaces":
		body, err = f.externalMetric(rest[2], sel)
	default:
		err = errNotFound
	}
	if err != nil {
		code := http.StatusInternalServerError
		if err == errNotFound {
			code = http.StatusNotFound
		}
		rw.WriteHeader(code)
		json.NewEncoder(rw).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
			Status: metav1.StatusFailure, Code: int32(code), Reason: metav1.StatusReasonNotFound, Message: "no such metric"})
		return
	}
	json.NewEncoder(rw).Encode(body)
}

var errNotFound = fmt.Errorf("not found")

func discovery(gv string) metav1.APIResourceList {
	l := metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: gv}
	get := []string{"get", "list"}
	switch gv {
	case "metrics.k8s.io/v1beta1":
		l.APIResources = []metav1.APIResource{{Name: "pods", Namespaced: true, Kind: "PodMetrics", Verbs: get}, {Name: "nodes", Kind: "NodeMetrics", Verbs: get}}
	case "custom.metrics.k8s.io/v1beta2":
		l.APIResources = []metav1.APIResource{{Name: "pods/requests_per_second", Namespaced: true, Kind: "MetricValueList", Verbs: []string{"get"}},
			{Name: "services/hits", Namespaced: true, Kind: "MetricValueList", Verbs: []string{"get"}}}
	case "external.metrics.k8s.io/v1beta1":
		l.APIResources = []metav1.APIResource{{Name: "queue_depth", Namespaced: true, Kind: "ExternalMetricValueList", Verbs: []string{"get"}}}
	}
	return l
}

// running lists the pods a real scrape could have seen: running, not going.
func (f *fakeMetrics) running(ctx context.Context, ns string, sel labels.Selector) ([]corev1.Pod, error) {
	list, err := f.env.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range list.Items {
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
			out = append(out, p)
		}
	}
	return out, nil
}

func milli(v int64) string { return resource.NewMilliQuantity(v, resource.DecimalSI).String() }

func (f *fakeMetrics) podMetrics(ctx context.Context, ns string, sel labels.Selector, name []string) (any, error) {
	pods, err := f.running(ctx, ns, sel)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	cpu := f.cpu
	f.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339)
	var items []any
	for _, p := range pods {
		if cpu == nil {
			break
		}
		v, ok := cpu(p.Name, len(pods))
		if !ok || (len(name) == 1 && name[0] != p.Name) {
			continue
		}
		items = append(items, map[string]any{
			"kind": "PodMetrics", "apiVersion": "metrics.k8s.io/v1beta1",
			"metadata":  map[string]any{"name": p.Name, "namespace": ns, "labels": p.Labels},
			"timestamp": now, "window": "1s",
			"containers": []any{map[string]any{"name": p.Spec.Containers[0].Name,
				"usage": map[string]any{"cpu": milli(v), "memory": "1Mi"}}},
		})
	}
	if len(name) == 1 {
		if len(items) == 0 {
			return nil, errNotFound
		}
		return items[0], nil
	}
	if items == nil {
		items = []any{}
	}
	return map[string]any{"kind": "PodMetricsList", "apiVersion": "metrics.k8s.io/v1beta1", "metadata": map[string]any{}, "items": items}, nil
}

func (f *fakeMetrics) customPods(ctx context.Context, ns, metric string, sel labels.Selector) (any, error) {
	f.mu.Lock()
	fn := f.podMetric[metric]
	f.mu.Unlock()
	if fn == nil {
		return nil, errNotFound
	}
	pods, err := f.running(ctx, ns, sel)
	if err != nil {
		return nil, err
	}
	items := []any{}
	for _, p := range pods {
		if v, ok := fn(p.Name); ok {
			items = append(items, metricValue("Pod", ns, p.Name, metric, v))
		}
	}
	return map[string]any{"kind": "MetricValueList", "apiVersion": "custom.metrics.k8s.io/v1beta2", "metadata": map[string]any{}, "items": items}, nil
}

func (f *fakeMetrics) customObject(ns, res, name, metric string) (any, error) {
	f.mu.Lock()
	v, ok := f.object[res+"/"+name+"/"+metric]
	f.mu.Unlock()
	if !ok {
		return nil, errNotFound
	}
	kind := strings.TrimSuffix(strings.ToUpper(res[:1])+res[1:], "s")
	return map[string]any{"kind": "MetricValueList", "apiVersion": "custom.metrics.k8s.io/v1beta2", "metadata": map[string]any{},
		"items": []any{metricValue(kind, ns, name, metric, v)}}, nil
}

func metricValue(kind, ns, name, metric string, v int64) map[string]any {
	return map[string]any{
		"describedObject": map[string]any{"kind": kind, "namespace": ns, "name": name, "apiVersion": "/v1"},
		"metric":          map[string]any{"name": metric},
		"timestamp":       time.Now().UTC().Format(time.RFC3339), "windowSeconds": 1, "value": milli(v),
	}
}

func (f *fakeMetrics) externalMetric(metric string, sel labels.Selector) (any, error) {
	f.mu.Lock()
	fn := f.external
	f.mu.Unlock()
	if fn == nil {
		return nil, errNotFound
	}
	v, ok := fn(metric, sel)
	if !ok {
		return nil, errNotFound
	}
	return map[string]any{"kind": "ExternalMetricValueList", "apiVersion": "external.metrics.k8s.io/v1beta1", "metadata": map[string]any{},
		"items": []any{map[string]any{"metricName": metric, "metricLabels": map[string]any{},
			"timestamp": time.Now().UTC().Format(time.RFC3339), "value": milli(v)}}}, nil
}
