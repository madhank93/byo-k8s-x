// Your admission webhook.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// The name of the configuration this program owns. One name means a restart
// updates its own registration rather than accumulating a second one.
const webhookConfigName = "byok8s-webhook"

// A rotation has to be observable inside one run of the course, so the leaf is
// far shorter-lived than a real one. Production intervals are hours or days;
// the shape of the reload is what matters, not the numbers.
const (
	leafLifetime = 2 * time.Minute
	rotateEvery  = 15 * time.Second
)

// The mark this webhook leaves, and the same key as a JSON Pointer: RFC 6901
// gives `/` meaning inside a path, so it is escaped as `~1` there.
const (
	injectedAnnotation = "byok8s.dev/injected"
	injectedPointer    = "/metadata/annotations/byok8s.dev~1injected"
)

// The floor handed to a container that asked for no CPU of its own.
const defaultCPURequest = "100m"

// Where this webhook writes down what it decided.
const verdictConfigMap = "byok8s-admissions"

// A pod may ask to be judged slowly. Nothing in production would offer this;
// it exists so the caller's deadline can be watched from outside the program.
const delayAnnotation = "byok8s.dev/delay"

// Injection is opt-in: a workload asks for the sidecar by annotation, because a
// webhook that injects into everything eventually injects into something that
// cannot survive it.
const (
	injectAnnotation = "byok8s.dev/inject"
	sidecarName      = "sidecar"
	sidecarImage     = "registry.k8s.io/pause:3.9"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	// The API server calls this program, so it runs until something asks it to
	// stop. SIGTERM is what Kubernetes sends a pod it is deleting.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	addr := flag.String("addr", "0.0.0.0:8443", "address to serve the webhook on")
	externalHost := flag.String("external-host", "host.docker.internal", "the name the API server reaches this program by")
	flag.Parse()

	ca, caKey, caDER, err := certAuthority(*externalHost)
	if err != nil {
		return err
	}
	leaf, err := leafFor(*externalHost, ca, caKey)
	if err != nil {
		return err
	}
	// Read per handshake rather than once at startup: tls.Config.Certificates
	// is a fixed slice, and swapping it under a running server is a data race
	// that open connections would not notice anyway.
	var current atomic.Pointer[tls.Certificate]
	current.Store(leaf)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/mutate", mutate)

	srv := &http.Server{
		Addr:    *addr,
		Handler: mux,
		TLSConfig: &tls.Config{
			GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
				return current.Load(), nil
			},
		},
		// A client that opens a connection and then says nothing otherwise
		// holds it open for as long as it likes.
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		fmt.Println("shutting down")
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	// Rotating from the same CA keeps both halves valid at once: only the leaf
	// changes, so the caBundle registered below never has to. Rotating on a
	// timer rather than per request keeps the cost off the admission path.
	go func() {
		t := time.NewTicker(rotateEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				next, err := leafFor(*externalHost, ca, caKey)
				if err != nil {
					fmt.Fprintf(os.Stderr, "rotate certificate: %v\n", err)
					continue
				}
				current.Store(next)
				// A rotation nobody can see is one nobody can correlate with
				// the outage that followed it.
				fmt.Printf("rotated certificate serial=%s expires=%s\n", next.Leaf.SerialNumber, next.Leaf.NotAfter.Format(time.RFC3339))
			}
		}
	}()

	// The API server reaches this program by URL rather than by Service,
	// because it runs on the host and not in the cluster: the address it dials
	// is the external name with the port this process actually listens on.
	cfg, ns, err := clientConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("build clientset: %w", err)
	}

	// Wired here rather than above because recording a verdict needs a client,
	// and there is no client until now.
	mux.HandleFunc("/validate", func(w http.ResponseWriter, r *http.Request) {
		validate(w, r, cs, ns)
	})

	_, port, err := net.SplitHostPort(*addr)
	if err != nil {
		return fmt.Errorf("read the port out of %q: %w", *addr, err)
	}
	url := fmt.Sprintf("https://%s/validate", net.JoinHostPort(*externalHost, port))
	mutateURL := fmt.Sprintf("https://%s/mutate", net.JoinHostPort(*externalHost, port))

	// The caBundle is parsed as PEM, not as the DER the certificate was built
	// from: raw bytes there fail inside the API server, not at registration.
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	if err := register(ctx, cs, url, caPEM, ns); err != nil {
		return err
	}
	if err := registerMutating(ctx, cs, mutateURL, caPEM, ns); err != nil {
		return err
	}
	// Installed, and deliberately not removed on the way out: a policy that
	// disappeared when this program stopped would have none of the property
	// that makes it worth writing.
	if err := installPolicy(ctx, cs, ns); err != nil {
		return err
	}
	fmt.Printf("installed policy %s\n", policyObjectName)
	defer func() {
		if err := unregister(cs); err != nil {
			fmt.Fprintf(os.Stderr, "unregister: %v\n", err)
		}
	}()
	fmt.Printf("registered %s -> %s\n", webhookConfigName, url)

	fmt.Printf("serving on %s\n", *addr)
	// The certificate and key are already in TLSConfig, so this takes neither.
	if err := srv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// installPolicy writes the owner rule as a ValidatingAdmissionPolicy: the same
// verdict as decide(), reached without a network hop, a certificate, or a
// program that can be down.
//
// The policy is cluster-scoped and the binding is what applies it. Every
// narrowing the webhook carries is repeated here, because both are live at
// once: a policy that judged what the webhook deliberately skips would refuse
// objects this program is supposed to let past.
func installPolicy(ctx context.Context, cs kubernetes.Interface, namespace string) error {
	scope := admissionregistrationv1.NamespacedScope
	fail := admissionregistrationv1.Fail

	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: policyObjectName},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: &fail,
			MatchConstraints: &admissionregistrationv1.MatchResources{
				ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
					RuleWithOperations: admissionregistrationv1.RuleWithOperations{
						Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
						Rule: admissionregistrationv1.Rule{
							APIGroups:   []string{""},
							APIVersions: []string{"v1"},
							Resources:   []string{"pods"},
							Scope:       &scope,
						},
					},
				}},
			},
			// The exemption from stage 18, in the one place CEL can see the
			// caller. A match condition on the policy is checked before the
			// validations, exactly as it is on the webhook.
			MatchConditions: []admissionregistrationv1.MatchCondition{{
				Name:       "exclude-exempt-writer",
				Expression: fmt.Sprintf("request.userInfo.username != %q", exemptUser),
			}},
			Validations: []admissionregistrationv1.Validation{{
				Expression: "has(object.metadata.labels) && 'owner' in object.metadata.labels",
				Message:    "every pod must carry an owner label",
			}},
		},
	}

	policies := cs.AdmissionregistrationV1().ValidatingAdmissionPolicies()
	_, err := policies.Create(ctx, policy, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := policies.Get(ctx, policyObjectName, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("read existing policy %s: %w", policyObjectName, getErr)
		}
		policy.ResourceVersion = existing.ResourceVersion
		_, err = policies.Update(ctx, policy, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("install policy: %w", err)
	}

	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: policyObjectName},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        policyObjectName,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
			// Without this the policy denies matching pods in every namespace
			// in the cluster. A binding is the blast radius.
			MatchResources: &admissionregistrationv1.MatchResources{
				NamespaceSelector: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key:      "kubernetes.io/metadata.name",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{namespace},
					}},
				},
				// The same opt-out the webhook honours, for the same reason.
				ObjectSelector: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key:      "byok8s.dev/skip",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					}},
				},
			},
		},
	}

	bindings := cs.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings()
	_, err = bindings.Create(ctx, binding, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := bindings.Get(ctx, policyObjectName, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("read existing binding %s: %w", policyObjectName, getErr)
		}
		binding.ResourceVersion = existing.ResourceVersion
		_, err = bindings.Update(ctx, binding, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("bind policy: %w", err)
	}
	return nil
}

// validate answers an AdmissionReview with an AdmissionReview.
//
// The wrapper is the protocol: the same kind goes back, carrying the request's
// uid and a verdict. A reply that loses the uid is discarded — the API server
// cannot tell which request it answers.
func validate(w http.ResponseWriter, r *http.Request, cs kubernetes.Interface, ns string) {
	// A panic would close the connection with no verdict in it, which reads to
	// the API server as an unreachable webhook rather than as a bug here.
	var uid types.UID
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Fprintf(os.Stderr, "panic while admitting: %v\n", rec)
			respond(w, uid, false, "the webhook failed while judging this object")
		}
	}()

	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, fmt.Sprintf("decode AdmissionReview: %v", err), http.StatusBadRequest)
		return
	}
	if review.Request == nil {
		http.Error(w, "AdmissionReview carried no request", http.StatusBadRequest)
		return
	}
	uid = review.Request.UID

	// A CREATE request may carry no name of its own — the name lives in the
	// object — so metadata is read from there.
	var meta metav1.PartialObjectMetadata
	_ = json.Unmarshal(review.Request.Object.Raw, &meta)
	name := review.Request.Name
	if name == "" {
		name = meta.Name
	}
	fmt.Printf("review %s %s/%s\n", review.Request.Operation, review.Request.Resource.Resource, name)

	// The deadline belongs to the caller, so the wait watches the caller's
	// context rather than a timer of this program's own. That is the difference
	// between a handler that stops when its answer stopped being wanted and one
	// that keeps working on a question nobody is listening to.
	if !waited(r.Context(), meta.Annotations, name) {
		return
	}

	allowed, message := decide(review.Request, name)

	respondAudited(w, uid, allowed, message, auditFor(review.Request, allowed), warningsFor(meta.Annotations))

	// Without this the reply sits in the server's buffer until the handler
	// returns, and "answer, then work" would still bill the caller for the work.
	if err := http.NewResponseController(w).Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush reply for %s: %v\n", name, err)
	}

	// Answer first, then work: the record is not on the path the caller waits
	// on. It deliberately outlives the request's context, because the reply has
	// already gone and the verdict still has to be written down.
	//
	// The verdict a hypothetical request gets is provably the one a real request
	// would have got, because the dry run only skips what happens after it.
	if !dryRun(review.Request) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer cancel()
		if err := recordVerdict(ctx, cs, ns, name, allowed); err != nil {
			fmt.Fprintf(os.Stderr, "record verdict for %s: %v\n", name, err)
		}
	}
}

// waited honours a pod that asked to be judged slowly, and reports whether the
// caller was still there at the end of it.
//
// Waiting on the context rather than on the clock is the whole lesson: when the
// API server gives up, this returns immediately instead of holding a connection
// nobody is reading.
func waited(ctx context.Context, annotations map[string]string, name string) bool {
	d, err := time.ParseDuration(annotations[delayAnnotation])
	if err != nil || d <= 0 {
		return true
	}

	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		fmt.Printf("gave up on %s: the API server stopped waiting (%v)\n", name, ctx.Err())
		return false
	}
}

// The one caller this policy does not judge. Real clusters name a controller's
// service account here; what matters is that the exemption is a fact the API
// server can check, not a name this program looks for in a request it already
// paid for.
const exemptUser = "byok8s-exempt"

// The same rule as decide(), expressed where the API server evaluates it
// itself. The object is cluster-scoped and outlives this program, which is the
// whole point of moving the rule here.
const policyObjectName = "byok8s-owner-required"

const policyName = "owner-required"

// auditFor leaves a trace for whoever reads the audit log months from now: which
// policy ran, and what it concluded. The API server namespaces these keys under
// this webhook's name, so short local names cannot collide with anyone else's.
func auditFor(req *admissionv1.AdmissionRequest, allowed bool) map[string]string {
	annotations := map[string]string{
		"policy":  policyName,
		"allowed": strconv.FormatBool(allowed),
	}
	if req != nil {
		annotations["resource"] = req.Resource.Resource
	}
	return annotations
}

// warningsFor tells the person applying the object what is about to stop
// working, without refusing anything today. A warning on every request is a
// warning nobody reads, so this speaks only when the deprecated key is used.
func warningsFor(annotations map[string]string) []string {
	if _, ok := annotations[delayAnnotation]; !ok {
		return nil
	}
	return []string{fmt.Sprintf("%s is deprecated: a future release will ignore it and judge the pod immediately", delayAnnotation)}
}

// decide is the whole policy: it reads a request and returns a verdict,
// touching nothing outside it.
func decide(req *admissionv1.AdmissionRequest, name string) (bool, string) {
	// Rules live in the cluster, not in this program: something else can widen
	// them. Anything that is not a pod is somebody else's business, and saying
	// so is what keeps a widened rule from becoming an outage.
	if req.Resource.Resource != "pods" {
		return true, ""
	}

	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		return false, fmt.Sprintf("this object could not be read as a pod: %v", err)
	}

	// The message is the whole explanation whoever applied the pod will get:
	// nothing else points at this program, so it says what was wrong and what
	// would have been right.
	if pod.Labels["owner"] == "" {
		return false, fmt.Sprintf("pod %q has no owner label: every pod must say who owns it", name)
	}

	return true, ""
}

// dryRun reports whether this request is hypothetical.
//
// The field is a pointer and nil on an ordinary request, so it is never
// dereferenced without asking.
func dryRun(req *admissionv1.AdmissionRequest) bool {
	return req.DryRun != nil && *req.DryRun
}

// recordVerdict writes down what was decided. This is the side effect a dry run
// has to suppress: the request never happened, so neither did the record.
//
// It patches rather than reading and writing back, because several admissions
// are in flight at once and a read-modify-write loses the ones it did not see.
func recordVerdict(ctx context.Context, cs kubernetes.Interface, ns, name string, allowed bool) error {
	verdict := "denied"
	if allowed {
		verdict = "allowed"
	}
	patch := fmt.Appendf(nil, `{"data":{%q:%q}}`, name, verdict)

	_, err := cs.CoreV1().ConfigMaps(ns).Patch(ctx, verdictConfigMap, types.MergePatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cs.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: verdictConfigMap, Namespace: ns},
			Data:       map[string]string{name: verdict},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			_, err = cs.CoreV1().ConfigMaps(ns).Patch(ctx, verdictConfigMap, types.MergePatchType, patch, metav1.PatchOptions{})
		}
	}
	return err
}

// mutate answers with a patch describing a change rather than with a verdict.
//
// A patch is sent instead of a rewritten object because several webhooks may
// edit the same pod: each one describes its own change, and none of them
// silently discards another's.
func mutate(w http.ResponseWriter, r *http.Request) {
	var uid types.UID
	defer func() {
		// A mutator that cannot decide has nothing to add, so it allows the
		// object through unpatched rather than failing the request.
		if rec := recover(); rec != nil {
			fmt.Fprintf(os.Stderr, "panic while mutating: %v\n", rec)
			respond(w, uid, true, "")
		}
	}()

	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, fmt.Sprintf("decode AdmissionReview: %v", err), http.StatusBadRequest)
		return
	}
	if review.Request == nil {
		http.Error(w, "AdmissionReview carried no request", http.StatusBadRequest)
		return
	}
	uid = review.Request.UID

	if review.Request.Resource.Resource != "pods" {
		respond(w, uid, true, "")
		return
	}

	var pod corev1.Pod
	if err := json.Unmarshal(review.Request.Object.Raw, &pod); err != nil {
		respond(w, uid, true, "")
		return
	}

	// A reinvocation carries the uid of the call it repeats, so two of these
	// lines with one uid is the second pass made visible.
	fmt.Printf("mutate %s pods/%s uid=%s\n", review.Request.Operation, pod.Name, uid)

	// Admission can deliver this program its own output — another webhook's
	// patch triggers a second call — so every op below is emitted only when it
	// is still missing: applying this twice changes nothing the first pass did.
	patch := []map[string]any{}
	if pod.Annotations[injectedAnnotation] != "true" {
		// `add` cannot create the map it writes into: on a pod with no
		// annotations at all, the key needs its parent created first.
		if pod.Annotations == nil {
			patch = append(patch, map[string]any{"op": "add", "path": "/metadata/annotations", "value": map[string]string{}})
		}
		patch = append(patch, map[string]any{"op": "add", "path": injectedPointer, "value": "true"})
	}

	// A container that requested no CPU gets a floor. Each container is its own
	// path, so this is one op per container rather than one for the pod.
	//
	// Only on the way in: a pod's resources are settled at creation, and an
	// update that changes them is refused by the API server, not by any policy
	// here.
	if review.Request.Operation == admissionv1.Create {
		for i, c := range pod.Spec.Containers {
			if _, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
				continue
			}
			if c.Resources.Requests == nil {
				patch = append(patch, map[string]any{"op": "add", "path": fmt.Sprintf("/spec/containers/%d/resources/requests", i), "value": map[string]string{}})
			}
			patch = append(patch, map[string]any{"op": "add", "path": fmt.Sprintf("/spec/containers/%d/resources/requests/cpu", i), "value": defaultCPURequest})
		}
	}

	// A container can be added at admission but never to a pod that already
	// exists, so injection is a create-time decision and an update is left as
	// it is.
	if review.Request.Operation == admissionv1.Create && pod.Annotations[injectAnnotation] == "true" {
		present := false
		for _, c := range pod.Spec.Containers {
			if c.Name == sidecarName {
				present = true
			}
		}
		// Appending without looking first gives the pod two containers sharing
		// one name, and the API server blames the pod rather than this webhook.
		if !present {
			patch = append(patch, map[string]any{"op": "add", "path": "/spec/containers/-", "value": corev1.Container{
				Name:  sidecarName,
				Image: sidecarImage,
				// A sidecar is a real container that counts toward the pod's
				// requests, so it declares its own rather than being defaulted
				// later, once resources can no longer change.
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(defaultCPURequest)},
				},
			}})
		}
	}

	if len(patch) == 0 {
		respond(w, uid, true, "")
		return
	}

	raw, err := json.Marshal(patch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal patch: %v\n", err)
		respond(w, uid, true, "")
		return
	}
	fmt.Printf("patch %s/%s\n", review.Request.Resource.Resource, pod.Name)
	respondPatch(w, uid, raw)
}

// respondPatch replies with a patch and the type it is written in. A patch with
// no patchType is ignored, and Patch is a []byte, which the encoder base64s on
// its own — encoding it here would send it doubly encoded.
func respondPatch(w http.ResponseWriter, uid types.UID, patch []byte) {
	kind := admissionv1.PatchTypeJSONPatch
	reply := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: admissionv1.SchemeGroupVersion.String(),
			Kind:       "AdmissionReview",
		},
		Response: &admissionv1.AdmissionResponse{
			UID:       uid,
			Allowed:   true,
			PatchType: &kind,
			Patch:     patch,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(reply); err != nil {
		fmt.Fprintf(os.Stderr, "write AdmissionReview: %v\n", err)
	}
}

// certAuthority mints the CA a webhook's caBundle is built from.
//
// It signs the serving certificate and is never served itself, which is what
// lets the leaf be replaced without touching the registration the API server
// already trusts. A real deployment gets this from cert-manager or the
// cluster's signer; the shape is the same — one bundle, many leaves under it.
func certAuthority(host string) (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate CA key: %w", err)
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host + " CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create CA certificate: %w", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	return ca, key, der, nil
}

// leafFor mints the certificate the program presents, signed by the CA whose
// bytes are already in the registration. Rotation is a new leaf, not a new
// bundle.
func leafFor(host string, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (*tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(leafLifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{host},
	}
	// A caller reaching the webhook by address rather than by name — the
	// harness does, over loopback — is checked against IP SANs, not DNS ones.
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	}
	tmpl.IPAddresses = append(tmpl.IPAddresses, net.IPv4(127, 0, 0, 1), net.IPv6loopback)

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}
	return &tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        leaf,
	}, nil
}

func serialNumber() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("serial number: %w", err)
	}
	return serial, nil
}

// clientConfig loads the kubeconfig the way every Kubernetes tool does, and
// returns the namespace its context selects — the one namespace this webhook
// has any business judging.
func clientConfig() (*rest.Config, string, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("load kubeconfig: %w", err)
	}
	ns, _, err := cc.Namespace()
	if err != nil {
		return nil, "", fmt.Errorf("read namespace: %w", err)
	}
	return cfg, ns, nil
}

// register makes the API server call this program by creating a
// ValidatingWebhookConfiguration. An Ignore failure policy means a request is
// admitted when the webhook is unreachable.
func register(ctx context.Context, cs kubernetes.Interface, url string, caBundle []byte, namespace string) error {
	// A rule that may be bypassed by killing this process is not a rule, so the
	// judging half fails closed: while it is down, nothing it matches is
	// written. That makes this program a dependency of every pod create in the
	// namespaces it selects, which is the price of the guarantee.
	fail := admissionregistrationv1.Fail
	// This half records what it admits, so it does have a side effect — and
	// suppresses it on a dry run. Declaring None here would be a lie the API
	// server has no way to check.
	side := admissionregistrationv1.SideEffectClassNoneOnDryRun
	scope := admissionregistrationv1.NamespacedScope
	timeout := int32(5)

	cfg := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: webhookConfigName},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: "pods.byok8s.dev",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				URL:      &url,
				CABundle: caBundle,
			},
			// A rule checked only on CREATE admits a correct pod and then lets
			// anyone edit it into an incorrect one, and a rule wider than the
			// objects the handler understands puts this program on the write
			// path for objects it would misjudge.
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
				Rule: admissionregistrationv1.Rule{
					APIGroups:   []string{""},
					APIVersions: []string{"v1"},
					Resources:   []string{"pods"},
					Scope:       &scope,
				},
			}},
			FailurePolicy:  &fail,
			SideEffects:    &side,
			TimeoutSeconds: &timeout,
			// The check this program used to make in its handler, moved to where
			// the API server can make it instead. A request excluded here costs
			// no round trip and cannot time out, and it keeps working while this
			// program is down — none of which is true of recognising the caller
			// after the call arrives.
			MatchConditions: []admissionregistrationv1.MatchCondition{{
				Name:       "exclude-exempt-writer",
				Expression: fmt.Sprintf("request.userInfo.username != %q", exemptUser),
			}},
			// The selector matches labels on the namespace rather than on the pod,
			// `kubernetes.io/metadata.name` is maintained by the API server on every
			// namespace so nothing has to be labelled first, and a webhook that does
			// not exclude the namespaces holding the control plane can stop the cluster
			// repairing itself.
			NamespaceSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "kubernetes.io/metadata.name",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{namespace},
				}},
			},
			// A request this selector excludes never leaves the API server, so
			// it costs no round trip and cannot time out. Skipping on absence
			// rather than presence keeps the policy on by default: a pod has to
			// ask to be left alone.
			ObjectSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "byok8s.dev/skip",
					Operator: metav1.LabelSelectorOpDoesNotExist,
				}},
			},
			AdmissionReviewVersions: []string{"v1"},
		}},
	}

	api := cs.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	_, err := api.Create(ctx, cfg, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := api.Get(ctx, webhookConfigName, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("read existing %s: %w", webhookConfigName, getErr)
		}
		cfg.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, cfg, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("register webhook: %w", err)
	}
	return nil
}

// registerMutating registers the patching half of this program.
//
// It is a separate configuration kind because it runs in a separate phase:
// every mutating webhook finishes before any validating one starts, so what
// this returns is what the validating half will judge.
func registerMutating(ctx context.Context, cs kubernetes.Interface, url string, caBundle []byte, namespace string) error {
	fail := admissionregistrationv1.Ignore
	side := admissionregistrationv1.SideEffectClassNone
	scope := admissionregistrationv1.NamespacedScope
	timeout := int32(5)
	reinvoke := admissionregistrationv1.IfNeededReinvocationPolicy

	cfg := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: webhookConfigName},
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name: "mutate.pods.byok8s.dev",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				URL:      &url,
				CABundle: caBundle,
			},
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
				Rule: admissionregistrationv1.Rule{
					APIGroups:   []string{""},
					APIVersions: []string{"v1"},
					Resources:   []string{"pods"},
					Scope:       &scope,
				},
			}},
			FailurePolicy:  &fail,
			SideEffects:    &side,
			TimeoutSeconds: &timeout,
			// Ordering among mutating webhooks is not ours to choose, so ask to
			// be called again when someone later changed the object out from
			// under the decision made here. It costs a second pass, and it is
			// only safe because every op this program emits is conditional.
			ReinvocationPolicy: &reinvoke,
			// The same exemption as the judging half, for the same reason. An
			// exemption that covers one registration and not the other still
			// pays for the round trip it was meant to avoid.
			MatchConditions: []admissionregistrationv1.MatchCondition{{
				Name:       "exclude-exempt-writer",
				Expression: fmt.Sprintf("request.userInfo.username != %q", exemptUser),
			}},
			// The patching half is scoped exactly like the judging half: a
			// mutator loose in the control plane's namespaces edits objects the
			// cluster needs in order to start.
			NamespaceSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "kubernetes.io/metadata.name",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{namespace},
				}},
			},
			ObjectSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "byok8s.dev/skip",
					Operator: metav1.LabelSelectorOpDoesNotExist,
				}},
			},
			AdmissionReviewVersions: []string{"v1"},
		}},
	}

	api := cs.AdmissionregistrationV1().MutatingWebhookConfigurations()
	_, err := api.Create(ctx, cfg, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := api.Get(ctx, webhookConfigName, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("read existing %s: %w", webhookConfigName, getErr)
		}
		cfg.ResourceVersion = existing.ResourceVersion
		_, err = api.Update(ctx, cfg, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("register mutating webhook: %w", err)
	}
	return nil
}

// unregister removes the registration on the way out. A configuration left
// behind points the API server at a port nothing is listening on.
func unregister(cs kubernetes.Interface) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cs.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(ctx, webhookConfigName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	err = cs.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, webhookConfigName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// respond writes the one reply shape this program has: 200, an
// AdmissionReview, and the uid of the request being answered. Anything else —
// a 400, a 500, a closed connection — is not a verdict the API server can
// read, and is handled by the failure policy instead of by this program.
func respond(w http.ResponseWriter, uid types.UID, allowed bool, message string) {
	writeReview(w, newResponse(uid, allowed, message))
}

// respondAudited answers the two audiences a verdict never reaches: the audit
// log, through auditAnnotations, and whoever ran the command, through warnings.
//
// Both ride on a response that allows the object, which is the only place they
// are any use — a rejection already explains itself.
func respondAudited(w http.ResponseWriter, uid types.UID, allowed bool, message string, annotations map[string]string, warnings []string) {
	resp := newResponse(uid, allowed, message)
	resp.AuditAnnotations = annotations
	resp.Warnings = warnings
	writeReview(w, resp)
}

func newResponse(uid types.UID, allowed bool, message string) *admissionv1.AdmissionResponse {
	resp := &admissionv1.AdmissionResponse{UID: uid, Allowed: allowed}
	if message != "" {
		resp.Result = &metav1.Status{
			Code:    http.StatusForbidden,
			Reason:  metav1.StatusReasonForbidden,
			Message: message,
		}
	}
	return resp
}

func writeReview(w http.ResponseWriter, resp *admissionv1.AdmissionResponse) {
	reply := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: admissionv1.SchemeGroupVersion.String(),
			Kind:       "AdmissionReview",
		},
		Response: resp,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(reply); err != nil {
		fmt.Fprintf(os.Stderr, "write AdmissionReview: %v\n", err)
	}
}
