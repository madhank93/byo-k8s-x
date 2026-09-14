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
	"syscall"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// The name of the configuration this program owns. One name means a restart
// updates its own registration rather than accumulating a second one.
const webhookConfigName = "byok8s-webhook"

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

	cert, caDER, err := selfSigned(*externalHost)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/validate", validate)

	srv := &http.Server{
		Addr:      *addr,
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{*cert}},
	}
	go func() {
		<-ctx.Done()
		fmt.Println("shutting down")
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
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

	_, port, err := net.SplitHostPort(*addr)
	if err != nil {
		return fmt.Errorf("read the port out of %q: %w", *addr, err)
	}
	url := fmt.Sprintf("https://%s/validate", net.JoinHostPort(*externalHost, port))

	// The caBundle is parsed as PEM, not as the DER the certificate was built
	// from: raw bytes there fail inside the API server, not at registration.
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	if err := register(ctx, cs, url, caPEM, ns); err != nil {
		return err
	}
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

// validate answers an AdmissionReview with an AdmissionReview.
//
// The wrapper is the protocol: the same kind goes back, carrying the request's
// uid and a verdict. A reply that loses the uid is discarded — the API server
// cannot tell which request it answers.
func validate(w http.ResponseWriter, r *http.Request) {
	// A panic would close the connection with no verdict in it, which reads to
	// the API server as an unreachable webhook rather than as a bug here.
	var uid types.UID
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "panic while admitting: %v\n", r)
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
	// object — so it is read from there when the request field is empty.
	name := review.Request.Name
	if name == "" {
		var meta metav1.PartialObjectMetadata
		if err := json.Unmarshal(review.Request.Object.Raw, &meta); err == nil {
			name = meta.Name
		}
	}
	fmt.Printf("review %s %s/%s\n", review.Request.Operation, review.Request.Resource.Resource, name)

	// Rules live in the cluster, not in this program: something else can widen
	// them. Anything that is not a pod is somebody else's business, and saying
	// so is what keeps a widened rule from becoming an outage.
	if review.Request.Resource.Resource != "pods" {
		respond(w, review.Request.UID, true, "")
		return
	}

	var pod corev1.Pod
	if err := json.Unmarshal(review.Request.Object.Raw, &pod); err != nil {
		respond(w, review.Request.UID, false, fmt.Sprintf("this object could not be read as a pod: %v", err))
		return
	}

	// The message is the whole explanation whoever applied the pod will get:
	// nothing else points at this program, so it says what was wrong and what
	// would have been right.
	if pod.Labels["owner"] == "" {
		respond(w, review.Request.UID, false, fmt.Sprintf("pod %q has no owner label: every pod must say who owns it", name))
		return
	}

	respond(w, review.Request.UID, true, "")
}

// selfSigned mints a certificate for the name the API server will use, and
// returns it alongside the DER bytes a webhook's caBundle is built from.
//
// The certificate signs itself, so it is its own CA: whoever is told to trust
// these bytes trusts this program and nothing else. A real deployment gets its
// certificate from cert-manager or the cluster's signer, but the shape is the
// same — something has to hand the API server a bundle it will accept.
func selfSigned(host string) (*tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("serial number: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{host},
	}
	// A caller reaching the webhook by address rather than name — the harness
	// does, over the loopback — is checked against the IP SANs, not the DNS
	// ones.
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	}
	tmpl.IPAddresses = append(tmpl.IPAddresses, net.IPv4(127, 0, 0, 1), net.IPv6loopback)

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("parse certificate: %w", err)
	}
	return &tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        leaf,
	}, der, nil
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
	fail := admissionregistrationv1.Ignore
	side := admissionregistrationv1.SideEffectClassNone
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

// unregister removes the registration on the way out. A configuration left
// behind points the API server at a port nothing is listening on.
func unregister(cs kubernetes.Interface) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cs.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(ctx, webhookConfigName, metav1.DeleteOptions{})
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
	resp := &admissionv1.AdmissionResponse{UID: uid, Allowed: allowed}
	if message != "" {
		resp.Result = &metav1.Status{
			Code:    http.StatusForbidden,
			Reason:  metav1.StatusReasonForbidden,
			Message: message,
		}
	}
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
