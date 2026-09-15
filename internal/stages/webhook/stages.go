// Package webhook holds the assertions for the "Build your own admission
// webhook" course — one function per stage, registered by slug.
//
// An admission webhook is judged from the other side: the API server calls the
// learner's program, so almost every stage here starts the program, registers
// it (or lets it register itself), then applies an object and asks what the
// API server did with it. The verdict is the cluster's, not ours. Nothing
// inspects the learner's source — any program the API server is satisfied by
// is a correct one.
package webhook

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// StageTimeout bounds one stage.
const StageTimeout = stages.Timeout

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

// externalHost is the name the API server uses to reach a program running on
// this machine.
//
// The cluster is a kind node, which is a container: "localhost" there is the
// container, not the host the learner's webhook is listening on. Docker
// publishes the host under this name, and it is passed to the program rather
// than compiled into it, so the same program would work unchanged against a
// cluster that reaches it some other way.
const externalHost = "host.docker.internal"

var registry = map[string]Stage{}

func register(s Stage) { registry[s.Slug] = s }

// Lookup resolves a stage slug within this course.
func Lookup(slug string) (Stage, bool) {
	s, ok := registry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "serve-tls", Run: stageServeTLS})
	register(Stage{Slug: "admission-review", Run: stageAdmissionReview})
	register(Stage{Slug: "register-validating", Run: stageRegisterValidating})
	register(Stage{Slug: "deny", Run: stageDeny})
	register(Stage{Slug: "uid-echo", Run: stageUIDEcho})
	register(Stage{Slug: "rules-scope", Run: stageRulesScope})
	register(Stage{Slug: "namespace-selector", Run: stageNamespaceSelector})
	register(Stage{Slug: "object-selector", Run: stageObjectSelector})
	register(Stage{Slug: "mutate-patch", Run: stageMutatePatch})
	register(Stage{Slug: "defaulting", Run: stageDefaulting})
	register(Stage{Slug: "sidecar-inject", Run: stageSidecarInject})
	register(Stage{Slug: "dry-run", Run: stageDryRun})
	register(Stage{Slug: "failure-policy", Run: stageFailurePolicy})
	register(Stage{Slug: "timeout", Run: stageTimeout})
	register(Stage{Slug: "reinvocation", Run: stageReinvocation})
	register(Stage{Slug: "cert-rotation", Run: stageCertRotation})
	register(Stage{Slug: "audit-annotations", Run: stageAuditAnnotations})
	register(Stage{Slug: "match-conditions", Run: stageMatchConditions})
}

// stageServeTLS checks the one thing every later stage rests on: that the
// program is listening, and listening with TLS.
//
// The API server will not speak plaintext to a webhook, and it will not
// negotiate: a webhook that serves HTTP is simply unreachable, and the failure
// arrives later as an admission timeout that says nothing about certificates.
func stageServeTLS(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return fmt.Errorf("the program never said it was serving: %w", err)
	}

	if err := waitFor(ctx, "the webhook to answer over TLS", 30*time.Second, func(ctx context.Context) (bool, error) {
		_, err := tlsGet(ctx, "https://"+addr+"/healthz")
		return err == nil, nil
	}); err != nil {
		return fmt.Errorf("nothing answered HTTPS on %s: %w\nthe program said:\n%s", addr, err, tail(p.Stdout()))
	}

	// A plaintext request to a TLS listener fails at the handshake. If this
	// one succeeds, the program is serving HTTP and the API server will never
	// reach it.
	if _, err := plainGet(ctx, "http://"+addr+"/healthz"); err == nil {
		return fmt.Errorf("the program answered a plain HTTP request on %s — the API server only calls webhooks over TLS", addr)
	}
	return nil
}

// stageCertRotation checks that the certificate can be replaced under a running
// server without the API server losing trust in what it finds there.
//
// Both halves have to be valid at the same instant: the leaf the program serves,
// and the caBundle its own registration is verified against. Minting the new leaf
// from the CA that is already registered is what buys that, so what this asserts
// is that the bundle never had to be rewritten at all.
func stageCertRotation(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	bundle := hook.ClientConfig.CABundle
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return fmt.Errorf("the caBundle in the registration holds no PEM certificate, so the API server has nothing to verify the webhook against")
	}

	before, err := servedLeaf(ctx, addr)
	if err != nil {
		return fmt.Errorf("open a TLS connection to the program: %w", err)
	}
	if err := verifyAgainst(before, roots); err != nil {
		return fmt.Errorf("the certificate the program serves does not verify against the caBundle it registered, so the API server refuses the call before the webhook ever sees it: %w", err)
	}
	if before.IsCA {
		return fmt.Errorf("the program serves its CA certificate rather than a leaf signed by it: the bundle is what verifies the connection, not what the connection presents")
	}

	// Saying it rotated and actually serving something new on the next handshake
	// are two different claims. Only the second one is worth checking.
	if err := awaitLine(ctx, p, "rotated certificate", 90*time.Second); err != nil {
		return fmt.Errorf("the program never said it rotated its certificate — a rotation nobody can see is one nobody can correlate with an outage: %w", err)
	}

	var after *x509.Certificate
	var lastDial error
	if err := waitFor(ctx, "a fresh handshake to get the rotated certificate", 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := servedLeaf(ctx, addr)
		if err != nil {
			// A refused or reset connection is the swap itself being caught in
			// the act, not a reason to stop looking: the poll aborts on a
			// returned error, and one unlucky handshake would fail a program
			// that rotates correctly.
			lastDial = err
			return false, nil
		}
		after = got
		return got.SerialNumber.Cmp(before.SerialNumber) != 0, nil
	}); err != nil {
		if after == nil && lastDial != nil {
			return fmt.Errorf("the program announced a rotation, but no connection after it ever completed a handshake: %w", lastDial)
		}
		return fmt.Errorf("the program announced a rotation, but a new connection still gets serial %s: tls.Config.Certificates is read once at startup, so the swap only reaches a handshake if it happens behind GetCertificate: %w", before.SerialNumber, err)
	}

	if err := verifyAgainst(after, roots); err != nil {
		return fmt.Errorf("the rotated certificate does not verify against the caBundle that was registered, so every call after the rotation fails verification: %w", err)
	}

	// Read the registration again rather than trust the copy taken above: a leaf
	// that only verifies because the bundle was rewritten to match it is the
	// failure this stage exists to catch.
	current, ok, err := findRegistration(ctx, env, url)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("the registration pointing at %s disappeared across the rotation", url)
	}
	if !slices.Equal(current.ClientConfig.CABundle, bundle) {
		return fmt.Errorf("the caBundle was rewritten during the rotation: minting the new leaf from the CA already in the bundle is what keeps the serving side and the trust side valid at the same instant")
	}

	// What all of it is for: admission is unchanged on the other side.
	const good = "after-rotation"
	if err := seedPod(ctx, env, good, map[string]string{"owner": "byok8s"}, nil); err != nil {
		return fmt.Errorf("pod %q carries an owner label and was rejected after the rotation: %w\nthe program said:\n%s", good, err, tail(p.Stdout()))
	}

	const bad = "after-rotation-no-owner"
	err = seedPod(ctx, env, bad, nil, nil)
	if err == nil {
		return fmt.Errorf("pod %q carries no owner label and was admitted after the rotation: the webhook stopped being consulted somewhere in the swap", bad)
	}
	if !strings.Contains(err.Error(), "denied the request") {
		return fmt.Errorf("pod %q was refused after the rotation, but not by the webhook: %w", bad, err)
	}
	return nil
}

// stageAuditAnnotations checks the two things a verdict cannot carry: a line
// in the audit log for whoever investigates months from now, and a warning for
// whoever is running the command right now.
//
// Both belong on a response that *allows* the object, so neither can be read
// off the admission decision. The stage asks the program directly for the
// response fields, then creates a pod through the API server to prove the
// warning survives the trip back to a client.
func stageAuditAnnotations(ctx context.Context, env *kube.Env, bin string) error {
	const deprecated = "byok8s.dev/delay"

	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, ok, err := findRegistration(ctx, env, url)
		return ok, err
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	owned := map[string]string{"owner": "platform"}
	legacy := map[string]string{deprecated: "200ms"}

	// The ordinary admitted pod. The audit trail has to exist when nothing is
	// wrong, because that is the request an investigation starts from.
	object, err := podObject("audited", env.Namespace, owned, nil)
	if err != nil {
		return err
	}
	admitted, err := askVerdict(ctx, addr, "audit-allowed", env.Namespace, object)
	if err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if !admitted.Allowed {
		return fmt.Errorf("a pod carrying an owner label was refused, so this stage cannot tell a missing audit trail from a rejection\nthe program said:\n%s", tail(p.Stdout()))
	}
	if len(admitted.AuditAnnotations) == 0 {
		return fmt.Errorf("the response admitting the pod carries no auditAnnotations: an allowed request leaves no other trace, so nobody reading the audit log later can tell this webhook ran at all")
	}
	for k, v := range admitted.AuditAnnotations {
		if k == "" || v == "" {
			return fmt.Errorf("an audit annotation has an empty key or value (%q: %q), which records nothing", k, v)
		}
		if strings.Contains(k, "/") {
			return fmt.Errorf("the audit annotation key %q is already namespaced: the API server prefixes these with the webhook's own name, so a key you prefix yourself arrives prefixed twice", k)
		}
	}
	if len(admitted.Warnings) != 0 {
		return fmt.Errorf("a pod that used nothing deprecated still came back with the warning %q: a warning on every request is one people stop reading, so keep it for the request that earns it", admitted.Warnings[0])
	}

	// The same pod plus the deprecated annotation: still admitted, and told why
	// it will not be next time.
	object, err = podObject("audited-legacy", env.Namespace, owned, legacy)
	if err != nil {
		return err
	}
	warned, err := askVerdict(ctx, addr, "audit-warned", env.Namespace, object)
	if err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if !warned.Allowed {
		return fmt.Errorf("the pod using %s was refused: a deprecation warns the people still on the old field, it does not break them", deprecated)
	}
	if len(warned.AuditAnnotations) == 0 {
		return fmt.Errorf("the response to the pod using %s carries no auditAnnotations, although the plain pod's did", deprecated)
	}
	if !slices.ContainsFunc(warned.Warnings, func(w string) bool { return strings.Contains(w, deprecated) }) {
		return fmt.Errorf("the pod using the deprecated %s annotation came back with warnings %q: none of them names the annotation, so nobody applying it learns what to change", deprecated, warned.Warnings)
	}

	// None of this is allowed to soften the verdict itself.
	object, err = podObject("audited-no-owner", env.Namespace, nil, nil)
	if err != nil {
		return err
	}
	refused, err := askVerdict(ctx, addr, "audit-refused", env.Namespace, object)
	if err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if refused.Allowed {
		return fmt.Errorf("a pod with no owner label was admitted, so the policy was lost somewhere in the new response fields")
	}

	// The last hop is the one the learner cannot see from their own logs: the
	// API server has to turn resp.Warnings into a header the client prints.
	seen, err := warningsFromCreate(ctx, env, "audited-through-api", owned, legacy)
	if err != nil {
		return fmt.Errorf("%w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if !slices.ContainsFunc(seen, func(w string) bool { return strings.Contains(w, deprecated) }) {
		return fmt.Errorf("creating that pod through the API server surfaced the warnings %q to the client: resp.Warnings is what becomes the Warning: header kubectl prints, so an empty field is silence at the terminal", seen)
	}

	return nil
}

// stageMatchConditions checks the caller check moved out of the handler and
// into the API server.
//
// An objectSelector can only ask about labels on the object. A match condition
// is CEL over the whole request — who is asking, which operation, which
// subresource — which is what it takes to exempt one identity. Recognising that
// identity in the handler reaches the same verdict, but it has already paid for
// the round trip, and the exemption stops working the moment the program does.
func stageMatchConditions(ctx context.Context, env *kube.Env, bin string) error {
	const exemptUser = "byok8s-exempt"

	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	mutateURL := fmt.Sprintf("https://%s:%d/mutate", externalHost, port)
	var mutating admissionregistrationv1.MutatingWebhook
	if err := waitFor(ctx, "the program to register its mutating half", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findMutatingRegistration(ctx, env, mutateURL)
		if err != nil || !ok {
			return false, err
		}
		mutating = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no MutatingWebhookConfiguration points at %s: %w", mutateURL, err)
	}

	if len(hook.MatchConditions) == 0 {
		return fmt.Errorf("the validating registration carries no matchConditions, so every request reaches this program before anything decides whether it should have: an exemption is worth only as much as the API server can check without calling you")
	}
	if len(mutating.MatchConditions) == 0 {
		return fmt.Errorf("the mutating registration carries no matchConditions, so the exempt caller still reaches this program's patching half: an exemption that covers one registration and not the other pays for the round trip it was meant to avoid")
	}
	for _, c := range slices.Concat(hook.MatchConditions, mutating.MatchConditions) {
		if c.Name == "" || c.Expression == "" {
			return fmt.Errorf("a match condition is missing its name or expression (%q: %q)", c.Name, c.Expression)
		}
	}

	// The ordinary caller is judged exactly as before.
	if err := deletePod(ctx, env.Client, env.Namespace, "matched-no-owner"); err != nil {
		return err
	}
	if err := seedPod(ctx, env, "matched-no-owner", nil, nil); err == nil {
		return fmt.Errorf("a pod with no owner label was admitted for an ordinary caller, so the match condition excludes more than the one identity it should")
	}

	// The exempt caller writes the same pod, and is never asked about.
	if err := grantNamespaceWrite(ctx, env, exemptUser); err != nil {
		return err
	}
	exempt, err := clientAs(env, exemptUser)
	if err != nil {
		return err
	}

	const skipped = "matched-exempt"
	if err := deletePod(ctx, env.Client, env.Namespace, skipped); err != nil {
		return err
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: skipped, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	var lastErr error
	if err := waitFor(ctx, fmt.Sprintf("the write from %s to be accepted", exemptUser), 60*time.Second, func(ctx context.Context) (bool, error) {
		_, err := exempt.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		if err != nil && strings.Contains(err.Error(), "admission webhook") {
			return false, fmt.Errorf("the webhook refused it: %w", err)
		}
		// A fresh RoleBinding takes a moment to reach the authorizer, and that
		// refusal looks nothing like an admission refusal.
		lastErr = err
		return err == nil, nil
	}); err != nil {
		return fmt.Errorf("the write from %s never succeeded (%v): a request the match condition excludes never reaches this program, so nothing is left to refuse it: %w\nthe program said:\n%s", exemptUser, lastErr, err, tail(p.Stdout()))
	}

	// A later request that *is* judged proves the program's output has been
	// read past the point where the exempt one would have appeared.
	const judged = "matched-ordinary"
	if err := deletePod(ctx, env.Client, env.Namespace, judged); err != nil {
		return err
	}
	if err := seedPod(ctx, env, judged, map[string]string{"owner": "platform"}, nil); err != nil {
		return fmt.Errorf("a pod carrying an owner label was refused: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if err := awaitLine(ctx, p, judged, 60*time.Second); err != nil {
		return fmt.Errorf("the program never logged the request for %s, so this stage cannot tell silence about %s from output it has not read yet: %w", judged, skipped, err)
	}

	if strings.Contains(p.Stdout(), skipped) {
		return fmt.Errorf("pod %s was admitted, but this program logged a request for it: the exemption is being made in the handler, so the round trip was paid to reach a verdict the API server could have skipped, and it stops applying the moment this program is down\nthe program said:\n%s", skipped, tail(p.Stdout()))
	}

	return nil
}

// stageNamespaceSelector checks the webhook narrows itself to one namespace.
//
// A webhook with no namespaceSelector sits on the write path for every
// namespace, including the ones the control plane needs in order to repair
// itself, so a webhook that is down or wrong takes the cluster with it.
func stageNamespaceSelector(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	if hook.NamespaceSelector == nil {
		return fmt.Errorf("the webhook has no namespaceSelector, so it is on the write path for every namespace, including the ones the control plane needs to repair itself")
	}

	const nameLabel = "kubernetes.io/metadata.name"
	narrowed := hook.NamespaceSelector.MatchLabels[nameLabel] == env.Namespace
	for _, req := range hook.NamespaceSelector.MatchExpressions {
		if req.Key == nameLabel && slices.Contains(req.Values, env.Namespace) {
			narrowed = true
		}
	}
	if !narrowed {
		return fmt.Errorf("the namespaceSelector does not select namespace %q by its %s label, so it does not narrow this webhook to the namespace it was meant to police", env.Namespace, nameLabel)
	}

	// A selector is only worth as much as the request it lets past: the claim
	// is checked against a real namespace this webhook was never meant to see.
	other := fmt.Sprintf("byok8s-other-%d", port)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: other}}
	if _, err := env.Client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", other, err)
	}
	defer func() {
		_ = env.Client.CoreV1().Namespaces().Delete(context.WithoutCancel(ctx), other, metav1.DeleteOptions{})
	}()

	bystander := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "no-owner", Namespace: other},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(other).Create(ctx, bystander, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("a pod with no owner label was refused in namespace %s, which this webhook was never meant to judge: %w\nthe program said:\n%s", other, err, tail(p.Stdout()))
	}

	// Narrowing must not turn into switching off: the rule still applies where
	// it was meant to.
	if err := waitFor(ctx, "the API server to refuse a pod with no owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := env.SeedPods(ctx, "no-owner")
		if err == nil {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, "no-owner", metav1.DeleteOptions{})
			return false, nil
		}
		return strings.Contains(err.Error(), "denied the request"), nil
	}); err != nil {
		return fmt.Errorf("a pod with no owner label was admitted into namespace %q, which the selector should still cover: %w\nthe program said:\n%s", env.Namespace, err, tail(p.Stdout()))
	}

	return nil
}

// stageObjectSelector checks the webhook narrows by labels on the object.
//
// A request an objectSelector excludes never leaves the API server, so it
// costs no round trip and cannot time out: the cheapest request is the one
// never sent.
func stageObjectSelector(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	const skipLabel = "byok8s.dev/skip"
	if hook.ObjectSelector == nil {
		return fmt.Errorf("the webhook has no objectSelector, so every pod in the namespace is sent here even when it has asked to be left alone")
	}
	opted := false
	for _, req := range hook.ObjectSelector.MatchExpressions {
		if req.Key == skipLabel && req.Operator == metav1.LabelSelectorOpDoesNotExist {
			opted = true
		}
	}
	if !opted {
		return fmt.Errorf("the objectSelector does not skip objects carrying %s, so nothing can opt out of this policy", skipLabel)
	}

	// The selector is only real if the API server never asks: a pod that would
	// be refused on its merits is admitted purely because it opted out.
	if err := waitFor(ctx, "the API server to admit a pod that opted out", 60*time.Second, func(ctx context.Context) (bool, error) {
		return env.SeedLabeledPod(ctx, "opted-out", map[string]string{skipLabel: "true"}) == nil, nil
	}); err != nil {
		return fmt.Errorf("pod %q carries %s and no owner label, so it should never have reached this webhook: %w\nthe program said:\n%s", "opted-out", skipLabel, err, tail(p.Stdout()))
	}

	// Opting out must stay opt-in: everything else is still judged.
	if err := waitFor(ctx, "the API server to refuse a pod with no owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := env.SeedPods(ctx, "no-owner")
		if err == nil {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, "no-owner", metav1.DeleteOptions{})
			return false, nil
		}
		return strings.Contains(err.Error(), "denied the request"), nil
	}); err != nil {
		return fmt.Errorf("a pod with no owner label and no %s was admitted, so the objectSelector is skipping objects it should judge: %w\nthe program said:\n%s", skipLabel, err, tail(p.Stdout()))
	}

	return nil
}

// stageRulesScope checks that the webhook's rules are precise and scoped.
//
// A rule is the webhook's blast radius written down, so this stage reads it for
// what it claims to judge and then checks the program behaves that way —
// refusing an edit that breaks the rule, and standing aside for an object that
// is not its business.
func stageRulesScope(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	judgesUpdates := false
	for _, rule := range hook.Rules {
		if slices.Contains(rule.Operations, admissionregistrationv1.Update) {
			judgesUpdates = true
		}
	}
	if !judgesUpdates {
		return fmt.Errorf("the rules cover creating a pod but not updating one, so a pod that is correct when it is created can be edited into one that is not")
	}

	for _, rule := range hook.Rules {
		if slices.Contains(rule.Operations, admissionregistrationv1.OperationAll) ||
			slices.Contains(rule.Resources, "*") ||
			slices.Contains(rule.APIGroups, "*") {
			return fmt.Errorf("the rules use a wildcard, which puts this webhook on the write path for objects it was not written to judge")
		}
		if rule.Scope == nil || *rule.Scope != admissionregistrationv1.NamespacedScope {
			return fmt.Errorf("the rules do not say the objects are namespaced, so the same rule matches cluster-scoped kinds")
		}
	}

	const pod = "edited-later"
	if err := waitFor(ctx, "the API server to admit a pod that carries an owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		return env.SeedLabeledPod(ctx, pod, map[string]string{"owner": "byok8s"}) == nil, nil
	}); err != nil {
		return fmt.Errorf("pod %q carries an owner label and was still rejected: %w\nthe program said:\n%s", pod, err, tail(p.Stdout()))
	}

	patch := []byte(`{"metadata":{"labels":{"owner":null}}}`)
	_, err = env.Client.CoreV1().Pods(env.Namespace).Patch(ctx, pod, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err == nil {
		return fmt.Errorf("the owner label was removed from pod %q by an update, which the rules should have sent here\nthe program said:\n%s", pod, tail(p.Stdout()))
	}
	if !strings.Contains(err.Error(), "denied the request") {
		return fmt.Errorf("removing the owner label from pod %q failed for a reason that is not this webhook: %w", pod, err)
	}

	const uid = "44444444-0000-4000-8000-000000000004"
	cm := `{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "not-a-pod"}, "data": {"k": "v"}}`
	review := strings.Replace(reviewFor(uid, env.Namespace, cm), `"resource": "pods"`, `"resource": "configmaps"`, 1)

	var body string
	if err := waitFor(ctx, "the webhook to answer about a ConfigMap", 30*time.Second, func(ctx context.Context) (bool, error) {
		got, err := tlsPost(ctx, "https://"+addr+"/validate", review)
		if err != nil {
			return false, nil
		}
		body = got
		return true, nil
	}); err != nil {
		return fmt.Errorf("asking about a ConfigMap got no answer: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if !strings.Contains(body, `"allowed":true`) && !strings.Contains(body, `"allowed": true`) {
		return fmt.Errorf("a ConfigMap was judged by a webhook that only understands pods:\n%s", tail(body))
	}
	return nil
}

// stageAdmissionReview checks the request and response shape on its own,
// before the API server is involved.
//
// An AdmissionReview is a wrapper: the same kind goes in and comes back, with
// the answer in .response. Getting this wrong is the most common way a webhook
// fails silently — the API server reads no verdict and treats the reply as
// malformed.
func stageAdmissionReview(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	const uid = "3f2b1c00-0000-4000-8000-000000000001"
	review := fmt.Sprintf(`{
	  "apiVersion": "admission.k8s.io/v1",
	  "kind": "AdmissionReview",
	  "request": {
	    "uid": %q,
	    "operation": "CREATE",
	    "resource": {"group": "", "version": "v1", "resource": "pods"},
	    "namespace": %q,
	    "object": {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "asked-about"}}
	  }
	}`, uid, env.Namespace)

	var body string
	if err := waitFor(ctx, "the webhook to answer an AdmissionReview", 30*time.Second, func(ctx context.Context) (bool, error) {
		got, err := tlsPost(ctx, "https://"+addr+"/validate", review)
		if err != nil {
			return false, nil
		}
		body = got
		return true, nil
	}); err != nil {
		return fmt.Errorf("POST /validate never answered: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	switch {
	case !strings.Contains(body, `"kind":"AdmissionReview"`) && !strings.Contains(body, `"kind": "AdmissionReview"`):
		return fmt.Errorf("the reply is not an AdmissionReview — the same kind goes out as came in:\n%s", tail(body))
	case !strings.Contains(body, uid):
		return fmt.Errorf("the reply does not carry the request's uid, so the API server cannot match it to the request it made:\n%s", tail(body))
	case !strings.Contains(body, `"allowed"`):
		return fmt.Errorf("the reply has no .response.allowed, which is the whole verdict:\n%s", tail(body))
	}
	return nil
}

// stageRegisterValidating checks the step that turns a program which merely
// serves into part of the cluster's admission chain.
//
// The proof is not that the configuration exists — it is that the API server
// used it. So the registration is read for the fields that make it usable at
// all, and then a pod is created and the program has to say it was asked.
func stageRegisterValidating(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w\nthe program said:\n%s", url, err, tail(p.Stdout()))
	}

	if len(hook.ClientConfig.CABundle) == 0 {
		return fmt.Errorf("the registration carries no caBundle, so the API server has no reason to trust the certificate the program serves")
	}
	if block, _ := pem.Decode(hook.ClientConfig.CABundle); block == nil {
		return fmt.Errorf("the caBundle is not PEM — the API server parses it as a PEM block, so raw DER registers cleanly and then fails on every request it should have judged")
	}
	if !slices.Contains(hook.AdmissionReviewVersions, "v1") {
		return fmt.Errorf("the registration asks for admission review versions %v, and the program answers v1", hook.AdmissionReviewVersions)
	}
	if hook.SideEffects == nil {
		return fmt.Errorf("the registration does not say whether the webhook has side effects — the API server rejects a webhook that leaves it unstated")
	}
	if !admitsPods(hook) {
		return fmt.Errorf("the registration's rules do not cover creating a pod, so nothing this stage does will reach the program")
	}

	// The pod carries the label later stages make mandatory: a stage's fixture
	// has to stay admissible as the program it grades grows.
	const pod = "admitted-by-you"
	if err := env.SeedLabeledPod(ctx, pod, map[string]string{"owner": "byok8s"}); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, pod, 60*time.Second); err != nil {
		return fmt.Errorf("the API server never asked about pod %q, or the program did not say so: %w\nthe program said:\n%s", pod, err, tail(p.Stdout()))
	}
	return nil
}

// stageDeny checks that the webhook rejects pods without an owner label, and
// admits ones with it.
//
// A validating webhook earns its name by saying no; the rejection is a
// successful HTTP response carrying allowed: false, and the message it carries
// is the only explanation the person applying the object ever sees.
// stageUIDEcho checks that every answer is one the API server can read.
//
// A reply is matched to its request by uid, so one that loses it is discarded
// as though the call never returned — and the paths that lose it are the error
// paths. So this stage asks about an object the program cannot decode, and
// expects a verdict rather than an HTTP error.
func stageUIDEcho(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	cases := []struct {
		what   string
		uid    string
		object string
	}{
		{"a pod that satisfies the rule", "11111111-0000-4000-8000-000000000001", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "fine", "labels": {"owner": "byok8s"}}}`},
		{"a pod the rule refuses", "22222222-0000-4000-8000-000000000002", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "unowned"}}`},
		{"an object that is not a pod at all", "33333333-0000-4000-8000-000000000003", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "broken"}, "spec": {"containers": "not-a-list"}}`},
	}

	for _, c := range cases {
		var body string
		if err := waitFor(ctx, "the webhook to answer "+c.what, 30*time.Second, func(ctx context.Context) (bool, error) {
			got, err := tlsPost(ctx, "https://"+addr+"/validate", reviewFor(c.uid, env.Namespace, c.object))
			if err != nil {
				return false, nil
			}
			body = got
			return true, nil
		}); err != nil {
			return fmt.Errorf("asking about %s got no answer: %w — an HTTP error is not a verdict, so the API server treats it as an unreachable webhook\nthe program said:\n%s", c.what, err, tail(p.Stdout()))
		}

		if !strings.Contains(body, c.uid) {
			return fmt.Errorf("the answer about %s does not carry the request's uid %s, so the API server would discard it:\n%s", c.what, c.uid, tail(body))
		}
		if !strings.Contains(body, `"kind":"AdmissionReview"`) && !strings.Contains(body, `"kind": "AdmissionReview"`) {
			return fmt.Errorf("the answer about %s is not an AdmissionReview:\n%s", c.what, tail(body))
		}
		if !strings.Contains(body, "\"allowed\"") {
			return fmt.Errorf("the answer about %s carries no verdict:\n%s", c.what, tail(body))
		}
	}
	return nil
}

func stageDeny(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, ok, err := findRegistration(ctx, env, url)
		return ok, err
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	const bad = "no-owner"
	var refusal string
	if err := waitFor(ctx, "the API server to refuse a pod with no owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := env.SeedPods(ctx, bad)
		if err == nil {
			// It got through: delete it and try again, in case the
			// registration had not taken effect yet.
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, bad, metav1.DeleteOptions{})
			return false, nil
		}
		refusal = err.Error()
		return strings.Contains(refusal, "denied the request"), nil
	}); err != nil {
		return fmt.Errorf("pod %q carries no owner label and the API server never refused it: either the webhook allowed it, or the call never produced a verdict at all — a webhook the API server cannot reach fails the write without ever saying it denied the request: %w\nthe program said:\n%s", bad, err, tail(p.Stdout()))
	}

	// A refusal with an empty message is the shape of a webhook that says no
	// and never says why.
	parts := strings.SplitN(refusal, "denied the request:", 2)
	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		return fmt.Errorf("the pod was refused with no message — the message is the only explanation whoever applied it will see")
	}

	const good = "has-owner"
	if err := env.SeedLabeledPod(ctx, good, map[string]string{"owner": "byok8s"}); err != nil {
		return fmt.Errorf("pod %q carries an owner label and was still rejected: %w\nthe program said:\n%s", good, err, tail(p.Stdout()))
	}
	return nil
}

// --- helpers ---------------------------------------------------------------

// launch starts the learner's webhook with a kubeconfig scoped to this stage's
// namespace, the port it should listen on, and the name the API server will
// reach it by.
func launch(ctx context.Context, env *kube.Env, bin string, port int) (*runner.Process, func(), error) {
	kc, cleanupEnv, err := scoped(env)
	if err != nil {
		return nil, nil, err
	}
	args := []string{
		fmt.Sprintf("--addr=0.0.0.0:%d", port),
		fmt.Sprintf("--external-host=%s", externalHost),
	}
	p, err := runner.Start(ctx, bin, kc, args...)
	if err != nil {
		cleanupEnv()
		return nil, nil, err
	}
	return p, func() {
		p.Stop(5 * time.Second)
		cleanupEnv()
	}, nil
}

// scoped writes a kubeconfig whose context selects this stage's namespace and
// returns it as an environment entry, so the program works in its own
// namespace without needing a flag it has not learned yet.
func scoped(env *kube.Env) ([]string, func(), error) {
	dir, err := os.MkdirTemp("", "byok8s-stage-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	path, err := env.KubeconfigScoped(dir)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return []string{"KUBECONFIG=" + path}, cleanup, nil
}

// freePort asks the kernel for a port nothing is using.
//
// The port has to be known before the program starts — it goes into the
// webhook's registration — and a fixed one would collide with whatever the
// last stage left behind.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find a free port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// insecureClient trusts whatever certificate the webhook presents.
//
// The API server is told to trust it through a caBundle; the harness has no
// such bundle and does not need one, because what is being graded is that the
// program serves TLS at all, not that this machine trusts it.
func insecureClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
}

func tlsGet(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	return send(insecureClient(), req)
}

func tlsPost(ctx context.Context, url, body string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	return send(insecureClient(), req)
}

func plainGet(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	return send(&http.Client{Timeout: 10 * time.Second}, req)
}

func send(client *http.Client, req *http.Request) (string, error) {
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	if res.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("%s: %s", req.URL, res.Status)
	}
	return string(body), nil
}

// servedLeaf opens one TLS connection and returns the certificate the program
// presented on it.
//
// A fresh connection every time is the point: a rotated certificate only shows
// up on a new handshake, and a pooled client would go on being answered with
// the old one.
func servedLeaf(ctx context.Context, addr string) (*x509.Certificate, error) {
	d := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true}}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	certs := conn.(*tls.Conn).ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return nil, fmt.Errorf("the program completed a handshake without presenting a certificate")
	}
	return certs[0], nil
}

// verifyAgainst reports whether a certificate chains to the given roots under the
// name the API server dials the webhook by.
//
// The name matters: a certificate valid for some other host verifies fine here
// and is still refused in the cluster.
func verifyAgainst(cert *x509.Certificate, roots *x509.CertPool) error {
	_, err := cert.Verify(x509.VerifyOptions{
		Roots:     roots,
		DNSName:   externalHost,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	return err
}

// awaitLine waits for the program to print something.
func awaitLine(ctx context.Context, p *runner.Process, want string, within time.Duration) error {
	return waitFor(ctx, fmt.Sprintf("the program to print %q", want), within, func(ctx context.Context) (bool, error) {
		if strings.Contains(p.Stdout(), want) {
			return true, nil
		}
		if done, res := p.Exited(); done {
			return false, fmt.Errorf("the program exited (%d) before printing it:\n%s", res.ExitCode, tail(res.Stderr))
		}
		return false, nil
	})
}

func waitFor(ctx context.Context, what string, within time.Duration, cond wait.ConditionWithContextFunc) error {
	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	if err := kube.WaitFor(ctx, what, cond); err != nil {
		return err
	}
	return nil
}

// tail keeps an error message readable when the program has been talkative.
func tail(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > 12 {
		lines = append([]string{"  …"}, lines[len(lines)-12:]...)
	}
	for i, l := range lines {
		if !strings.HasPrefix(l, "  ") {
			lines[i] = "  " + l
		}
	}
	return strings.Join(lines, "\n")
}

// findRegistration returns the webhook, within any configuration in the
// cluster, whose clientConfig URL is the one this stage expects.
func findRegistration(ctx context.Context, env *kube.Env, url string) (admissionregistrationv1.ValidatingWebhook, bool, error) {
	list, err := env.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return admissionregistrationv1.ValidatingWebhook{}, false, fmt.Errorf("list webhook configurations: %w", err)
	}

	for _, item := range list.Items {
		for _, w := range item.Webhooks {
			if w.ClientConfig.URL != nil && *w.ClientConfig.URL == url {
				return w, true, nil
			}
		}
	}
	return admissionregistrationv1.ValidatingWebhook{}, false, nil
}

// stageMutatePatch checks the webhook changes an object instead of judging it.
//
// The proof is the object the cluster stored, not the reply: a patch that is
// malformed, wrongly typed, or double-encoded is dropped by the API server
// without complaint, and only the stored pod shows whether it landed.
func stageMutatePatch(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/mutate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	if err := waitFor(ctx, "the program to register a mutating webhook", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, ok, err := findMutatingRegistration(ctx, env, url)
		return ok, err
	}); err != nil {
		return fmt.Errorf("no MutatingWebhookConfiguration points at %s: %w", url, err)
	}

	// The pod carries an owner so the judging half admits it: what is under
	// test here is the patch, not the verdict.
	const name = "patched"
	const annotation = "byok8s.dev/injected"
	if err := waitFor(ctx, "the API server to store a patched pod", 60*time.Second, func(ctx context.Context) (bool, error) {
		if err := env.SeedLabeledPod(ctx, name, map[string]string{"owner": "platform"}); err != nil {
			return false, nil
		}
		pod, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.Annotations[annotation] == "true" {
			return true, nil
		}
		// The registration may not have taken effect yet, and a pod is only
		// patched on the way in: judge a fresh one.
		_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
		return false, nil
	}); err != nil {
		return fmt.Errorf("pod %q was stored without the %s annotation, so the patch never reached it: %w\nthe program said:\n%s", name, annotation, err, tail(p.Stdout()))
	}

	// Patching must not become a way of admitting what the policy refuses.
	if err := waitFor(ctx, "the API server to refuse a pod with no owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := env.SeedPods(ctx, "no-owner")
		if err == nil {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, "no-owner", metav1.DeleteOptions{})
			return false, nil
		}
		return strings.Contains(err.Error(), "denied the request"), nil
	}); err != nil {
		return fmt.Errorf("a pod with no owner label was admitted, so the mutating half is answering a question the validating half exists to ask: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	return nil
}

// stageDefaulting checks the webhook supplies what an object left out.
//
// Defaulting is only safe on a field no policy refuses, so this also checks the
// gap stage 4 exists to catch is still a gap: a defaulter that fills in what
// the validating half judges disarms it silently.
func stageDefaulting(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/mutate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	if err := waitFor(ctx, "the program to register a mutating webhook", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, ok, err := findMutatingRegistration(ctx, env, url)
		return ok, err
	}); err != nil {
		return fmt.Errorf("no MutatingWebhookConfiguration points at %s: %w", url, err)
	}

	const name = "defaulted"
	const want = "100m"
	if err := waitFor(ctx, "the API server to store a pod with a defaulted CPU request", 60*time.Second, func(ctx context.Context) (bool, error) {
		if err := env.SeedLabeledPod(ctx, name, map[string]string{"owner": "platform"}); err != nil {
			return false, nil
		}
		pod, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if cpu := pod.Spec.Containers[0].Resources.Requests.Cpu(); cpu != nil && cpu.String() == want {
			return true, nil
		}
		// A pod is only defaulted on the way in, so a fresh one is the only way
		// to retry once the registration has taken effect.
		_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
		return false, nil
	}); err != nil {
		return fmt.Errorf("pod %q was stored with no CPU request, so nothing defaulted it to %s: %w\nthe program said:\n%s", name, want, err, tail(p.Stdout()))
	}

	// Its own output, fed back: an update re-runs the mutation over a pod that
	// already carries the default, and must leave it exactly as it was.
	// A conflict is the cluster writing to the pod underneath this, not a
	// verdict, so it is retried on a fresh copy; anything else is the webhook
	// actually refusing the update.
	if err := waitFor(ctx, "an update to an already-defaulted pod to be accepted", 60*time.Second, func(ctx context.Context) (bool, error) {
		pod, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		pod.Labels["byok8s.dev/round"] = "two"
		if _, err := env.Client.CoreV1().Pods(env.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("an update to an already-defaulted pod was refused, so applying the mutation to its own output did not leave the pod alone: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	updated, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read pod %s: %w", name, err)
	}
	if cpu := updated.Spec.Containers[0].Resources.Requests.Cpu(); cpu == nil || cpu.String() != want {
		return fmt.Errorf("the CPU request became %v after an update, so applying the default to its own output did not leave it alone", updated.Spec.Containers[0].Resources.Requests.Cpu())
	}

	// Defaulting must not become a way of admitting what the policy refuses.
	if err := waitFor(ctx, "the API server to refuse a pod with no owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := env.SeedPods(ctx, "no-owner")
		if err == nil {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, "no-owner", metav1.DeleteOptions{})
			return false, nil
		}
		return strings.Contains(err.Error(), "denied the request"), nil
	}); err != nil {
		return fmt.Errorf("a pod with no owner label was admitted, so the defaulter is filling in a field the policy exists to judge: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	return nil
}

// stageSidecarInject checks the webhook adds a container, once.
//
// The failure this guards against is not a missing sidecar but a doubled one:
// appending without looking gives the pod two containers with the same name,
// and the error the user sees points at their pod instead of at the webhook.
func stageSidecarInject(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/mutate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	if err := waitFor(ctx, "the program to register a mutating webhook", 60*time.Second, func(ctx context.Context) (bool, error) {
		_, ok, err := findMutatingRegistration(ctx, env, url)
		return ok, err
	}); err != nil {
		return fmt.Errorf("no MutatingWebhookConfiguration points at %s: %w", url, err)
	}

	const (
		meshed  = "meshed"
		sidecar = "sidecar"
	)
	owner := map[string]string{"owner": "platform"}
	optIn := map[string]string{"byok8s.dev/inject": "true"}

	if err := waitFor(ctx, "the API server to store a pod carrying an injected sidecar", 60*time.Second, func(ctx context.Context) (bool, error) {
		if err := seedPod(ctx, env, meshed, owner, optIn); err != nil {
			return false, nil
		}
		pod, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, meshed, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if containerNamed(pod, sidecar) {
			return true, nil
		}
		// A pod is only injected on the way in, so retrying means a fresh one.
		_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, meshed, metav1.DeleteOptions{})
		return false, nil
	}); err != nil {
		return fmt.Errorf("pod %q asked for a sidecar and was stored without one: %w\nthe program said:\n%s", meshed, err, tail(p.Stdout()))
	}

	// Asking is what makes it happen: a pod that never opted in keeps the shape
	// its author wrote.
	const plain = "unmeshed"
	if err := seedPod(ctx, env, plain, owner, nil); err != nil {
		return fmt.Errorf("create pod %s: %w", plain, err)
	}
	untouched, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, plain, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read pod %s: %w", plain, err)
	}
	if containerNamed(untouched, sidecar) {
		return fmt.Errorf("pod %q never asked for a sidecar and was given one anyway, so injection is opt-out rather than opt-in", plain)
	}

	// Its own output, fed back: the pod already carries the sidecar, and a
	// second look must not try to append it again.
	if err := waitFor(ctx, "an update to an already-injected pod to be accepted", 60*time.Second, func(ctx context.Context) (bool, error) {
		pod, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, meshed, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		pod.Labels["byok8s.dev/round"] = "two"
		if _, err := env.Client.CoreV1().Pods(env.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("an update to a pod that already carries the sidecar was refused, so the injection tried to happen twice: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	final, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, meshed, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read pod %s: %w", meshed, err)
	}
	count := 0
	for _, c := range final.Spec.Containers {
		if c.Name == sidecar {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("pod %q carries %d containers named %q, so the injection is not idempotent", meshed, count, sidecar)
	}

	// Injecting must not become a way of admitting what the policy refuses.
	if err := waitFor(ctx, "the API server to refuse a pod with no owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := env.SeedPods(ctx, "no-owner")
		if err == nil {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, "no-owner", metav1.DeleteOptions{})
			return false, nil
		}
		return strings.Contains(err.Error(), "denied the request"), nil
	}); err != nil {
		return fmt.Errorf("a pod with no owner label was admitted, so the injecting half is answering a question the validating half exists to ask: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	return nil
}

// stageTimeout checks the deadline is real and that the handler respects it.
//
// A webhook that is merely slow is worse than one that is down: the caller pays
// the full deadline every time, and mutating webhooks pay it one after another.
func stageTimeout(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	if hook.TimeoutSeconds == nil {
		return fmt.Errorf("the webhook sets no timeoutSeconds, so every matching request waits the default ten seconds on a program that should answer in milliseconds")
	}
	if *hook.TimeoutSeconds < 1 || *hook.TimeoutSeconds > 5 {
		return fmt.Errorf("the webhook declares timeoutSeconds %d: set the deadline you actually meet rather than the maximum, because mutating webhooks are called one after another and every chain member spends this in turn", *hook.TimeoutSeconds)
	}

	// The baseline: an ordinary pod is judged promptly, so the delay below is
	// the only thing that changed.
	const prompt = "prompt"
	if err := waitFor(ctx, "the API server to admit an ordinary pod", 60*time.Second, func(ctx context.Context) (bool, error) {
		return seedPod(ctx, env, prompt, map[string]string{"owner": "platform"}, nil) == nil, nil
	}); err != nil {
		return fmt.Errorf("a pod carrying an owner label was not admitted while the webhook was answering normally: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	// A pod that asks to be judged slowly. The sleep is far longer than the
	// deadline, so the deadline is what decides how long anyone waits.
	const slow = "slow"
	deadline := time.Duration(*hook.TimeoutSeconds) * time.Second
	start := time.Now()
	err = seedPod(ctx, env, slow, map[string]string{"owner": "platform"}, map[string]string{"byok8s.dev/delay": "60s"})
	elapsed := time.Since(start)

	if err == nil {
		_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, slow, metav1.DeleteOptions{})
		return fmt.Errorf("a pod was admitted although the webhook never answered within its deadline, so either the delay was ignored or the failure policy is not Fail\nthe program said:\n%s", tail(p.Stdout()))
	}
	if !strings.Contains(err.Error(), "failed calling webhook") {
		return fmt.Errorf("the write failed while the webhook was slow, but not because the API server gave up waiting: %w", err)
	}
	if elapsed > deadline+15*time.Second {
		return fmt.Errorf("the API server waited %s on a webhook declaring a %s deadline, so the bound is not being applied", elapsed.Round(time.Second), deadline)
	}

	// The other half of the deadline: the handler noticed it was abandoned
	// rather than working on for the full sleep.
	if err := waitFor(ctx, "the handler to notice the caller stopped waiting", 30*time.Second, func(ctx context.Context) (bool, error) {
		return strings.Contains(p.Stdout(), "gave up"), nil
	}); err != nil {
		return fmt.Errorf("the API server gave up on the slow pod, but the handler kept going: a handler that outlives its caller is deciding something nobody will read: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	// A deadline is not an excuse to stop judging.
	if err := waitFor(ctx, "the webhook to refuse a pod with no owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := seedPod(ctx, env, "no-owner", nil, nil)
		if err == nil {
			return false, fmt.Errorf("a pod carrying no owner label was admitted")
		}
		return strings.Contains(err.Error(), "denied the request"), nil
	}); err != nil {
		return fmt.Errorf("the webhook stopped refusing pods with no owner label: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	return nil
}

// stageReinvocation checks the webhook survives being called twice for one
// request.
//
// Mutating webhooks run in an order nobody chooses, so a later webhook can
// change the object this one just decided about. Asking to be reinvoked is the
// fix, and it is only safe if the second call is a no-op.
func stageReinvocation(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	mutateURL := fmt.Sprintf("https://%s:%d/mutate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.MutatingWebhook
	if err := waitFor(ctx, "the program to register its mutating half", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findMutatingRegistration(ctx, env, mutateURL)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no MutatingWebhookConfiguration points at %s: %w", mutateURL, err)
	}

	if hook.ReinvocationPolicy == nil || *hook.ReinvocationPolicy != admissionregistrationv1.IfNeededReinvocationPolicy {
		return fmt.Errorf("the mutating webhook declares reinvocationPolicy %v: without IfNeeded, a webhook that runs after this one can undo its patch and it never finds out", hook.ReinvocationPolicy)
	}

	// The pod that opts in to everything this program does: an annotation to
	// add, a CPU request to default, and a sidecar to append. A second pass
	// that is not a no-op shows up here as duplicated work.
	const opted = "reinvoked"
	if err := waitFor(ctx, "the API server to admit a pod that opts in to injection", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := seedPod(ctx, env, opted, map[string]string{"owner": "platform"}, map[string]string{"byok8s.dev/inject": "true"})
		return err == nil, nil
	}); err != nil {
		return fmt.Errorf("a pod carrying an owner label and asking for a sidecar was not admitted: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	pod, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, opted, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read pod %s: %w", opted, err)
	}

	sidecars := 0
	for _, c := range pod.Spec.Containers {
		if c.Name == "sidecar" {
			sidecars++
		}
	}
	if sidecars != 1 {
		return fmt.Errorf("the stored pod has %d containers named sidecar: an injector that appends without looking first turns a second pass into a second sidecar", sidecars)
	}
	if got := pod.Annotations["byok8s.dev/injected"]; got != "true" {
		return fmt.Errorf("the stored pod is annotated byok8s.dev/injected=%q, so the mutation did not survive the pass that followed it", got)
	}

	// Feeding the webhook its own output is the cheapest way to know a second
	// pass is safe, and it does not depend on another webhook existing to
	// trigger one.
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	const replayUID = "55555555-0000-4000-8000-000000000015"
	// The empty resources object is not decoration: the API server serialises
	// it on every container, and a patch that adds a path underneath it needs
	// the parent already there. Leaving it out tests a document the webhook
	// will never be sent.
	object := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"replayed","annotations":{"byok8s.dev/inject":"true"}},"spec":{"containers":[{"name":"app","image":"registry.k8s.io/pause:3.9","resources":{}}]}}`

	patched, changed, err := applyMutation(ctx, addr, replayUID, env.Namespace, object)
	if err != nil {
		return fmt.Errorf("asking the webhook about a pod that opts in to injection: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if !changed {
		return fmt.Errorf("the webhook returned no patch for a pod that asks for a sidecar and requests no CPU, so this stage has no first pass to repeat\nthe program said:\n%s", tail(p.Stdout()))
	}

	if _, again, err := applyMutation(ctx, addr, replayUID, env.Namespace, patched); err != nil {
		return fmt.Errorf("asking the webhook about its own output: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	} else if again {
		return fmt.Errorf("handed back the object it just produced, the webhook patched it a second time: under IfNeeded that second call really happens, and a patch that is not empty the second time does its work twice")
	}

	// Being called twice is not an excuse to stop judging.
	if err := waitFor(ctx, "the webhook to refuse a pod with no owner label", 60*time.Second, func(ctx context.Context) (bool, error) {
		err := seedPod(ctx, env, "no-owner", nil, nil)
		if err == nil {
			return false, fmt.Errorf("a pod carrying no owner label was admitted")
		}
		return strings.Contains(err.Error(), "denied the request"), nil
	}); err != nil {
		return fmt.Errorf("the webhook stopped refusing pods with no owner label: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	return nil
}

// stageFailurePolicy checks what the cluster does while the webhook is down.
//
// The declaration alone proves nothing, so this stage takes the program away
// without letting it unregister — a crash, not a shutdown — and asks the API
// server to write something the rules match.
func stageFailurePolicy(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	if hook.FailurePolicy == nil || *hook.FailurePolicy != admissionregistrationv1.Fail {
		return fmt.Errorf("the webhook declares failurePolicy %v, so killing this program is enough to get an unjudged pod into the cluster: a policy that must not be bypassed fails closed", hook.FailurePolicy)
	}

	// The baseline: while the program is up, a pod that satisfies the policy is
	// admitted. Without this, the refusal below could be about the pod.
	const healthy = "before-outage"
	if err := waitFor(ctx, "the API server to admit a pod while the webhook is up", 60*time.Second, func(ctx context.Context) (bool, error) {
		return seedPod(ctx, env, healthy, map[string]string{"owner": "platform"}, nil) == nil, nil
	}); err != nil {
		return fmt.Errorf("a pod carrying an owner label was not admitted while the webhook was running, so this stage cannot tell an outage from a refusal: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	// A crash, not a shutdown: SIGKILL leaves the registration pointing at a
	// port with nothing behind it.
	p.Kill()

	if _, ok, err := findRegistration(ctx, env, url); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("the registration disappeared when the program was killed, so nothing was left to fail closed")
	}

	// Now the real question: the API server cannot get an answer, and the pod
	// is a perfectly good one.
	const during = "during-outage"
	err = seedPod(ctx, env, during, map[string]string{"owner": "platform"}, nil)
	if err == nil {
		_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, during, metav1.DeleteOptions{})
		return fmt.Errorf("a pod was written while the webhook was unreachable, so the policy is advisory: anything created during an outage goes in unjudged and nothing re-checks it later")
	}
	if !strings.Contains(err.Error(), "failed calling webhook") {
		return fmt.Errorf("the write failed while the webhook was down, but not because the API server could not reach it, so this proves nothing about the failure policy: %w", err)
	}

	return nil
}

// stageDryRun checks a hypothetical request stays hypothetical.
//
// Two things have to hold at once, and they pull in opposite directions: the
// verdict must not depend on whether the request is real, and the side effect
// must.
func stageDryRun(ctx context.Context, env *kube.Env, bin string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s:%d/validate", externalHost, port)

	if err := sweepRegistrations(ctx, env); err != nil {
		return err
	}
	defer sweepRegistrations(context.WithoutCancel(ctx), env)

	p, cleanup, err := launch(ctx, env, bin, port)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "serving", 60*time.Second); err != nil {
		return err
	}

	var hook admissionregistrationv1.ValidatingWebhook
	if err := waitFor(ctx, "the program to register itself", 60*time.Second, func(ctx context.Context) (bool, error) {
		found, ok, err := findRegistration(ctx, env, url)
		if err != nil || !ok {
			return false, err
		}
		hook = found
		return true, nil
	}); err != nil {
		return fmt.Errorf("no ValidatingWebhookConfiguration points at %s: %w", url, err)
	}

	// The declaration is a contract the API server enforces by refusing dry-run
	// requests outright, so a webhook that keeps a record has to say so.
	if hook.SideEffects == nil || *hook.SideEffects != admissionregistrationv1.SideEffectClassNoneOnDryRun {
		return fmt.Errorf("the webhook declares sideEffects %v, but it writes down every verdict: a webhook with a side effect it suppresses on a dry run declares NoneOnDryRun", hook.SideEffects)
	}

	const records = "byok8s-admissions"
	owner := map[string]string{"owner": "platform"}

	// A real request is recorded, which is what makes the dry-run check below
	// mean anything: the absence of a record has to be evidence.
	const real = "recorded"
	if err := waitFor(ctx, "the webhook to record a real admission", 60*time.Second, func(ctx context.Context) (bool, error) {
		if err := seedPod(ctx, env, real, owner, nil); err != nil {
			return false, nil
		}
		cm, err := env.Client.CoreV1().ConfigMaps(env.Namespace).Get(ctx, records, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return cm.Data[real] != "", nil
	}); err != nil {
		return fmt.Errorf("a pod was admitted but nothing was written to the %s ConfigMap, so this webhook has no side effect for a dry run to suppress: %w\nthe program said:\n%s", records, err, tail(p.Stdout()))
	}

	// The hypothetical pod: admitted, stored nowhere, recorded nowhere.
	const ghost = "ghost"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: ghost, Namespace: env.Namespace, Labels: owner},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		return fmt.Errorf("a dry-run create of a pod carrying an owner label was refused, so the verdict changes when the request is hypothetical: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, ghost, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return fmt.Errorf("pod %q exists after a dry-run create, which is the API server's job rather than this webhook's, but the rest of this stage cannot be trusted while it does", ghost)
	}

	cm, err := env.Client.CoreV1().ConfigMaps(env.Namespace).Get(ctx, records, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read ConfigMap %s: %w", records, err)
	}
	if got, ok := cm.Data[ghost]; ok {
		return fmt.Errorf("a dry-run create left %q recorded as %q, so the webhook did the side effect for a request that never happened", ghost, got)
	}

	// The verdict is the same either way: a dry run that reports success for a
	// pod the real path would refuse is worse than not supporting dry run.
	const phantom = "phantom"
	refused := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: phantom, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	_, err = env.Client.CoreV1().Pods(env.Namespace).Create(ctx, refused, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err == nil {
		_ = env.Client.CoreV1().Pods(env.Namespace).Delete(ctx, phantom, metav1.DeleteOptions{})
		return fmt.Errorf("a dry-run create of a pod with no owner label was admitted, though a real one is refused: --dry-run=server now reports a success the cluster would not give\nthe program said:\n%s", tail(p.Stdout()))
	}
	if !strings.Contains(err.Error(), "denied the request") {
		return fmt.Errorf("a dry-run create of a pod with no owner label failed for the wrong reason, so the webhook is not what refused it: %w", err)
	}

	return nil
}

// containerNamed reports whether a pod carries a container with this name.
func containerNamed(pod *corev1.Pod, name string) bool {
	for _, c := range pod.Spec.Containers {
		if c.Name == name {
			return true
		}
	}
	return false
}

// applyMutation asks the webhook about one object and returns the object as the
// webhook would leave it, along with whether it asked for any change at all.
//
// The patch arrives base64-encoded inside a JSON string, which a []byte field
// decodes for us — a patch that looks like gibberish in the reply is usually a
// program that encoded it twice.
func applyMutation(ctx context.Context, addr, uid, namespace, object string) (string, bool, error) {
	body, err := tlsPost(ctx, "https://"+addr+"/mutate", reviewFor(uid, namespace, object))
	if err != nil {
		return "", false, err
	}

	var review struct {
		Response struct {
			Allowed bool   `json:"allowed"`
			Patch   []byte `json:"patch"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return "", false, fmt.Errorf("the reply is not an AdmissionReview: %w\n%s", err, tail(body))
	}
	if !review.Response.Allowed {
		return "", false, fmt.Errorf("the mutating webhook refused the object: mutation is not the place to say no")
	}
	if len(review.Response.Patch) == 0 {
		return object, false, nil
	}

	patch, err := jsonpatch.DecodePatch(review.Response.Patch)
	if err != nil {
		return "", false, fmt.Errorf("the reply carries something that is not a JSON patch: %w", err)
	}
	out, err := patch.Apply([]byte(object))
	if err != nil {
		return "", false, fmt.Errorf("the patch does not apply to the object it was built from: %w", err)
	}
	return string(out), true, nil
}

// seedPod creates a pod carrying the given labels and annotations.
func seedPod(ctx context.Context, env *kube.Env, name string, labels, annotations map[string]string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   env.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	_, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// findMutatingRegistration finds the mutating webhook pointing at a URL.
func findMutatingRegistration(ctx context.Context, env *kube.Env, url string) (admissionregistrationv1.MutatingWebhook, bool, error) {
	list, err := env.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return admissionregistrationv1.MutatingWebhook{}, false, fmt.Errorf("list mutating webhook configurations: %w", err)
	}

	for _, item := range list.Items {
		for _, w := range item.Webhooks {
			if w.ClientConfig.URL != nil && *w.ClientConfig.URL == url {
				return w, true, nil
			}
		}
	}
	return admissionregistrationv1.MutatingWebhook{}, false, nil
}

// sweepRegistrations deletes every configuration that points at this machine.
//
// A ValidatingWebhookConfiguration is cluster-scoped, so it outlives the
// namespace a stage runs in: one left behind sends the API server to a port
// nothing is listening on.
func sweepRegistrations(ctx context.Context, env *kube.Env) error {
	list, err := env.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list webhook configurations: %w", err)
	}

	for _, item := range list.Items {
		for _, w := range item.Webhooks {
			if w.ClientConfig.URL != nil && strings.Contains(*w.ClientConfig.URL, externalHost) {
				err := env.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(ctx, item.Name, metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("delete webhook configuration %s: %w", item.Name, err)
				}
				break
			}
		}
	}

	mutating, err := env.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list mutating webhook configurations: %w", err)
	}

	for _, item := range mutating.Items {
		for _, w := range item.Webhooks {
			if w.ClientConfig.URL != nil && strings.Contains(*w.ClientConfig.URL, externalHost) {
				err := env.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, item.Name, metav1.DeleteOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("delete mutating webhook configuration %s: %w", item.Name, err)
				}
				break
			}
		}
	}
	return nil
}

// admitsPods reports whether a rule in the registration covers creating a pod.
func admitsPods(hook admissionregistrationv1.ValidatingWebhook) bool {
	for _, r := range hook.Rules {
		hasCreate := slices.Contains(r.Operations, admissionregistrationv1.Create) || slices.Contains(r.Operations, admissionregistrationv1.OperationAll)
		hasPods := slices.Contains(r.Resources, "pods") || slices.Contains(r.Resources, "*")
		hasCoreGroup := slices.Contains(r.APIGroups, "") || slices.Contains(r.APIGroups, "*")

		if hasCreate && hasPods && hasCoreGroup {
			return true
		}
	}
	return false
}

// reviewFor builds an AdmissionReview asking about one pod, with the object
// inlined as raw JSON so a stage can ask about something that is not a valid
// pod at all.
func reviewFor(uid, namespace, object string) string {
	return fmt.Sprintf(`{
	  "apiVersion": "admission.k8s.io/v1",
	  "kind": "AdmissionReview",
	  "request": {
	    "uid": %q,
	    "operation": "CREATE",
	    "resource": {"group": "", "version": "v1", "resource": "pods"},
	    "namespace": %q,
	    "object": %s
	  }
	}`, uid, namespace, object)
}

// podObject renders a pod as the raw JSON that goes inside an AdmissionReview.
func podObject(name, namespace string, labels, annotations map[string]string) (string, error) {
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		return "", fmt.Errorf("render pod %s: %w", name, err)
	}
	return string(raw), nil
}

// verdict is the part of an AdmissionResponse a stage reads when it asks the
// program directly rather than through the API server.
type verdict struct {
	Allowed          bool              `json:"allowed"`
	AuditAnnotations map[string]string `json:"auditAnnotations"`
	Warnings         []string          `json:"warnings"`
}

// askVerdict posts one hand-built AdmissionReview and decodes the answer.
func askVerdict(ctx context.Context, addr, uid, namespace, object string) (verdict, error) {
	body, err := tlsPost(ctx, fmt.Sprintf("https://%s/validate", addr), reviewFor(uid, namespace, object))
	if err != nil {
		return verdict{}, fmt.Errorf("asking about %s got no answer: %w", uid, err)
	}
	var review struct {
		Response verdict `json:"response"`
	}
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return verdict{}, fmt.Errorf("the reply to %s is not an AdmissionReview: %w\n%s", uid, err, tail(body))
	}
	return review.Response, nil
}

// warningsFromCreate creates a pod with a client that keeps the Warning:
// headers instead of dropping them.
//
// The course's own client sets rest.NoWarnings so stage output stays readable,
// which is exactly the channel this stage needs to watch.
func warningsFromCreate(ctx context.Context, env *kube.Env, name string, labels, annotations map[string]string) ([]string, error) {
	recorder := &warningRecorder{}
	cfg := rest.CopyConfig(env.Config)
	cfg.WarningHandler = recorder

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build a client that keeps warnings: %w", err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: annotations},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("create pod %s: %w", name, err)
	}
	return recorder.seen, nil
}

// warningRecorder collects what the API server warned about. The handler runs
// inline on the request's own goroutine, so the slice needs no lock.
type warningRecorder struct{ seen []string }

func (r *warningRecorder) HandleWarningHeader(code int, _ string, text string) {
	if code != 299 || text == "" {
		return
	}
	r.seen = append(r.seen, text)
}

// clientAs builds a client that acts as another user through impersonation.
func clientAs(env *kube.Env, user string) (kubernetes.Interface, error) {
	cfg := rest.CopyConfig(env.Config)
	cfg.Impersonate = rest.ImpersonationConfig{UserName: user}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build a client impersonating %s: %w", user, err)
	}
	return client, nil
}

// grantNamespaceWrite lets an impersonated user write pods in the stage's
// namespace.
//
// Impersonation changes who the API server thinks is asking, and an identity
// with no RBAC is refused before admission runs at all — a refusal that would
// read exactly like the webhook's own.
func grantNamespaceWrite(ctx context.Context, env *kube.Env, user string) error {
	const name = "byok8s-exempt-writer"

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"create", "get", "list", "delete"},
		}},
	}
	if _, err := env.Client.RbacV1().Roles(env.Namespace).Create(ctx, role, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create a role for %s: %w", user, err)
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, Name: user, APIGroup: rbacv1.GroupName}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
	}
	if _, err := env.Client.RbacV1().RoleBindings(env.Namespace).Create(ctx, binding, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("bind that role to %s: %w", user, err)
	}
	return nil
}

// deletePod removes a pod left by an earlier run, so that a create which must
// be judged is a real create.
//
// Stage namespaces outlive the stage and seedPod treats an existing pod as
// success, which would otherwise let a replay pass without the API server ever
// calling the webhook.
func deletePod(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
	grace := int64(0)
	err := client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pod %s: %w", name, err)
	}
	return waitFor(ctx, fmt.Sprintf("pod %s to go away", name), 60*time.Second, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
}
