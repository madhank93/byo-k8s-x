package apiserver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
)

func init() {
	register(Stage{Slug: "admission-chain", Run: stageAdmissionChain})
}

const (
	admitAlice      = "Bearer alice-token-0001"
	admitMutating   = "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations"
	admitValidating = "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations"
	admitConfigMaps = "/api/v1/namespaces/default/configmaps"
	admitRCs        = "/api/v1/namespaces/default/replicationcontrollers"
)

// stageAdmissionChain checks the server hands every write to the webhooks
// configured for it before storing it: mutating ones in order, each seeing
// the last one's change, then validating ones, any of which can refuse.
//
// The webhooks are this harness, serving HTTPS under a CA of its own that the
// configurations name in caBundle. Every assertion is about what the server
// sent them, what it stored after, and what it answered the client.
func stageAdmissionChain(ctx context.Context, _ *kube.Env, bin string) error {
	hooks, err := startHooks()
	if err != nil {
		return err
	}
	defer hooks.close()

	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the token file: %w", err)
	}
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "tokens.csv")
	if err := os.WriteFile(file, []byte(authnTokens), 0o600); err != nil {
		return fmt.Errorf("write the token file: %w", err)
	}
	srv, cleanup, err := serve(ctx, bin, "-token-auth-file", file)
	if err != nil {
		return err
	}
	defer cleanup()

	// With nothing configured, admission is nothing at all.
	if _, err := admitCreate(ctx, srv, admitConfigMaps, admitConfigMap("before", nil)); err != nil {
		return fmt.Errorf("%w\n\nwith no webhook configurations a write goes through exactly as it did before this stage", err)
	}

	// Registered out of name order on purpose: the order webhooks run in is
	// by configuration name, not by when each configuration arrived.
	cmRules := []string{"CREATE", "UPDATE"}
	for _, config := range []map[string]any{
		admitConfig("MutatingWebhookConfiguration", "b-second",
			hooks.webhook("b1.admission.test", "b1", cmRules, "configmaps", nil)),
		admitConfig("MutatingWebhookConfiguration", "a-first",
			hooks.webhook("a1.admission.test", "a1", cmRules, "configmaps", nil),
			hooks.webhook("a2.admission.test", "a2", cmRules, "configmaps", nil)),
	} {
		if err := admitRegister(ctx, srv, admitMutating, config); err != nil {
			return err
		}
	}
	guard := hooks.webhook("guard.admission.test", "guard", []string{"CREATE", "UPDATE", "DELETE"}, "configmaps", map[string]any{
		"objectSelector": map[string]any{"matchLabels": map[string]any{"guarded": "yes"}},
	})
	if err := admitRegister(ctx, srv, admitValidating, admitConfig("ValidatingWebhookConfiguration", "guard", guard)); err != nil {
		return err
	}

	if err := admitCreateChain(ctx, srv, hooks); err != nil {
		return err
	}
	if err := admitDeny(ctx, srv, hooks); err != nil {
		return err
	}
	if err := admitUpdates(ctx, srv, hooks); err != nil {
		return err
	}
	if err := admitDelete(ctx, srv, hooks); err != nil {
		return err
	}
	if err := admitUnmatched(ctx, srv, hooks); err != nil {
		return err
	}
	if err := admitFailures(ctx, srv, hooks); err != nil {
		return err
	}
	if err := admitSelectAndValidate(ctx, srv, hooks); err != nil {
		return err
	}
	return admitTimeout(ctx, srv, hooks)
}

// admitCreateChain checks a create passes through all three mutating webhooks
// in order and the validating one sees what they made of it.
func admitCreateChain(ctx context.Context, srv *server, hooks *hookServer) error {
	hooks.reset()
	created, err := admitCreate(ctx, srv, admitConfigMaps, admitConfigMap("chained", map[string]any{"guarded": "yes"}))
	if err != nil {
		return err
	}
	for _, hook := range []string{"a1", "a2", "b1", "guard"} {
		calls := hooks.calls(hook)
		if len(calls) != 1 {
			return fmt.Errorf("creating configmap chained called webhook %s %d times, and it is called once: its rules name CREATE on configmaps, and this create is one", hook, len(calls))
		}
		if err := admitReview(calls[0], hook, "CREATE", "chained", "configmaps", "ConfigMap"); err != nil {
			return err
		}
		if calls[0]["oldObject"] != nil {
			return fmt.Errorf("the review %s was sent for a create has oldObject %v, and a create replaces nothing: oldObject is null", hook, calls[0]["oldObject"])
		}
	}
	if got := admitChain(created); got != "a1,a2,b1" {
		return fmt.Errorf("configmap chained was stored with annotation chain=%q, and the three mutating webhooks make it \"a1,a2,b1\" when each is called in turn on what the one before it returned: by configuration name (a-first before b-second, whichever was created first), then in the order a configuration lists its webhooks — and each one's patch applied before the next is called%s",
			got, admitChainHint(got))
	}
	stored, err := admitGet(ctx, srv, admitConfigMaps+"/chained")
	if err != nil {
		return err
	}
	if admitChain(stored) != "a1,a2,b1" {
		return fmt.Errorf("the reply to the create carried chain=a1,a2,b1, but GET reads back chain=%q: the mutated object is the one stored, not just the one answered", admitChain(stored))
	}
	seen, _ := hooks.calls("guard")[0]["object"].(map[string]any)
	if admitChain(seen) != "a1,a2,b1" {
		return fmt.Errorf("the validating webhook was sent configmap chained with chain=%q, and it is sent the object after every mutating webhook has changed it: validation is of what will be stored, so it runs last", admitChain(seen))
	}
	return nil
}

// admitDeny checks a refusal is the client's answer, and nothing is stored.
func admitDeny(ctx context.Context, srv *server, hooks *hookServer) error {
	for _, c := range []struct {
		name, deny string
		code       int
	}{{"refused", "true", http.StatusForbidden}, {"refused-invalid", "invalid", http.StatusUnprocessableEntity}} {
		hooks.reset()
		res, body, err := admitSend(ctx, srv, http.MethodPost, admitConfigMaps, "", admitConfigMap(c.name, map[string]any{"guarded": "yes", "deny": c.deny}))
		if err != nil {
			return err
		}
		if res.StatusCode != c.code {
			why := "a webhook that answers allowed: false with no status code is a 403 Forbidden to the client"
			if c.code != http.StatusForbidden {
				why = "the webhook answered allowed: false with status.code 422, and a code the webhook gives is the code the client gets"
			}
			return fmt.Errorf("POST of configmap %s, which webhook guard refuses, answered %d, and %s\nthe body was:\n%s", c.name, res.StatusCode, why, tail(string(body)))
		}
		status, _ := decode(body)
		message, _ := status["message"].(string)
		want := `admission webhook "guard.admission.test" denied the request: ` + admitDenyMessage
		if status["kind"] != "Status" || message != want {
			return fmt.Errorf("the refusal of configmap %s says %q, and it is a Status whose message is %q: the webhook's name, so a person knows which policy to read, and the webhook's own message\nthe body was:\n%s", c.name, message, want, tail(string(body)))
		}
		if err := admitGone(ctx, srv, admitConfigMaps+"/"+c.name, "a create refused by a validating webhook"); err != nil {
			return err
		}
	}
	return nil
}

// admitUpdates checks PUT and each PATCH dialect go through the chain, with
// the webhooks seeing the result of the patch and the object it replaces.
func admitUpdates(ctx context.Context, srv *server, hooks *hookServer) error {
	path := admitConfigMaps + "/chained"
	before, err := admitGet(ctx, srv, path)
	if err != nil {
		return err
	}
	hooks.reset()
	put := admitConfigMap("chained", map[string]any{"guarded": "yes"})
	put["data"] = map[string]any{"step": "put"}
	res, body, err := admitSend(ctx, srv, http.MethodPut, path, "", put)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s answered %d rather than 200: the webhooks allow it\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
	}
	for _, hook := range []string{"a1", "guard"} {
		calls := hooks.calls(hook)
		if len(calls) != 1 {
			return fmt.Errorf("a PUT of configmap chained called webhook %s %d times, and its rules name UPDATE: a PUT is one", hook, len(calls))
		}
		if err := admitReview(calls[0], hook, "UPDATE", "chained", "configmaps", "ConfigMap"); err != nil {
			return err
		}
		old, _ := calls[0]["oldObject"].(map[string]any)
		if metaField(old, "resourceVersion") != metaField(before, "resourceVersion") {
			return fmt.Errorf("the UPDATE review sent to %s has an oldObject at resourceVersion %q, and the object being replaced is at %q: oldObject is what is stored now, so a webhook can tell what is changing",
				hook, metaField(old, "resourceVersion"), metaField(before, "resourceVersion"))
		}
	}

	patches := []struct{ dialect, contentType, query string }{
		{"merge", "application/merge-patch+json", ""},
		{"json", "application/json-patch+json", ""},
		{"strategic", "application/strategic-merge-patch+json", ""},
		{"apply", "application/apply-patch+yaml", "?fieldManager=admission-test&force=true"},
	}
	for _, p := range patches {
		var patch any = map[string]any{"data": map[string]any{"step": p.dialect}}
		switch p.dialect {
		case "json":
			patch = []any{map[string]any{"op": "replace", "path": "/data/step", "value": p.dialect}}
		case "apply":
			patch = map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "chained"}, "data": map[string]any{"step": p.dialect}}
		}
		hooks.reset()
		res, body, err := admitSend(ctx, srv, http.MethodPatch, path+p.query, p.contentType, patch)
		if err != nil {
			return err
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("a %s patch of %s answered %d rather than 200\nthe body was:\n%s", p.dialect, path, res.StatusCode, tail(string(body)))
		}
		calls := hooks.calls("guard")
		if len(calls) != 1 {
			return fmt.Errorf("a %s patch of configmap chained called the validating webhook %d times, and once: a patch is an UPDATE like a PUT, whichever dialect it is written in", p.dialect, len(calls))
		}
		seen, _ := calls[0]["object"].(map[string]any)
		data, _ := seen["data"].(map[string]any)
		if data["step"] != p.dialect || admitChain(seen) != "a1,a2,b1" {
			return fmt.Errorf("after a %s patch setting data.step=%s the validating webhook was sent data.step=%v, chain=%q: admission sees the whole object the patch produces — the patch applied to what is stored, then every mutating webhook — never the patch itself",
				p.dialect, p.dialect, data["step"], admitChain(seen))
		}
		stored, err := admitGet(ctx, srv, path)
		if err != nil {
			return err
		}
		if d, _ := stored["data"].(map[string]any); d["step"] != p.dialect || admitChain(stored) != "a1,a2,b1" {
			return fmt.Errorf("after an allowed %s patch, configmap chained reads back data.step=%v, chain=%q: what the webhooks allowed, as they changed it, is what is stored", p.dialect, d["step"], admitChain(stored))
		}
	}

	// A patch the validating webhook refuses changes nothing.
	hooks.reset()
	res, body, err = admitSend(ctx, srv, http.MethodPatch, path, "application/merge-patch+json",
		map[string]any{"metadata": map[string]any{"labels": map[string]any{"deny": "true"}}, "data": map[string]any{"step": "refused"}})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusForbidden {
		return fmt.Errorf("a merge patch adding label deny=true to configmap chained answered %d, and the validating webhook refuses it: 403\nthe body was:\n%s", res.StatusCode, tail(string(body)))
	}
	stored, err := admitGet(ctx, srv, path)
	if err != nil {
		return err
	}
	if d, _ := stored["data"].(map[string]any); d["step"] != "apply" {
		return fmt.Errorf("a patch refused with 403 left configmap chained with data.step=%v: a refused write stores nothing, so admission has to finish before anything is written", d["step"])
	}
	return nil
}

// admitDelete checks a delete is admitted against the object it removes.
func admitDelete(ctx context.Context, srv *server, hooks *hookServer) error {
	if _, err := admitCreate(ctx, srv, admitConfigMaps, admitConfigMap("keep", map[string]any{"guarded": "yes", "protect": "yes"})); err != nil {
		return err
	}
	hooks.reset()
	res, body, err := admitSend(ctx, srv, http.MethodDelete, admitConfigMaps+"/keep", "", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusForbidden {
		return fmt.Errorf("DELETE of configmap keep, which the validating webhook refuses to let go, answered %d rather than 403: a delete is admitted like any other write, with the object being deleted as oldObject\nthe body was:\n%s", res.StatusCode, tail(string(body)))
	}
	if _, err := admitGet(ctx, srv, admitConfigMaps+"/keep"); err != nil {
		return fmt.Errorf("%w\n\na delete the webhook refused must leave the object where it was", err)
	}
	if hooks.count("a1") != 0 {
		return fmt.Errorf("deleting configmap keep called mutating webhook a1, whose rules name only CREATE and UPDATE: the operations in a rule are a filter, not a suggestion")
	}

	hooks.reset()
	res, body, err = admitSend(ctx, srv, http.MethodDelete, admitConfigMaps+"/chained", "", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE of configmap chained answered %d rather than 200: the webhook allows it\nthe body was:\n%s", res.StatusCode, tail(string(body)))
	}
	calls := hooks.calls("guard")
	if len(calls) != 1 {
		return fmt.Errorf("deleting configmap chained called the validating webhook %d times, and its rules name DELETE", len(calls))
	}
	if err := admitReview(calls[0], "guard", "DELETE", "chained", "configmaps", "ConfigMap"); err != nil {
		return err
	}
	old, _ := calls[0]["oldObject"].(map[string]any)
	if metaField(old, "name") != "chained" || admitChain(old) != "a1,a2,b1" {
		return fmt.Errorf("the DELETE review has oldObject %v, and it is configmap chained as stored: a delete has no new object, so oldObject is the only thing a webhook can judge it by", old)
	}
	if calls[0]["object"] != nil {
		return fmt.Errorf("the DELETE review has object %v, and a delete leaves nothing behind: object is null", calls[0]["object"])
	}
	return nil
}

// admitUnmatched checks webhooks are called only for what their rules and
// selectors name.
func admitUnmatched(ctx context.Context, srv *server, hooks *hookServer) error {
	hooks.reset()
	if _, err := admitCreate(ctx, srv, admitRCs, admitRC("untouched", nil)); err != nil {
		return err
	}
	if n := hooks.total(); n != 0 {
		return fmt.Errorf("creating a replicationcontroller called the webhooks %d times, and every rule configured names configmaps: a webhook is called only for the resources its rules list", n)
	}
	hooks.reset()
	if _, err := admitCreate(ctx, srv, admitConfigMaps, admitConfigMap("unguarded", nil)); err != nil {
		return err
	}
	if n := hooks.count("guard"); n != 0 {
		return fmt.Errorf("creating configmap unguarded, with no labels, called the validating webhook, whose objectSelector is matchLabels guarded=yes: the selector is matched against the object's labels before the webhook is called")
	}
	if n := hooks.count("a1"); n != 1 {
		return fmt.Errorf("creating configmap unguarded called mutating webhook a1 %d times, and it has no objectSelector: no selector selects everything", n)
	}
	return nil
}

// admitFailures checks what a webhook that cannot answer properly does to a
// request: Fail refuses it, Ignore lets it through.
func admitFailures(ctx context.Context, srv *server, hooks *hookServer) error {
	dead, err := freeAddr()
	if err != nil {
		return err
	}
	_, _, other, err := admitCA()
	if err != nil {
		return err
	}
	rcCreate := []string{"CREATE"}

	deadHook := hooks.webhook("dead.admission.test", "dead", rcCreate, "replicationcontrollers", nil)
	deadHook["clientConfig"] = map[string]any{"url": "https://" + dead + "/dead", "caBundle": hooks.caBundle()}
	wrongCA := hooks.webhook("stranger.admission.test", "allow", rcCreate, "replicationcontrollers", nil)
	wrongCA["clientConfig"] = map[string]any{"url": hooks.url + "/allow", "caBundle": base64.StdEncoding.EncodeToString(other)}
	liar := hooks.webhook("liar.admission.test", "liar", rcCreate, "replicationcontrollers", nil)

	cases := []struct {
		hook        map[string]any
		name, label string
		why         string
	}{
		{deadHook, "dead.admission.test", "dead", "nothing is listening at its URL"},
		{wrongCA, "stranger.admission.test", "stranger", "its certificate is not signed by the CA in its caBundle — trusting any certificate, or the system's, lets whoever answers on that address decide what is stored"},
		{liar, "liar.admission.test", "liar", "it answered with a response.uid that is not the request's: the uid is what ties a verdict to a request, and one about some other request is no verdict at all"},
	}
	for _, c := range cases {
		for _, policy := range []string{"Fail", "Ignore"} {
			hook := maps.Clone(c.hook)
			hook["failurePolicy"] = policy
			configName := "c-" + c.label + "-" + strings.ToLower(policy)
			if policy == "Fail" {
				// Fail is the default, so it is left unsaid.
				delete(hook, "failurePolicy")
			}
			if err := admitRegister(ctx, srv, admitMutating, admitConfig("MutatingWebhookConfiguration", configName, hook)); err != nil {
				return err
			}
			name := c.label + "-" + strings.ToLower(policy)
			res, body, err := admitSend(ctx, srv, http.MethodPost, admitRCs, "", admitRC(name, nil))
			if err != nil {
				return err
			}
			if policy == "Fail" {
				status, _ := decode(body)
				message, _ := status["message"].(string)
				if res.StatusCode != http.StatusInternalServerError || !strings.Contains(message, fmt.Sprintf("failed calling webhook %q", c.name)) {
					return fmt.Errorf("creating a replicationcontroller with webhook %s configured answered %d, and %s: with failurePolicy Fail, the default, that is a 500 whose message says failed calling webhook %q — a policy that cannot be checked is not one to wave through\nthe body was:\n%s",
						c.name, res.StatusCode, c.why, c.name, tail(string(body)))
				}
				if err := admitGone(ctx, srv, admitRCs+"/"+name, "a create whose webhook failed"); err != nil {
					return err
				}
			} else if res.StatusCode != http.StatusCreated {
				return fmt.Errorf("creating a replicationcontroller answered %d with webhook %s configured with failurePolicy Ignore, and %s: Ignore means the request goes on as if that webhook were not there\nthe body was:\n%s",
					res.StatusCode, c.name, c.why, tail(string(body)))
			} else if obj, _ := decode(body); admitLabels(obj)["liar"] != "" {
				return fmt.Errorf("the replicationcontroller was stored with label liar=%s, which is the patch from the webhook that answered the wrong uid: an answer that is not for this request is ignored whole, patch and all", admitLabels(obj)["liar"])
			}
			if err := admitUnregister(ctx, srv, admitMutating, configName); err != nil {
				return err
			}
		}
	}
	return nil
}

// admitSelectAndValidate checks objectSelector on a mutating webhook, and
// that the server validates what the mutating webhooks made before a
// validating webhook ever sees it.
func admitSelectAndValidate(ctx context.Context, srv *server, hooks *hookServer) error {
	breaker := hooks.webhook("breaker.admission.test", "breaker", []string{"CREATE"}, "replicationcontrollers", map[string]any{
		"objectSelector": map[string]any{"matchExpressions": []any{
			map[string]any{"key": "break", "operator": "In", "values": []any{"yes"}},
		}},
	})
	watcher := hooks.webhook("watcher.admission.test", "watcher", []string{"CREATE"}, "replicationcontrollers", nil)
	if err := admitRegister(ctx, srv, admitMutating, admitConfig("MutatingWebhookConfiguration", "d-breaker", breaker)); err != nil {
		return err
	}
	if err := admitRegister(ctx, srv, admitValidating, admitConfig("ValidatingWebhookConfiguration", "watcher", watcher)); err != nil {
		return err
	}

	hooks.reset()
	res, body, err := admitSend(ctx, srv, http.MethodPost, admitRCs, "", admitRC("broken", map[string]any{"break": "yes"}))
	if err != nil {
		return err
	}
	if err := wantStatus("POST of replicationcontroller broken, which a mutating webhook patches to spec.replicas -1", res, body, http.StatusUnprocessableEntity, "Invalid"); err != nil {
		return fmt.Errorf("%w\n\nthe server's own validation runs on the object the mutating webhooks returned, and -1 replicas is invalid however it got there", err)
	}
	if hooks.count("breaker") != 1 {
		return fmt.Errorf("replicationcontroller broken, labelled break=yes, called webhook breaker %d times, and its objectSelector matchExpressions break In (yes) selects it", hooks.count("breaker"))
	}
	if hooks.count("watcher") != 0 {
		return fmt.Errorf("the validating webhook was called for replicationcontroller broken, which the server's own validation had already refused: validating webhooks come after it, and are not asked about an object that is not going to be stored")
	}
	if err := admitGone(ctx, srv, admitRCs+"/broken", "a create the server found invalid"); err != nil {
		return err
	}

	hooks.reset()
	if _, err := admitCreate(ctx, srv, admitRCs, admitRC("steady", map[string]any{"break": "no"})); err != nil {
		return err
	}
	if hooks.count("breaker") != 0 {
		return fmt.Errorf("replicationcontroller steady, labelled break=no, called webhook breaker, whose objectSelector is break In (yes)")
	}
	if hooks.count("watcher") != 1 {
		return fmt.Errorf("creating replicationcontroller steady called validating webhook watcher %d times, and once", hooks.count("watcher"))
	}
	if err := admitUnregister(ctx, srv, admitMutating, "d-breaker"); err != nil {
		return err
	}
	return admitUnregister(ctx, srv, admitValidating, "watcher")
}

// admitTimeout checks timeoutSeconds is honoured, and that a webhook taking
// its time does not stop the server answering anybody else.
func admitTimeout(ctx context.Context, srv *server, hooks *hookServer) error {
	slow := hooks.webhook("slow.admission.test", "slow", []string{"UPDATE"}, "replicationcontrollers", map[string]any{"timeoutSeconds": 1})
	if err := admitRegister(ctx, srv, admitValidating, admitConfig("ValidatingWebhookConfiguration", "slow", slow)); err != nil {
		return err
	}
	path := admitRCs + "/steady"
	type result struct {
		res     *http.Response
		body    []byte
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		res, body, err := admitSend(ctx, srv, http.MethodPatch, path, "application/merge-patch+json", map[string]any{"spec": map[string]any{"replicas": 3}})
		done <- result{res, body, err, time.Since(start)}
	}()
	select {
	case <-hooks.slowArrived:
	case r := <-done:
		return fmt.Errorf("a merge patch of replicationcontroller steady answered %v without calling validating webhook slow, whose rules name UPDATE on replicationcontrollers", admitCode(r.res))
	case <-time.After(10 * time.Second):
		return fmt.Errorf("a merge patch of replicationcontroller steady did not call validating webhook slow within 10s, and its rules name UPDATE on replicationcontrollers")
	}

	// The webhook is holding the patch. Everything else carries on.
	for _, probe := range []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, path, nil},
		{http.MethodGet, admitConfigMaps, nil},
		{http.MethodPost, admitConfigMaps, admitConfigMap("meanwhile", nil)},
	} {
		began := time.Now()
		res, body, err := admitSend(ctx, srv, probe.method, probe.path, "", probe.body)
		if err != nil {
			return err
		}
		if took := time.Since(began); took > 700*time.Millisecond {
			return fmt.Errorf("%s %s took %v while a webhook was still deciding about a patch of another object: a webhook call is a network round trip of up to timeoutSeconds, and making it while holding the store's lock stops every read and write until it returns", probe.method, probe.path, took.Round(time.Millisecond))
		}
		if res.StatusCode >= 300 {
			return fmt.Errorf("%s %s answered %d while a webhook was deciding about a patch of another object\nthe body was:\n%s", probe.method, probe.path, res.StatusCode, tail(string(body)))
		}
	}

	var r result
	select {
	case r = <-done:
	case <-time.After(15 * time.Second):
		return fmt.Errorf("a patch whose validating webhook has timeoutSeconds: 1 had not been answered 15s later: the timeout bounds the call, and when it passes the webhook has failed")
	}
	if r.err != nil {
		return r.err
	}
	status, _ := decode(r.body)
	message, _ := status["message"].(string)
	if r.res.StatusCode != http.StatusInternalServerError || !strings.Contains(message, `failed calling webhook "slow.admission.test"`) {
		return fmt.Errorf("the patch held by webhook slow, which never answers within its timeoutSeconds of 1, answered %d: a webhook that runs out of time has failed, and with failurePolicy Fail that is a 500 saying failed calling webhook \"slow.admission.test\"\nthe body was:\n%s", r.res.StatusCode, tail(string(r.body)))
	}
	if r.elapsed > 4*time.Second {
		return fmt.Errorf("the patch held by webhook slow took %v to be refused, and its timeoutSeconds is 1: the timeout is the webhook's, not a default — the default 10s is only for a webhook that does not say", r.elapsed.Round(time.Millisecond))
	}
	stored, err := admitGet(ctx, srv, path)
	if err != nil {
		return err
	}
	if spec, _ := stored["spec"].(map[string]any); spec["replicas"] != 1.0 {
		return fmt.Errorf("replicationcontroller steady has spec.replicas %v after a patch to 3 was refused by a webhook that timed out: a refused write stores nothing", spec["replicas"])
	}
	return admitUnregister(ctx, srv, admitValidating, "slow")
}

// admitReview checks the shape of one AdmissionReview request as received.
func admitReview(req map[string]any, hook, op, name, resource, kind string) error {
	where := fmt.Sprintf("the AdmissionReview sent to webhook %s", hook)
	if uid, _ := req["uid"].(string); uid == "" {
		return fmt.Errorf("%s has no request.uid: the webhook echoes it in its response, which is how its answer is matched to this request", where)
	}
	if req["operation"] != op {
		return fmt.Errorf("%s has operation %v, and this request is a %s", where, req["operation"], op)
	}
	if req["name"] != name || req["namespace"] != "default" {
		return fmt.Errorf("%s names %v in namespace %v, and the request is for %s in default", where, req["name"], req["namespace"], name)
	}
	gotKind, _ := req["kind"].(map[string]any)
	gotResource, _ := req["resource"].(map[string]any)
	if gotKind["group"] != "" || gotKind["version"] != "v1" || gotKind["kind"] != kind ||
		gotResource["group"] != "" || gotResource["version"] != "v1" || gotResource["resource"] != resource {
		return fmt.Errorf("%s has kind %v and resource %v, and they are {group: \"\", version: v1, kind: %s} and {group: \"\", version: v1, resource: %s}: the core group's name is the empty string, and it is spelled out", where, req["kind"], req["resource"], kind, resource)
	}
	if req["dryRun"] != false {
		return fmt.Errorf("%s has dryRun %v, and this request is not a dry run: dryRun is false", where, req["dryRun"])
	}
	user, _ := req["userInfo"].(map[string]any)
	groups, _ := user["groups"].([]any)
	if user["username"] != "alice" || user["uid"] != "1001" || !contains(groups, "developers") || !contains(groups, "system:authenticated") {
		return fmt.Errorf("%s has userInfo %v, and the request came from alice (uid 1001, groups developers, oncall, system:authenticated): who is asking is half of what most policies are about, and it is what authentication decided", where, req["userInfo"])
	}
	return nil
}

// The message webhook guard refuses with.
const admitDenyMessage = "objects labelled deny are refused here"

// hookServer is the webhooks: one HTTPS server, a path per webhook, and a
// record of every review each one was sent.
type hookServer struct {
	srv         *httptest.Server
	url         string
	caPEM       []byte
	mu          sync.Mutex
	seen        map[string][]map[string]any
	slowArrived chan struct{}
}

func startHooks() (*hookServer, error) {
	ca, caKey, caPEM, err := admitCA()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	cert, key, err := certIssue(&x509.Certificate{
		Subject: pkix.Name{CommonName: "127.0.0.1"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
	}, ca, caKey)
	if err != nil {
		return nil, err
	}
	h := &hookServer{caPEM: caPEM, seen: map[string][]map[string]any{}, slowArrived: make(chan struct{}, 1)}
	h.srv = httptest.NewUnstartedServer(http.HandlerFunc(h.serveHTTP))
	h.srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{cert.Raw}, PrivateKey: key}}}
	// The stranger case is a TLS handshake the server refuses, by design.
	h.srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	h.srv.StartTLS()
	h.url = h.srv.URL
	return h, nil
}

func (h *hookServer) close() { h.srv.Close() }

func (h *hookServer) caBundle() string { return base64.StdEncoding.EncodeToString(h.caPEM) }

// webhook is one entry of a configuration's webhooks list, served at path.
func (h *hookServer) webhook(name, path string, ops []string, resource string, extra map[string]any) map[string]any {
	opList := []any{}
	for _, op := range ops {
		opList = append(opList, op)
	}
	hook := map[string]any{
		"name":         name,
		"clientConfig": map[string]any{"url": h.url + "/" + path, "caBundle": h.caBundle()},
		"rules": []any{map[string]any{
			"operations": opList, "apiGroups": []any{""}, "apiVersions": []any{"v1"}, "resources": []any{resource},
		}},
		"sideEffects":             "None",
		"admissionReviewVersions": []any{"v1"},
	}
	maps.Copy(hook, extra)
	return hook
}

func (h *hookServer) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = map[string][]map[string]any{}
}

func (h *hookServer) calls(hook string) []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[hook]
}

func (h *hookServer) count(hook string) int { return len(h.calls(hook)) }

func (h *hookServer) total() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, calls := range h.seen {
		n += len(calls)
	}
	return n
}

// serveHTTP is every webhook. Each records what it was sent and answers by
// its path:
//
//	a1        sets annotation chain=a1
//	a2, b1    append their name to chain
//	guard     refuses label deny (with 422 for deny=invalid), and a DELETE of label protect
//	allow     allows
//	liar      allows with a patch, under the wrong uid
//	breaker   patches spec.replicas to -1
//	watcher   allows
//	slow      never answers in time
func (h *hookServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	hook := strings.TrimPrefix(r.URL.Path, "/")
	var review struct {
		Request map[string]any `json:"request"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 3<<20)).Decode(&review); err != nil || review.Request == nil {
		http.Error(w, "not an AdmissionReview", http.StatusBadRequest)
		return
	}
	req := review.Request
	h.mu.Lock()
	h.seen[hook] = append(h.seen[hook], req)
	h.mu.Unlock()

	uid, _ := req["uid"].(string)
	obj, _ := req["object"].(map[string]any)
	response := map[string]any{"uid": uid, "allowed": true}
	patch := func(ops ...any) {
		raw, _ := json.Marshal(ops)
		response["patchType"] = "JSONPatch"
		response["patch"] = base64.StdEncoding.EncodeToString(raw)
	}
	switch hook {
	case "a1", "a2", "b1":
		chain := hook
		if hook != "a1" {
			chain = strings.TrimPrefix(admitChain(obj)+","+hook, ",")
		}
		if _, ok := admitMeta(obj)["annotations"].(map[string]any); ok {
			patch(map[string]any{"op": "add", "path": "/metadata/annotations/chain", "value": chain})
		} else {
			patch(map[string]any{"op": "add", "path": "/metadata/annotations", "value": map[string]any{"chain": chain}})
		}
	case "guard":
		old, _ := req["oldObject"].(map[string]any)
		switch deny := admitLabels(obj)["deny"]; {
		case deny == "invalid":
			response["allowed"] = false
			response["status"] = map[string]any{"code": http.StatusUnprocessableEntity, "reason": "Invalid", "message": admitDenyMessage}
		case deny != "":
			response["allowed"] = false
			response["status"] = map[string]any{"message": admitDenyMessage}
		case req["operation"] == "DELETE" && admitLabels(old)["protect"] == "yes":
			response["allowed"] = false
			response["status"] = map[string]any{"message": admitDenyMessage}
		}
	case "liar":
		response["uid"] = "not-" + uid
		patch(map[string]any{"op": "add", "path": "/metadata/labels", "value": map[string]any{"liar": "yes"}})
	case "breaker":
		patch(map[string]any{"op": "replace", "path": "/spec/replicas", "value": -1})
	case "slow":
		select {
		case h.slowArrived <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(6 * time.Second):
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "response": response})
}

// admitCA is a certificate authority made for one run, so the only way to
// trust the webhooks is the caBundle the configurations carry.
func admitCA() (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	now := time.Now()
	ca, key, err := certIssue(&x509.Certificate{
		Subject: pkix.Name{CommonName: "byok8s admission test CA"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
	}, nil, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	return ca, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), nil
}

func admitConfig(kind, name string, hooks ...map[string]any) map[string]any {
	list := []any{}
	for _, h := range hooks {
		list = append(list, h)
	}
	return map[string]any{
		"apiVersion": "admissionregistration.k8s.io/v1", "kind": kind,
		"metadata": map[string]any{"name": name}, "webhooks": list,
	}
}

func admitConfigMap(name string, labels map[string]any) map[string]any {
	meta := map[string]any{"name": name}
	if labels != nil {
		meta["labels"] = labels
	}
	return map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta, "data": map[string]any{"step": "create"}}
}

func admitRC(name string, labels map[string]any) map[string]any {
	meta := map[string]any{"name": name}
	if labels != nil {
		meta["labels"] = labels
	}
	return map[string]any{"apiVersion": "v1", "kind": "ReplicationController", "metadata": meta,
		"spec": map[string]any{"replicas": 1, "template": map[string]any{
			"metadata": map[string]any{"labels": map[string]any{"app": name}},
		}}}
}

func admitMeta(obj map[string]any) map[string]any {
	meta, _ := obj["metadata"].(map[string]any)
	return meta
}

func admitLabels(obj map[string]any) map[string]string {
	raw, _ := admitMeta(obj)["labels"].(map[string]any)
	out := map[string]string{}
	for k, v := range raw {
		out[k], _ = v.(string)
	}
	return out
}

func admitChain(obj map[string]any) string {
	annotations, _ := admitMeta(obj)["annotations"].(map[string]any)
	chain, _ := annotations["chain"].(string)
	return chain
}

// admitChainHint names the likely mistake behind a wrong chain.
func admitChainHint(got string) string {
	switch got {
	case "":
		return "\n\nno chain at all: the mutating webhooks answered with a base64 JSON patch in response.patch, and it was not applied"
	case "a1", "a1,a2":
		return "\n\nthe webhooks ran out of order: sort the configurations by name before calling any of them"
	case "a2", "b1":
		return "\n\na webhook was handed the object as the client sent it rather than as the previous webhook left it"
	}
	return ""
}

func admitCode(res *http.Response) any {
	if res == nil {
		return "nothing"
	}
	return res.StatusCode
}

// admitSend is one request as alice. contentType defaults to JSON.
func admitSend(ctx context.Context, srv *server, method, path, contentType string, body any) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("encode the request body: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, srv.url+path, reader)
	if err != nil {
		return nil, nil, err
	}
	if body != nil {
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", admitAlice)
	res, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%s %s: %w\nthe program said:\n%s", method, path, err, tail(srv.p.Stdout()))
	}
	defer res.Body.Close()
	got, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("read the response body: %w", err)
	}
	return res, got, nil
}

func admitCreate(ctx context.Context, srv *server, path string, body map[string]any) (map[string]any, error) {
	res, raw, err := admitSend(ctx, srv, http.MethodPost, path, "", body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("POST %s of %s answered %d rather than 201: every webhook configured for it allows it\nthe body was:\n%s",
			path, metaField(body, "name"), res.StatusCode, tail(string(raw)))
	}
	return decode(raw)
}

func admitGet(ctx context.Context, srv *server, path string) (map[string]any, error) {
	res, raw, err := admitSend(ctx, srv, http.MethodGet, path, "", nil)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s answered %d rather than 200\nthe body was:\n%s", path, res.StatusCode, tail(string(raw)))
	}
	return decode(raw)
}

// admitGone insists nothing is stored at path.
func admitGone(ctx context.Context, srv *server, path, what string) error {
	res, raw, err := admitSend(ctx, srv, http.MethodGet, path, "", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GET %s answered %d after %s: a write admission refused is a write that never happened, so admission finishes before anything is stored\nthe body was:\n%s",
			path, res.StatusCode, what, tail(string(raw)))
	}
	return nil
}

func admitRegister(ctx context.Context, srv *server, collection string, config map[string]any) error {
	res, raw, err := admitSend(ctx, srv, http.MethodPost, collection, "", config)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("POST %s of %s %s answered %d rather than 201: webhook configurations are cluster-scoped objects of admissionregistration.k8s.io/v1, created like any other\nthe body was:\n%s",
			collection, config["kind"], metaField(config, "name"), res.StatusCode, tail(string(raw)))
	}
	return nil
}

func admitUnregister(ctx context.Context, srv *server, collection, name string) error {
	res, raw, err := admitSend(ctx, srv, http.MethodDelete, collection+"/"+name, "", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE %s/%s answered %d rather than 200: a webhook configuration is removed like any other object, and is never itself sent to a webhook\nthe body was:\n%s",
			collection, name, res.StatusCode, tail(string(raw)))
	}
	return nil
}
