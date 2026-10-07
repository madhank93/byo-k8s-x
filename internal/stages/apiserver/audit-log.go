package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
)

func init() { register(Stage{Slug: "audit-log", Run: stageAuditLog}) }

const auditTokens = `alice-token-0001,alice,1001,"developers,oncall"
bob-token-0002,bob,1002
carol-token-0003,carol,1003
dave-token-0004,dave,1004,"robots"
`

// auditPolicy is ordered so that most requests could match more than one
// rule, and only taking the first match gives the levels the stage expects.
const auditPolicy = `{
  "apiVersion": "audit.k8s.io/v1",
  "kind": "Policy",
  "omitStages": ["RequestReceived"],
  "rules": [
    {"level": "None", "users": ["carol"]},
    {"level": "None", "nonResourceURLs": ["/healthz", "/readyz*"]},
    {"level": "None", "userGroups": ["robots"], "verbs": ["get"]},
    {"level": "RequestResponse", "resources": [{"group": "", "resources": ["configmaps"]}]},
    {"level": "Request", "resources": [{"group": "", "resources": ["replicationcontrollers/scale"]}]},
    {"level": "Request", "resources": [{"group": "", "resources": ["replicationcontrollers"]}], "namespaces": ["team-a"]},
    {"level": "Metadata", "resources": [{"group": ""}]},
    {"level": "Metadata", "nonResourceURLs": ["/openapi/*"]}
  ]
}
`

const auditAgent = "byok8s-audit-grader/1.0"

const (
	auditAlice = "Bearer alice-token-0001"
	auditBob   = "Bearer bob-token-0002"
	auditCarol = "Bearer carol-token-0003"
	auditDave  = "Bearer dave-token-0004"
)

// auditCall is one request the stage made, and the audit ID the server said
// it filed it under.
type auditCall struct {
	what string
	uri  string
	id   string
	code int
}

// auditWant is what the event for one call has to say. An empty level means
// the policy says None and there must be no event at all.
type auditWant struct {
	level, verb, user, group                   string
	resource, namespace, name, subresource     string
	requestObject, responseObject, nonResource bool
}

// stageAuditLog checks the server keeps a record of who did what: a line per
// request, at the level of detail a policy picks for it.
//
// The log is what is left after an incident. It is graded the way it is read
// then: every line has to parse on its own, each request has to be findable by
// the ID the client was given, and a request the server refused has to be in
// it as well as the ones it allowed.
func stageAuditLog(ctx context.Context, _ *kube.Env, bin string) error {
	if err := auditDefault(ctx, bin); err != nil {
		return err
	}

	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the audit files: %w", err)
	}
	defer os.RemoveAll(dir)
	tokens, policy, log := filepath.Join(dir, "tokens.csv"), filepath.Join(dir, "policy.json"), filepath.Join(dir, "audit.log")
	if err := os.WriteFile(tokens, []byte(auditTokens), 0o600); err != nil {
		return fmt.Errorf("write the token file: %w", err)
	}
	if err := os.WriteFile(policy, []byte(auditPolicy), 0o600); err != nil {
		return fmt.Errorf("write the audit policy: %w", err)
	}
	srv, cleanup, err := serve(ctx, bin, "-token-auth-file", tokens, "-audit-log-path", log, "-audit-policy-file", policy)
	if err != nil {
		return fmt.Errorf("%w\n\nthis stage passes -audit-log-path <file> and -audit-policy-file <file>, a Policy written as JSON", err)
	}
	defer cleanup()

	for _, ns := range []string{"team-a", "team-b"} {
		body := map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": ns}}
		if _, err := auditDo(ctx, srv, http.MethodPost, "/api/v1/namespaces", auditAlice, "", body, http.StatusCreated); err != nil {
			return err
		}
	}

	const cms = "/api/v1/namespaces/team-a/configmaps"
	cm := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings"}, "data": map[string]any{"mode": "strict"}}
	rc := func(name string) map[string]any {
		return map[string]any{"apiVersion": "v1", "kind": "ReplicationController", "metadata": map[string]any{"name": name},
			"spec": map[string]any{"replicas": 1, "selector": map[string]any{"app": "web"}, "template": scaleTemplate()}}
	}
	type step struct {
		method, path, auth, contentType string
		body                            any
		code                            int
		want                            auditWant
	}
	steps := []step{
		{http.MethodPost, cms, auditAlice, "", cm, http.StatusCreated,
			auditWant{level: "RequestResponse", verb: "create", user: "alice", group: "developers", resource: "configmaps", namespace: "team-a", requestObject: true, responseObject: true}},
		{http.MethodGet, cms + "/settings", auditAlice, "", nil, http.StatusOK,
			auditWant{level: "RequestResponse", verb: "get", user: "alice", group: "oncall", resource: "configmaps", namespace: "team-a", name: "settings", responseObject: true}},
		{http.MethodGet, cms + "?limit=10", auditAlice, "", nil, http.StatusOK,
			auditWant{level: "RequestResponse", verb: "list", user: "alice", resource: "configmaps", namespace: "team-a", responseObject: true}},
		{http.MethodPatch, cms + "/settings", auditAlice, "application/merge-patch+json", map[string]any{"data": map[string]any{"mode": "relaxed"}}, http.StatusOK,
			auditWant{level: "RequestResponse", verb: "patch", user: "alice", resource: "configmaps", namespace: "team-a", name: "settings", requestObject: true, responseObject: true}},
		{http.MethodGet, cms + "/missing", auditBob, "", nil, http.StatusNotFound,
			auditWant{level: "RequestResponse", verb: "get", user: "bob", group: "system:authenticated", resource: "configmaps", namespace: "team-a", name: "missing", responseObject: true}},
		// dave is a robot: his reads of one object are not recorded, his lists are.
		{http.MethodGet, cms + "/settings", auditDave, "", nil, http.StatusOK, auditWant{}},
		{http.MethodGet, cms, auditDave, "", nil, http.StatusOK,
			auditWant{level: "RequestResponse", verb: "list", user: "dave", group: "robots", resource: "configmaps", namespace: "team-a", responseObject: true}},
		// carol is not recorded at all, whatever she does.
		{http.MethodGet, cms, auditCarol, "", nil, http.StatusOK, auditWant{}},
		{http.MethodDelete, cms + "/settings", auditCarol, "", nil, http.StatusOK, auditWant{}},
		{http.MethodPost, "/api/v1/namespaces/team-a/replicationcontrollers", auditBob, "", rc("web"), http.StatusCreated,
			auditWant{level: "Request", verb: "create", user: "bob", resource: "replicationcontrollers", namespace: "team-a", requestObject: true}},
		{http.MethodPost, "/api/v1/namespaces/team-b/replicationcontrollers", auditBob, "", rc("batch"), http.StatusCreated,
			auditWant{level: "Metadata", verb: "create", user: "bob", resource: "replicationcontrollers", namespace: "team-b"}},
		{http.MethodPut, "/api/v1/namespaces/team-b/replicationcontrollers/batch/scale", auditBob, "", scaleBody("batch", "", 3), http.StatusOK,
			auditWant{level: "Request", verb: "update", user: "bob", resource: "replicationcontrollers", namespace: "team-b", name: "batch", subresource: "scale", requestObject: true}},
		{http.MethodGet, "/api/v1/namespaces/team-a/replicationcontrollers/web/status", auditBob, "", nil, http.StatusOK,
			auditWant{level: "Metadata", verb: "get", user: "bob", resource: "replicationcontrollers", namespace: "team-a", name: "web", subresource: "status"}},
		{http.MethodDelete, "/api/v1/namespaces/team-b/replicationcontrollers/batch", auditBob, "", nil, http.StatusOK,
			auditWant{level: "Metadata", verb: "delete", user: "bob", resource: "replicationcontrollers", namespace: "team-b", name: "batch"}},
		{http.MethodGet, "/healthz", auditAlice, "", nil, http.StatusOK, auditWant{}},
		{http.MethodGet, "/readyz", auditAlice, "", nil, http.StatusOK, auditWant{}},
		{http.MethodGet, "/api", auditAlice, "", nil, http.StatusOK, auditWant{}},
		{http.MethodGet, "/openapi/v3", auditAlice, "", nil, http.StatusOK,
			auditWant{level: "Metadata", verb: "get", user: "alice", nonResource: true}},
		{http.MethodGet, cms, "Bearer mallory-token-9999", "", nil, http.StatusUnauthorized,
			auditWant{level: "RequestResponse", verb: "list", resource: "configmaps", namespace: "team-a", responseObject: true}},
	}
	calls := make([]auditCall, len(steps))
	for i, s := range steps {
		if calls[i], err = auditDo(ctx, srv, s.method, s.path, s.auth, s.contentType, s.body, s.code); err != nil {
			return err
		}
	}

	var logged []auditCall
	for i, s := range steps {
		if s.want.level != "" {
			logged = append(logged, calls[i])
		}
	}
	events, err := auditWait(ctx, log, logged, "ResponseComplete")
	if err != nil {
		return err
	}
	for i, s := range steps {
		if err := auditCheck(events[calls[i].id], "ResponseComplete", calls[i], s.want); err != nil {
			return err
		}
	}

	if err := auditWatch(ctx, srv, log); err != nil {
		return err
	}
	return auditConcurrent(ctx, srv, log)
}

// auditDefault checks a server given a log and no policy: everything is
// recorded, at Metadata, and nothing else changes.
func auditDefault(ctx context.Context, bin string) error {
	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the audit log: %w", err)
	}
	defer os.RemoveAll(dir)
	log := filepath.Join(dir, "audit.log")
	srv, cleanup, err := serve(ctx, bin, "-audit-log-path", log)
	if err != nil {
		return fmt.Errorf("%w\n\nthis stage first passes -audit-log-path <file> alone: with no policy file every request is recorded at Metadata", err)
	}
	defer cleanup()

	cm := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "plain"}, "data": map[string]any{"k": "v"}}
	create, err := auditDo(ctx, srv, http.MethodPost, "/api/v1/namespaces/default/configmaps", "", "", cm, http.StatusCreated)
	if err != nil {
		return err
	}
	discovery, err := auditDo(ctx, srv, http.MethodGet, "/api/v1", "", "", nil, http.StatusOK)
	if err != nil {
		return err
	}
	events, err := auditWait(ctx, log, []auditCall{create, discovery}, "ResponseComplete")
	if err != nil {
		return fmt.Errorf("%w\n\nthis server was started with -audit-log-path and no -audit-policy-file, which records every request at Metadata", err)
	}
	want := auditWant{level: "Metadata", verb: "create", user: "system:anonymous", group: "system:unauthenticated", resource: "configmaps", namespace: "default"}
	if err := auditCheck(events[create.id], "ResponseComplete", create, want); err != nil {
		return fmt.Errorf("%w\n\n(with no policy file, the default is Metadata for every request)", err)
	}
	want = auditWant{level: "Metadata", verb: "get", user: "system:anonymous", nonResource: true}
	if err := auditCheck(events[discovery.id], "ResponseComplete", discovery, want); err != nil {
		return fmt.Errorf("%w\n\n(with no policy file, the default is Metadata for every request, the ones outside /api/v1/<resource> too)", err)
	}
	return nil
}

// auditWatch checks a watch is on record while it is still open, and again
// when it ends.
func auditWatch(ctx context.Context, srv *server, log string) error {
	const path = "/api/v1/namespaces/team-a/configmaps?watch=true"
	what := "GET " + path + " as alice"
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(wctx, http.MethodGet, srv.url+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auditAlice)
	req.Header.Set("User-Agent", auditAgent)
	type opened struct {
		res *http.Response
		err error
	}
	answer := make(chan opened, 1)
	go func() {
		res, err := (&http.Client{}).Do(req)
		answer <- opened{res, err}
	}()
	var res *http.Response
	select {
	case got := <-answer:
		if got.err != nil {
			return fmt.Errorf("%s: %w\nthe program said:\n%s", what, got.err, tail(srv.p.Stdout()))
		}
		res = got.res
	case <-time.After(30 * time.Second):
		return fmt.Errorf("%s did not answer within 30s: the auditing wrapped around a watch has to pass Flush through to the real ResponseWriter, or the stream's header never leaves the server", what)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
		return fmt.Errorf("%s answered %d rather than 200\nthe body was:\n%s", what, res.StatusCode, tail(string(body)))
	}
	call := auditCall{what: what, uri: path, id: res.Header.Get("Audit-Id"), code: http.StatusOK}
	if call.id == "" {
		return fmt.Errorf("%s answered with no Audit-Id header: every response carries the ID its audit events are filed under, a watch included", what)
	}
	want := auditWant{level: "RequestResponse", verb: "watch", user: "alice", resource: "configmaps", namespace: "team-a"}

	started, err := auditWait(ctx, log, []auditCall{call}, "ResponseStarted")
	if err != nil {
		return fmt.Errorf("%w\n\na watch can stay open for hours, and a log that only records it when it ends shows nothing of it while it is happening: the real server writes a ResponseStarted event as soon as the response begins, and ResponseComplete when it ends", err)
	}
	if err := auditCheck(started[call.id], "ResponseStarted", call, want); err != nil {
		return err
	}

	stream := json.NewDecoder(res.Body)
	arrived := make(chan error, 1)
	go func() {
		for {
			var event map[string]any
			if err := stream.Decode(&event); err != nil {
				arrived <- fmt.Errorf("the watch ended (%v)", err)
				return
			}
			if obj, _ := event["object"].(map[string]any); metaField(obj, "name") == "watched" {
				arrived <- nil
				return
			}
		}
	}()
	cm := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "watched"}}
	if _, err := auditDo(ctx, srv, http.MethodPost, "/api/v1/namespaces/team-a/configmaps", auditBob, "", cm, http.StatusCreated); err != nil {
		return err
	}
	select {
	case err := <-arrived:
		if err != nil {
			return fmt.Errorf("%s stopped streaming before the ADDED event for a configmap created while it was open: %v — the auditing wraps the ResponseWriter, and a wrapper that does not implement http.Flusher hides the real one's Flush from the watch handler", what, err)
		}
	case <-time.After(15 * time.Second):
		return fmt.Errorf("%s sent no ADDED event within 15s for a configmap created while it was open: the auditing wraps the ResponseWriter, and a wrapper has to pass Flush through or every event the watch writes sits in a buffer", what)
	}
	events, err := auditRead(log, false)
	if err != nil {
		return err
	}
	for _, e := range events {
		if e["auditID"] == call.id && e["stage"] == "ResponseComplete" {
			return fmt.Errorf("the audit log has a ResponseComplete event for %s while the watch is still open: ResponseComplete is written when the response has ended, and this one has not", what)
		}
	}

	cancel()
	res.Body.Close()
	complete, err := auditWait(ctx, log, []auditCall{call}, "ResponseComplete")
	if err != nil {
		return fmt.Errorf("%w\n\nwhen the client leaves, the watch's handler returns, and that is when its ResponseComplete event is written, under the same auditID as its ResponseStarted", err)
	}
	return auditCheck(complete[call.id], "ResponseComplete", call, want)
}

// auditConcurrent checks events from requests running at once come out as
// whole lines: a log in which two events are spliced together loses both.
func auditConcurrent(ctx context.Context, srv *server, log string) error {
	const workers, each = 32, 10
	filler := strings.Repeat("abcdefghijklmnopqrstuvwxyz0123456789", 400)
	var (
		mu    sync.Mutex
		calls []auditCall
		fail  error
		wg    sync.WaitGroup
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				cm := map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{"name": fmt.Sprintf("load-%d-%d", w, i)}, "data": map[string]any{"blob": filler}}
				call, err := auditDo(ctx, srv, http.MethodPost, "/api/v1/namespaces/team-b/configmaps", auditAlice, "", cm, http.StatusCreated)
				mu.Lock()
				if err != nil && fail == nil {
					fail = err
				}
				calls = append(calls, call)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if fail != nil {
		return fail
	}
	events, err := auditWait(ctx, log, calls, "ResponseComplete")
	if err != nil {
		return fmt.Errorf("%w\n\nthese were %d large creates sent %d at a time: an event has to reach the file as one write of one whole line, made while holding a lock, or two requests finishing together splice their events into lines that parse as neither", err, len(calls), workers)
	}
	want := auditWant{level: "RequestResponse", verb: "create", user: "alice", resource: "configmaps", namespace: "team-b", requestObject: true, responseObject: true}
	for _, c := range calls {
		if err := auditCheck(events[c.id], "ResponseComplete", c, want); err != nil {
			return err
		}
	}
	return nil
}

// auditDo sends one request as a known client and insists on its code, and
// on the Audit-Id every response carries.
func auditDo(ctx context.Context, srv *server, method, path, auth, contentType string, body any, code int) (auditCall, error) {
	who := "with no token"
	if name, _, ok := strings.Cut(strings.TrimPrefix(auth, "Bearer "), "-token"); ok {
		who = "as " + name
	}
	call := auditCall{what: fmt.Sprintf("%s %s %s", method, path, who), uri: path, code: code}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return call, fmt.Errorf("encode the request body: %w", err)
		}
		reader = bytes.NewReader(raw)
		if contentType == "" {
			contentType = "application/json"
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, srv.url+path, reader)
	if err != nil {
		return call, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", auditAgent)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return call, fmt.Errorf("%s: %w\nthe program said:\n%s", call.what, err, tail(srv.p.Stdout()))
	}
	defer res.Body.Close()
	got, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != code {
		return call, fmt.Errorf("%s answered %d rather than %d, and this stage builds on the earlier ones: auditing records requests, it does not change how they are answered\nthe body was:\n%s",
			call.what, res.StatusCode, code, tail(string(got)))
	}
	call.id = res.Header.Get("Audit-Id")
	if call.id == "" {
		return call, fmt.Errorf("%s answered with no Audit-Id header: every response carries the ID its audit events are filed under — it is how someone holding a failed response finds the record of it, so it is sent whatever the policy says, refusals included", call.what)
	}
	return call, nil
}

// auditWait reads the log until every call has an event at this stage, and
// returns every event in the log by auditID, whatever its stage.
func auditWait(ctx context.Context, log string, calls []auditCall, stage string) (map[string][]map[string]any, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		events, err := auditRead(log, false)
		if err != nil {
			return nil, err
		}
		found := map[string][]map[string]any{}
		for _, e := range events {
			id, _ := e["auditID"].(string)
			found[id] = append(found[id], e)
		}
		var missing *auditCall
		for i := range calls {
			if len(auditAt(found[calls[i].id], stage)) == 0 {
				missing = &calls[i]
				break
			}
		}
		if missing == nil {
			// Every line written by now has to be whole, the last one too.
			if _, err := auditRead(log, true); err != nil {
				return nil, err
			}
			return found, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%s was answered with Audit-Id %s, and after 10s the audit log has no %s event with that auditID (it holds %d events), though the policy records it: either the event was never written, or a rule above the one meant for it matched first — rules match on the API verb (get, list, watch, create, ...), the user and groups, and the resource or URL — or its auditID is not the ID sent in the header",
				missing.what, missing.id, stage, len(events))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// auditRead parses the log, one event per line. Unless whole is set, a last
// line with no newline yet is left for the next read: it may be mid-write.
func auditRead(log string, whole bool) ([]map[string]any, error) {
	raw, err := os.ReadFile(log)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the audit log: %w", err)
	}
	lines := strings.Split(string(raw), "\n")
	last := lines[len(lines)-1]
	lines = lines[:len(lines)-1]
	if whole && last != "" {
		lines = append(lines, last)
	}
	var events []map[string]any
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			shown := line
			if len(shown) > 300 {
				shown = shown[:150] + " … " + shown[len(shown)-150:]
			}
			return nil, fmt.Errorf("line %d of the audit log is not one JSON object (%v):\n  %s\n\neach event is a whole JSON object on one line, written in one piece: a line that is two events spliced together, or an event split over two lines, is a record nothing can read back", i+1, err, shown)
		}
		events = append(events, event)
	}
	return events, nil
}

// auditAt is the events at one stage.
func auditAt(events []map[string]any, stage string) []map[string]any {
	var at []map[string]any
	for _, e := range events {
		if e["stage"] == stage {
			at = append(at, e)
		}
	}
	return at
}

// auditCheck compares the events a call left at one stage with what the
// policy says it should have left.
func auditCheck(all []map[string]any, stage string, call auditCall, want auditWant) error {
	if want.level == "" {
		if len(all) > 0 {
			return fmt.Errorf("%s left an audit event at level %v, and the policy puts it at None: rules are tried in order and the first that matches decides, so a None rule above a broader one keeps those requests out of the log entirely", call.what, all[0]["level"])
		}
		return nil
	}
	events := auditAt(all, stage)
	if len(events) != 1 {
		return fmt.Errorf("%s left %d %s events under auditID %s, and it is one request: one event per stage", call.what, len(events), stage, call.id)
	}
	e := events[0]
	if e["apiVersion"] != "audit.k8s.io/v1" || e["kind"] != "Event" {
		return fmt.Errorf("the audit event for %s has apiVersion %v, kind %v, and it is an audit.k8s.io/v1 Event: tools that read audit logs dispatch on those two fields", call.what, e["apiVersion"], e["kind"])
	}
	if e["level"] != want.level {
		return fmt.Errorf("the audit event for %s is at level %v, and the policy puts it at %s: the rules are tried top to bottom and the first one whose every field matches decides — users, userGroups, verbs, resources (where \"configmaps\" is the object and not its subresources, and \"replicationcontrollers/scale\" is one subresource), namespaces, nonResourceURLs (with a trailing * as a prefix)",
			call.what, e["level"], want.level)
	}
	if e["verb"] != want.verb {
		return fmt.Errorf("the audit event for %s has verb %v, and it is %q: the verb is the API's, not HTTP's — a GET is get for one object, list for a collection and watch with ?watch=true; POST is create, PUT update, PATCH patch; outside the API it is the method in lower case",
			call.what, e["verb"], want.verb)
	}
	if e["requestURI"] != call.uri {
		return fmt.Errorf("the audit event for %s has requestURI %v, and it is the path and query exactly as the client sent them, %q", call.what, e["requestURI"], call.uri)
	}
	status, _ := e["responseStatus"].(map[string]any)
	if code, _ := status["code"].(float64); int(code) != call.code {
		return fmt.Errorf("the audit event for %s has responseStatus %v, and the client was answered %d: the code recorded is the one the response went out with, which the auditing learns by wrapping the ResponseWriter", call.what, e["responseStatus"], call.code)
	}

	user, _ := e["user"].(map[string]any)
	if want.user == "" {
		if name, _ := user["username"].(string); name != "" && name != "system:anonymous" {
			return fmt.Errorf("the audit event for %s, refused with 401, records the user as %q: authentication failed, so who sent it is exactly what is not known — the event is still written, with the code, and an empty user", call.what, name)
		}
	} else {
		groups, _ := user["groups"].([]any)
		if user["username"] != want.user || (want.group != "" && !contains(groups, want.group)) {
			return fmt.Errorf("the audit event for %s records user %v, and it is %s with %q among the groups: the user is whoever authentication decided the request is from, so the auditing has to learn it after authentication has run", call.what, e["user"], want.user, want.group)
		}
	}
	ips, _ := e["sourceIPs"].([]any)
	if !contains(ips, "127.0.0.1") {
		return fmt.Errorf("the audit event for %s has sourceIPs %v, and the request came from 127.0.0.1: the address it arrived from is in the request's RemoteAddr, after the port is cut off", call.what, e["sourceIPs"])
	}
	if e["userAgent"] != auditAgent {
		return fmt.Errorf("the audit event for %s has userAgent %v, and the client sent User-Agent %q", call.what, e["userAgent"], auditAgent)
	}
	received, err1 := time.Parse(time.RFC3339Nano, fmt.Sprint(e["requestReceivedTimestamp"]))
	at, err2 := time.Parse(time.RFC3339Nano, fmt.Sprint(e["stageTimestamp"]))
	if err1 != nil || err2 != nil {
		return fmt.Errorf("the audit event for %s has requestReceivedTimestamp %v and stageTimestamp %v, and both are RFC 3339 times: when the request arrived, and when this stage of it was recorded", call.what, e["requestReceivedTimestamp"], e["stageTimestamp"])
	}
	if at.Before(received) || time.Since(received) > 5*time.Minute || time.Until(received) > time.Minute {
		return fmt.Errorf("the audit event for %s says the request arrived at %v and the stage was recorded at %v: the arrival is now-ish, and comes first", call.what, received, at)
	}

	ref, hasRef := e["objectRef"].(map[string]any)
	if want.nonResource {
		if hasRef && len(ref) > 0 {
			return fmt.Errorf("the audit event for %s has objectRef %v, and %s is not an object in the API: only requests under /api and /apis name a resource", call.what, ref, call.uri)
		}
	} else {
		for field, value := range map[string]string{"resource": want.resource, "namespace": want.namespace, "name": want.name, "subresource": want.subresource, "apiVersion": "v1"} {
			got, _ := ref[field].(string)
			if value == "" && field == "name" {
				continue
			}
			if got != value {
				return fmt.Errorf("the audit event for %s has objectRef %v, and its %s is %q: the objectRef is what the URL is about, read off the path — /api/v1/namespaces/<namespace>/<resource>/<name>/<subresource>",
					call.what, e["objectRef"], field, value)
			}
		}
	}

	_, hasRequest := e["requestObject"]
	request, _ := e["requestObject"].(map[string]any)
	_, hasResponse := e["responseObject"]
	response, _ := e["responseObject"].(map[string]any)
	switch {
	case want.requestObject && request == nil:
		return fmt.Errorf("the audit event for %s, at level %s, has no requestObject: Request and RequestResponse record the body the client sent, as JSON, not as a string", call.what, want.level)
	case !want.requestObject && hasRequest:
		return fmt.Errorf("the audit event for %s, at level %s, has a requestObject: %s", call.what, want.level, auditLevelNote(want))
	case want.responseObject && response == nil:
		return fmt.Errorf("the audit event for %s, at level %s, has no responseObject: RequestResponse also records the body the server answered with, which means keeping a copy of what is written to the ResponseWriter", call.what, want.level)
	case !want.responseObject && hasResponse:
		return fmt.Errorf("the audit event for %s, at level %s, has a responseObject: %s", call.what, want.level, auditLevelNote(want))
	}
	if want.responseObject && want.verb == "create" && metaField(response, "uid") == "" {
		return fmt.Errorf("the responseObject for %s has no metadata.uid: it is the object the server answered with, which has the fields the server set, not a second copy of the request", call.what)
	}
	if want.requestObject && want.verb == "create" && metaField(request, "uid") != "" {
		return fmt.Errorf("the requestObject for %s has a metadata.uid, which the client never sent: it is the body as it arrived, before the server filled anything in", call.what)
	}
	return nil
}

func auditLevelNote(want auditWant) string {
	switch {
	case want.verb == "watch":
		return "a watch's response is a stream with no end, and the real server never records it"
	case want.level == "Metadata":
		return "Metadata is who, what and when, and no bodies — which is what makes it the level that is safe to keep for everything, Secrets included"
	case want.level == "Request":
		return "Request records what the client sent and not what it was answered"
	default:
		return "a GET has no body to record"
	}
}
