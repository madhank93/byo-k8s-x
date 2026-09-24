// Package apiserver holds the assertions for the "Build your own kube-apiserver"
// course — one function per stage, registered by slug.
//
// This course is graded with no cluster at all. The learner's program is the
// server, started on an address of the harness's choosing, and the verdict is
// what it answers over HTTP: the status code, the headers, and the JSON. Late
// stages hand the real kubectl a kubeconfig pointing at it and let that be the
// assertion instead; nothing inspects the learner's source.
package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

var registry = map[string]Stage{}

func register(s Stage) { registry[s.Slug] = s }

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := registry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "serve", Run: stageServe})
	register(Stage{Slug: "discovery-root", Run: stageDiscoveryRoot})
	register(Stage{Slug: "resource-create", Run: stageResourceCreate})
}

// stageServe checks the program serves HTTP where it was told to, says it is
// healthy, and refuses what it does not have.
//
// The address is not a constant anywhere in this course: the harness picks a
// free port and passes it, so a program that ignores the flag and listens on
// its own favourite port is answering nobody.
func stageServe(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	for _, path := range []string{"/healthz", "/livez", "/readyz"} {
		res, body, err := request(ctx, http.MethodGet, srv.url+path, nil)
		if err != nil {
			return fmt.Errorf("GET %s: %w\nthe program said:\n%s", path, err, tail(srv.p.Stdout()))
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s answered %d, and a server that is up answers 200: kubelet, load balancers and anything watching this process read these three and nothing else\nthe body was:\n%s",
				path, res.StatusCode, tail(string(body)))
		}
		if !strings.HasPrefix(strings.TrimSpace(string(body)), "ok") {
			return fmt.Errorf("GET %s answered 200 with body %q, and the answer these endpoints give is \"ok\"", path, strings.TrimSpace(string(body)))
		}
	}

	// A path this server does not serve is a 404 carrying a Status, because
	// every client reads the reason out of the body rather than the code.
	res, body, err := request(ctx, http.MethodGet, srv.url+"/nothing-here", nil)
	if err != nil {
		return fmt.Errorf("GET /nothing-here: %w", err)
	}
	if res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GET /nothing-here answered %d, and a path this server does not serve is a 404", res.StatusCode)
	}
	status, err := decode(body)
	if err != nil {
		return fmt.Errorf("the 404 for /nothing-here is not JSON (%w): every failure this server reports is a Status object\nthe body was:\n%s", err, tail(string(body)))
	}
	for field, want := range map[string]any{"kind": "Status", "status": "Failure", "code": float64(404)} {
		if got := status[field]; got != want {
			return fmt.Errorf("the 404 for /nothing-here has %s = %v, and a Status object carries %s = %v: this is the object client-go turns back into a typed error, so IsNotFound can be asked on the other side\nthe body was:\n%s",
				field, got, field, want, tail(string(body)))
		}
	}
	return nil
}

// stageDiscoveryRoot checks the three answers every client reads before it
// sends a single request of its own.
//
// kubectl does not know what a ConfigMap is. It asks, and what comes back
// decides which URL it builds, whether it puts a namespace in that URL, and
// what it prints. A server whose discovery is wrong is a server nothing can
// drive, however well the resources underneath it work.
func stageDiscoveryRoot(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	root, err := srv.getJSON(ctx, "/api")
	if err != nil {
		return err
	}
	if root["kind"] != "APIVersions" {
		return fmt.Errorf("GET /api answered kind %v, and the root of the core API is an APIVersions object", root["kind"])
	}
	versions, _ := root["versions"].([]any)
	if !contains(versions, "v1") {
		return fmt.Errorf("GET /api answered versions %v, which does not offer v1: the core group is version v1 and a client that cannot find it stops there", root["versions"])
	}

	// Everything outside the core group lives under /apis. There is nothing
	// there yet, and the empty list still has to be served.
	groups, err := srv.getJSON(ctx, "/apis")
	if err != nil {
		return err
	}
	if groups["kind"] != "APIGroupList" {
		return fmt.Errorf("GET /apis answered kind %v, and the list of named groups is an APIGroupList: serving no groups is an empty list, not a 404", groups["kind"])
	}
	if _, ok := groups["groups"].([]any); !ok {
		return fmt.Errorf("GET /apis has no groups array: a client reads it before it decides the server is broken")
	}

	list, err := srv.getJSON(ctx, "/api/v1")
	if err != nil {
		return err
	}
	if list["kind"] != "APIResourceList" || list["groupVersion"] != "v1" {
		return fmt.Errorf("GET /api/v1 answered kind %v and groupVersion %v, and what a version offers is an APIResourceList with groupVersion v1",
			list["kind"], list["groupVersion"])
	}
	resources, _ := list["resources"].([]any)
	var configmaps map[string]any
	for _, r := range resources {
		if m, ok := r.(map[string]any); ok && m["name"] == "configmaps" {
			configmaps = m
		}
	}
	if configmaps == nil {
		return fmt.Errorf("GET /api/v1 lists no resource named configmaps: the plural name is the URL segment, and it is how a client turns the word configmap into a request")
	}
	if configmaps["kind"] != "ConfigMap" {
		return fmt.Errorf("the configmaps entry has kind %v: the kind is what a client puts in the objects it sends and matches in what it gets back", configmaps["kind"])
	}
	if namespaced, _ := configmaps["namespaced"].(bool); !namespaced {
		return fmt.Errorf("the configmaps entry is not namespaced: this one field decides whether a client builds /api/v1/configmaps or /api/v1/namespaces/<ns>/configmaps")
	}
	verbs, _ := configmaps["verbs"].([]any)
	for _, want := range []string{"create", "get", "list"} {
		if !contains(verbs, want) {
			return fmt.Errorf("the configmaps entry does not offer the verb %q (it offers %v): kubectl hides a command whose verb is not listed", want, configmaps["verbs"])
		}
	}
	return nil
}

// stageResourceCreate checks a POST stores an object, and gives back what only
// the server could know about it.
//
// A create is not an echo. The client sends a name and some data; what comes
// back carries a uid, a creationTimestamp and a resourceVersion, and every
// later stage of this course is built on those three.
func stageResourceCreate(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const path = "/api/v1/namespaces/default/configmaps"
	sent := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "settings"},
		"data":       map[string]any{"colour": "blue"},
	}
	res, body, err := srv.post(ctx, path, sent)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("POST %s answered %d, and a create that stored something answers 201: 200 is what an update answers, and a client tells them apart\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	stored, err := decode(body)
	if err != nil {
		return fmt.Errorf("the reply to POST %s is not JSON (%w)\nthe body was:\n%s", path, err, tail(string(body)))
	}
	meta, _ := stored["metadata"].(map[string]any)
	if meta == nil {
		return fmt.Errorf("the reply to POST %s carries no metadata: what is stored is the object plus what the server stamped on it\nthe body was:\n%s", path, tail(string(body)))
	}
	for _, field := range []string{"uid", "creationTimestamp", "resourceVersion"} {
		if s, _ := meta[field].(string); s == "" {
			return fmt.Errorf("the created object has no metadata.%s: a create answers with what only the server knows, and every later stage of this course is built on those three fields\nthe body was:\n%s",
				field, tail(string(body)))
		}
	}
	if _, err := time.Parse(time.RFC3339, meta["creationTimestamp"].(string)); err != nil {
		return fmt.Errorf("metadata.creationTimestamp is %q, which is not RFC 3339: a client parses it as a time and refuses what it cannot", meta["creationTimestamp"])
	}
	if meta["namespace"] != "default" {
		return fmt.Errorf("the created object has namespace %v: the namespace comes from the URL, and the stored object has to say which one it is in", meta["namespace"])
	}
	if data, _ := stored["data"].(map[string]any); data["colour"] != "blue" {
		return fmt.Errorf("the created object's data is %v rather than what was sent: the server adds to the object it was given, it does not replace it", stored["data"])
	}

	// The same name twice is the one conflict every client is written to
	// expect, and it is a 409 with a reason on it.
	res, body, err = srv.post(ctx, path, sent)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusConflict {
		return fmt.Errorf("POST %s twice with the name settings answered %d the second time, and a name that is taken is a 409: a second create that succeeds has silently thrown the first object away\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	status, err := decode(body)
	if err != nil {
		return fmt.Errorf("the 409 is not JSON (%w)\nthe body was:\n%s", err, tail(string(body)))
	}
	if status["kind"] != "Status" || status["reason"] != "AlreadyExists" {
		return fmt.Errorf("the 409 has kind %v and reason %v, and this failure is a Status whose reason is AlreadyExists: the reason is what client-go matches, not the message\nthe body was:\n%s",
			status["kind"], status["reason"], tail(string(body)))
	}

	// An object with no name cannot be stored under one. The request was
	// understood, so it is the object that is refused.
	res, body, err = srv.post(ctx, path, map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{},
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusUnprocessableEntity {
		return fmt.Errorf("POST %s with no metadata.name answered %d, and an object the server understood but cannot accept is a 422\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	return nil
}

// server is the learner's program, running and reachable.
type server struct {
	p   *runner.Process
	url string
}

// serve starts the program on a free port of the harness's choosing and waits
// for it to answer, returning it and the cleanup that stops it.
func serve(ctx context.Context, bin string) (*server, func(), error) {
	addr, err := freeAddr()
	if err != nil {
		return nil, nil, err
	}
	p, err := runner.Start(ctx, bin, nil, "-addr", addr)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { p.Stop(5 * time.Second) }

	srv := &server{p: p, url: "http://" + addr}
	// The program has to be up before anything is asked of it, and how long
	// that takes is not its fault: a cold binary on a busy machine is slow
	// once and never again.
	deadline := time.Now().Add(30 * time.Second)
	for {
		res, _, err := request(ctx, http.MethodGet, srv.url+"/healthz", nil)
		if err == nil && res.StatusCode == http.StatusOK {
			return srv, cleanup, nil
		}
		if done, result := p.Exited(); done {
			cleanup()
			return nil, nil, fmt.Errorf("the program exited (%d) instead of serving on %s\nstdout:\n%s\nstderr:\n%s",
				result.ExitCode, addr, tail(result.Stdout), tail(result.Stderr))
		}
		if time.Now().After(deadline) {
			cleanup()
			return nil, nil, fmt.Errorf("nothing answered GET /healthz on %s within 30s: the address to serve on is the -addr flag, and the harness picks a free port rather than expecting one\nthe program said:\n%s\nstderr:\n%s",
				addr, tail(p.Stdout()), tail(p.Stderr()))
		}
		select {
		case <-ctx.Done():
			cleanup()
			return nil, nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// getJSON reads one JSON object from a path that has to answer 200.
func (s *server) getJSON(ctx context.Context, path string) (map[string]any, error) {
	res, body, err := request(ctx, http.MethodGet, s.url+path, nil)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w\nthe program said:\n%s", path, err, tail(s.p.Stdout()))
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s answered %d rather than 200\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return nil, fmt.Errorf("GET %s answered with Content-Type %q, and these answers are JSON: a client that asked for JSON and got text decides the server is broken", path, ct)
	}
	obj, err := decode(body)
	if err != nil {
		return nil, fmt.Errorf("GET %s did not answer JSON (%w)\nthe body was:\n%s", path, err, tail(string(body)))
	}
	return obj, nil
}

// post sends one object as JSON.
func (s *server) post(ctx context.Context, path string, body any) (*http.Response, []byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, nil, fmt.Errorf("encode the request body: %w", err)
	}
	res, got, err := request(ctx, http.MethodPost, s.url+path, raw)
	if err != nil {
		return nil, nil, fmt.Errorf("POST %s: %w\nthe program said:\n%s", path, err, tail(s.p.Stdout()))
	}
	return res, got, nil
}

// request makes one HTTP request and reads the whole answer, which is small
// enough here that streaming it would only hide the body from an error message.
func request(ctx context.Context, method, url string, body []byte) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	got, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("read the response body: %w", err)
	}
	return res, got, nil
}

// freeAddr is an address nothing is listening on. The listener is closed
// before the program is told to use it, which is a race the operating system
// makes unlikely rather than impossible — and the alternative, a fixed port,
// collides with whatever else is on the machine every time.
func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("find a free port: %w", err)
	}
	defer l.Close()
	return l.Addr().String(), nil
}

func decode(body []byte) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func contains(list []any, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// tail keeps an error message readable when the program has been talkative.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "(nothing)"
	}
	lines := strings.Split(s, "\n")
	if len(lines) > 20 {
		lines = append([]string{"…"}, lines[len(lines)-20:]...)
	}
	return "  " + strings.Join(lines, "\n  ")
}
