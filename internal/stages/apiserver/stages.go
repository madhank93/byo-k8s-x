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
	register(Stage{Slug: "resource-get", Run: stageResourceGet})
	register(Stage{Slug: "list", Run: stageList})
	register(Stage{Slug: "update", Run: stageUpdate})
	register(Stage{Slug: "delete", Run: stageDelete})
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
	res, body, err := srv.send(ctx, http.MethodPost, path, sent)
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
	res, body, err = srv.send(ctx, http.MethodPost, path, sent)
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
	res, body, err = srv.send(ctx, http.MethodPost, path, map[string]any{
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

// stageResourceGet checks a stored object can be asked for by name, and that a
// name nothing is stored under is a 404 a client can branch on.
//
// A get is the cheapest thing this server does and the one every other verb is
// built on: an update reads before it writes, a watch is a get that does not
// end, and `kubectl get` is this endpoint with a printer attached.
func stageResourceGet(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const path = "/api/v1/namespaces/default/configmaps"
	created, err := srv.create(ctx, path, map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "settings"},
		"data":       map[string]any{"colour": "blue"},
	})
	if err != nil {
		return err
	}

	got, err := srv.getJSON(ctx, path+"/settings")
	if err != nil {
		return err
	}
	// A client is handed this object on its own, with no URL attached, and has
	// to be able to say what it is: apiVersion and kind are how it can.
	if got["apiVersion"] != "v1" || got["kind"] != "ConfigMap" {
		return fmt.Errorf("GET %s/settings answered apiVersion %v and kind %v: every object this server hands back says what it is, because a client that read it from a file or a pipe has nothing else to go on",
			path, got["apiVersion"], got["kind"])
	}
	if data, _ := got["data"].(map[string]any); data["colour"] != "blue" {
		return fmt.Errorf("GET %s/settings answered data %v rather than what was stored: a get reads the store, it does not make something up", path, got["data"])
	}
	// The same object, not a new one made to look like it. The uid especially:
	// a get that mints a fresh one has broken every owner reference in the
	// cluster, and nothing would say so.
	for _, field := range []string{"uid", "creationTimestamp", "resourceVersion"} {
		if want, have := metaField(created, field), metaField(got, field); want != have {
			return fmt.Errorf("the created object had metadata.%s = %q and the get answered %q: a get returns the stored object, so these do not change between the create and the read",
				field, want, have)
		}
	}
	if metaField(got, "name") != "settings" {
		return fmt.Errorf("the object at %s/settings has metadata.name = %q: the stored object carries its own name", path, metaField(got, "name"))
	}

	// A name nothing is stored under. This is the reply client-go turns into
	// the error IsNotFound asks about, and controllers are written around it:
	// "get it, and if it is not there, create it" is most of a reconcile loop.
	res, body, err := srv.send(ctx, http.MethodGet, path+"/absent", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+path+"/absent", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return err
	}

	// A namespace is part of the key, not decoration on it. An object stored
	// in another namespace is not reachable through this one, or every
	// namespace in the cluster is the same namespace.
	if _, err := srv.create(ctx, "/api/v1/namespaces/other/configmaps", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "elsewhere"},
	}); err != nil {
		return err
	}
	res, body, err = srv.send(ctx, http.MethodGet, path+"/elsewhere", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+path+"/elsewhere, for an object stored in the namespace other", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return err
	}
	return nil
}

// stageList checks the collection endpoint: a List object, in name order,
// scoped to its namespace, and empty rather than absent when there is nothing.
//
// The list is also where a watch begins. Its resourceVersion is the point in
// the store's history the answer describes, which is what lets a client list,
// then watch from that number, and neither miss a write nor see one twice.
func stageList(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const path = "/api/v1/namespaces/default/configmaps"
	// Stored out of order on purpose: the order in the answer is the server's
	// to decide, not an accident of how the map happened to be filled.
	for _, name := range []string{"beta", "alpha"} {
		if _, err := srv.create(ctx, path, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": name},
			"data":       map[string]any{"colour": "blue"},
		}); err != nil {
			return err
		}
	}
	if _, err := srv.create(ctx, "/api/v1/namespaces/other/configmaps", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "gamma"},
	}); err != nil {
		return err
	}

	list, err := srv.getJSON(ctx, path)
	if err != nil {
		return err
	}
	if list["kind"] != "ConfigMapList" || list["apiVersion"] != "v1" {
		return fmt.Errorf("GET %s answered kind %v and apiVersion %v, and a collection is a <Kind>List — ConfigMapList here: a client decides how to decode what came back from the kind, and a bare JSON array is not something it can",
			path, list["kind"], list["apiVersion"])
	}
	items, ok := list["items"].([]any)
	if !ok {
		return fmt.Errorf("GET %s has no items array: the objects in a collection live under items, beside the list's own metadata\nthe body was:\n%v", path, list)
	}
	var names []string
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("GET %s has an item that is not an object: a list holds whole objects, not names", path)
		}
		names = append(names, metaField(obj, "name"))
		// Whole objects, stamped as they were stored. A list that strips the
		// metadata makes every client re-get what it already has.
		for _, field := range []string{"uid", "resourceVersion"} {
			if metaField(obj, field) == "" {
				return fmt.Errorf("the item %q in %s has no metadata.%s: the objects in a list are the stored objects, complete — an informer builds its whole cache out of one list and never gets them individually",
					metaField(obj, "name"), path, field)
			}
		}
	}
	if strings.Join(names, ",") != "alpha,beta" {
		return fmt.Errorf("GET %s listed %v, and this namespace holds alpha and beta in that order: gamma is in the namespace other, and a list sorted by name is what makes kubectl's output the same twice in a row",
			path, names)
	}

	// The list's own resourceVersion, which is not any item's: it is where a
	// watch started from this answer would begin.
	if metaField(list, "resourceVersion") == "" {
		return fmt.Errorf("GET %s has no metadata.resourceVersion on the list itself: a client lists, then watches from that number, and without it there is no way to ask for \"everything after what I just read\"", path)
	}

	// A namespace with nothing in it is an empty list, not a 404 and not null.
	// Every client does `for _, item := range list.Items`, and a nil there is
	// the difference between "nothing yet" and a crash.
	empty, err := srv.getJSON(ctx, "/api/v1/namespaces/empty/configmaps")
	if err != nil {
		return fmt.Errorf("%w\n\nlisting a namespace nothing has been stored in is a 200 with an empty list: there is no such thing as a namespace that does not exist yet to a collection endpoint", err)
	}
	if items, ok := empty["items"].([]any); !ok || len(items) != 0 {
		return fmt.Errorf("listing an empty namespace answered items %v, and an empty collection is an empty array: a Go client that ranges over a null gets nothing, but one that reads len() of an absent field is reading a nil map",
			empty["items"])
	}
	return nil
}

// stageUpdate checks a PUT replaces a stored object, keeps the parts of it that
// are the server's, and refuses a write built on a version that has moved on.
//
// This is the stage where resourceVersion stops being decoration. Two clients
// read the same object and both write it back; without a version check the
// second silently erases the first, and that is the bug every controller in the
// cluster would be quietly hitting.
func stageUpdate(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const path = "/api/v1/namespaces/default/configmaps"
	created, err := srv.create(ctx, path, map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "settings"},
		"data":       map[string]any{"colour": "blue"},
	})
	if err != nil {
		return err
	}
	first := metaField(created, "resourceVersion")

	// A read-modify-write, the way every client does it — and with a uid and a
	// creationTimestamp the client had no business inventing, because identity
	// is the server's to keep.
	res, body, err := srv.send(ctx, http.MethodPut, path+"/settings", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":              "settings",
			"resourceVersion":   first,
			"uid":               "11111111-1111-1111-1111-111111111111",
			"creationTimestamp": "2001-01-01T00:00:00Z",
		},
		"data": map[string]any{"colour": "green"},
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s/settings answered %d, and an update that replaced something answers 200: 201 is a create, and a client that asked to update an object it had read would have to wonder which happened\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	updated, err := decode(body)
	if err != nil {
		return fmt.Errorf("the reply to PUT %s/settings is not JSON (%w)\nthe body was:\n%s", path, err, tail(string(body)))
	}
	if data, _ := updated["data"].(map[string]any); data["colour"] != "green" {
		return fmt.Errorf("after the update the object's data is %v rather than what was sent: a PUT replaces the object, it does not merge into it — merging is what the patch stages are for", updated["data"])
	}
	second := metaField(updated, "resourceVersion")
	if second == first {
		return fmt.Errorf("the object still has resourceVersion %q after the update: every write moves the store forward and stamps the new number on the object, or a client cannot tell whether what it is holding is current", first)
	}
	for _, field := range []string{"uid", "creationTimestamp"} {
		if metaField(updated, field) != metaField(created, field) {
			return fmt.Errorf("the update changed metadata.%s from %q to %q: those two fields are the server's and are set once, at create — a client is free to send nonsense back and the server is not free to believe it",
				field, metaField(created, field), metaField(updated, field))
		}
	}
	// The reply is easy to get right by echoing the request. The store is the
	// part that matters.
	stored, err := srv.getJSON(ctx, path+"/settings")
	if err != nil {
		return err
	}
	if data, _ := stored["data"].(map[string]any); data["colour"] != "green" {
		return fmt.Errorf("the update answered 200 but a get still reads data %v: the reply to a write has to be what was stored, not what was sent", stored["data"])
	}

	// The same write again, still claiming the version it read the first time.
	// This is the conflict every controller retries on.
	res, body, err = srv.send(ctx, http.MethodPut, path+"/settings", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "settings", "resourceVersion": first},
		"data":       map[string]any{"colour": "red"},
	})
	if err != nil {
		return err
	}
	if err := wantStatus("PUT "+path+"/settings with the resourceVersion the object had before the last write", res, body, http.StatusConflict, "Conflict"); err != nil {
		return fmt.Errorf("%w\n\nthis is optimistic concurrency, and it is the whole reason the field exists: two clients read the same object, both write it back, and the one working from a version that has moved on has to be told rather than allowed to erase the other", err)
	}
	stored, err = srv.getJSON(ctx, path+"/settings")
	if err != nil {
		return err
	}
	if data, _ := stored["data"].(map[string]any); data["colour"] != "green" {
		return fmt.Errorf("the refused update changed the object anyway: it now reads %v, and a write that answered 409 has to have left the store alone", stored["data"])
	}

	// No resourceVersion at all is a client saying it does not care what was
	// there. That is allowed — `kubectl replace` does exactly this — and it is
	// the caller taking the risk knowingly rather than by accident.
	res, body, err = srv.send(ctx, http.MethodPut, path+"/settings", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "settings"},
		"data":       map[string]any{"colour": "red"},
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s/settings with no metadata.resourceVersion answered %d, and an update that names no version is unconditional: it is a client saying \"whatever is there, make it this\", which is what kubectl replace does\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}

	// A name nothing is stored under. The real server can create through a PUT
	// for a few resources, but a client that sent an update expects to hear
	// that what it was updating is gone.
	res, body, err = srv.send(ctx, http.MethodPut, path+"/absent", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "absent"},
	})
	if err != nil {
		return err
	}
	if err := wantStatus("PUT "+path+"/absent", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return err
	}

	// The name is in the URL and in the object, and an update is not a rename.
	// Two names in one request is a request the server cannot carry out as
	// asked, whichever one it picked.
	res, body, err = srv.send(ctx, http.MethodPut, path+"/settings", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "something-else"},
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusBadRequest {
		return fmt.Errorf("PUT %s/settings carrying metadata.name \"something-else\" answered %d, and a name in the body that disagrees with the one in the URL is a 400: an update is not a rename, and picking a winner writes to a URL nobody asked for\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	return nil
}

// stageDelete checks an object can be taken back out, that what is gone reads
// as gone, and that the name coming free does not bring the old object back.
//
// The last part is the point. A name is a slot; the uid is the object. Reusing
// a name after a delete gives a new uid, which is what stops a controller
// adopting a stranger it thinks it created earlier — the controller course's
// owner references rest on exactly this.
func stageDelete(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const path = "/api/v1/namespaces/default/configmaps"
	send := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "doomed"},
		"data":       map[string]any{"colour": "blue"},
	}
	created, err := srv.create(ctx, path, send)
	if err != nil {
		return err
	}

	res, body, err := srv.send(ctx, http.MethodDelete, path+"/doomed", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE %s/doomed answered %d, and a delete that removed something answers 200\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
	}
	// The real server answers a delete with either the object as it last was
	// or a Status saying Success, depending on the resource. Both are useful —
	// a client learns the delete happened rather than inferring it — and an
	// empty body is neither.
	answer, err := decode(body)
	if err != nil {
		return fmt.Errorf("the reply to DELETE %s/doomed is not JSON (%w): a delete answers with the object it removed, or a Status saying Success — not an empty body\nthe body was:\n%s",
			path, err, tail(string(body)))
	}
	if answer["kind"] == "Status" {
		if answer["status"] != "Success" {
			return fmt.Errorf("the reply to DELETE %s/doomed is a Status with status %v, and a delete that worked says Success\nthe body was:\n%s", path, answer["status"], tail(string(body)))
		}
	} else if metaField(answer, "name") != "doomed" {
		return fmt.Errorf("the reply to DELETE %s/doomed is neither the object it removed nor a Status saying Success\nthe body was:\n%s", path, tail(string(body)))
	}

	res, body, err = srv.send(ctx, http.MethodGet, path+"/doomed", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+path+"/doomed after deleting it", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return err
	}

	list, err := srv.getJSON(ctx, path)
	if err != nil {
		return err
	}
	if items, _ := list["items"].([]any); len(items) != 0 {
		return fmt.Errorf("GET %s still lists %d object(s) after the only one was deleted: a delete removes it from the store, so every reader stops seeing it — not only the one that asks by name",
			path, len(items))
	}

	// Deleting what is not there is a 404, and a client relies on it: a
	// cleanup that runs twice has to be able to tell "I removed it" from "it
	// was already gone" without either being an error it has to stop on.
	res, body, err = srv.send(ctx, http.MethodDelete, path+"/doomed", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("DELETE "+path+"/doomed a second time", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return err
	}

	// The name is free again, and what takes it is a different object.
	again, err := srv.create(ctx, path, send)
	if err != nil {
		return fmt.Errorf("%w\n\nthe name was deleted, so storing it again is a create that works: a 409 here means the delete left something behind", err)
	}
	if metaField(again, "uid") == metaField(created, "uid") {
		return fmt.Errorf("the recreated object has the same metadata.uid %q as the one that was deleted: a uid is minted per object, not per name — this is what makes the second \"doomed\" a different object to everything that referenced the first, and why owner references are uid-based",
			metaField(again, "uid"))
	}
	if metaField(again, "resourceVersion") == metaField(created, "resourceVersion") {
		return fmt.Errorf("the recreated object has the same metadata.resourceVersion %q as the one that was deleted: the counter only goes forward, and a watcher that saw the first create has to be able to place the delete and the second create after it",
			metaField(again, "resourceVersion"))
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

// send makes one request, with an optional object as its JSON body, and does
// not insist on the answer: a stage that is checking a failure needs the code
// and the body it came with.
func (s *server) send(ctx context.Context, method, path string, body any) (*http.Response, []byte, error) {
	var raw []byte
	if body != nil {
		var err error
		if raw, err = json.Marshal(body); err != nil {
			return nil, nil, fmt.Errorf("encode the request body: %w", err)
		}
	}
	res, got, err := request(ctx, method, s.url+path, raw)
	if err != nil {
		return nil, nil, fmt.Errorf("%s %s: %w\nthe program said:\n%s", method, path, err, tail(s.p.Stdout()))
	}
	return res, got, nil
}

// create stores one object and returns it as the server stored it, which is
// where a later request gets the name, uid and resourceVersion it needs.
func (s *server) create(ctx context.Context, path string, body any) (map[string]any, error) {
	res, raw, err := s.send(ctx, http.MethodPost, path, body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("POST %s answered %d rather than 201, and this stage builds on a create that works — stage 3 is where that is graded\nthe body was:\n%s",
			path, res.StatusCode, tail(string(raw)))
	}
	obj, err := decode(raw)
	if err != nil {
		return nil, fmt.Errorf("the reply to POST %s is not JSON (%w)\nthe body was:\n%s", path, err, tail(string(raw)))
	}
	return obj, nil
}

// wantStatus insists a reply is the failure clients are written against: the
// HTTP code, and a Status object carrying the reason they branch on.
func wantStatus(what string, res *http.Response, body []byte, code int, reason string) error {
	if res.StatusCode != code {
		return fmt.Errorf("%s answered %d, and this one is a %d\nthe body was:\n%s", what, res.StatusCode, code, tail(string(body)))
	}
	status, err := decode(body)
	if err != nil {
		return fmt.Errorf("the %d for %s is not JSON (%w): every failure this server reports is a Status object\nthe body was:\n%s",
			code, what, err, tail(string(body)))
	}
	if status["kind"] != "Status" || status["reason"] != reason {
		return fmt.Errorf("the %d for %s has kind %v and reason %v, and this failure is a Status whose reason is %q: the reason is what client-go turns into a typed error, not the message\nthe body was:\n%s",
			code, what, status["kind"], status["reason"], reason, tail(string(body)))
	}
	return nil
}

// metaField reads one metadata field of an object, which is a string or absent.
func metaField(obj map[string]any, field string) string {
	m, _ := obj["metadata"].(map[string]any)
	s, _ := m[field].(string)
	return s
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
