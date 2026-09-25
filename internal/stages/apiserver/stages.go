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
	"net/url"
	"os"
	"strconv"
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
	register(Stage{Slug: "etcd", Run: stageEtcd})
	register(Stage{Slug: "resourceversion", Run: stageResourceVersion})
	register(Stage{Slug: "namespaces", Run: stageNamespaces})
	register(Stage{Slug: "field-selector", Run: stageFieldSelector})
	register(Stage{Slug: "label-selector", Run: stageLabelSelector})
	register(Stage{Slug: "pagination", Run: stagePagination})
	register(Stage{Slug: "watch", Run: stageWatch})
	register(Stage{Slug: "watch-from-rv", Run: stageWatchFromRV})
	register(Stage{Slug: "bookmarks", Run: stageBookmarks})
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
	if _, err := srv.create(ctx, "/api/v1/namespaces/kube-public/configmaps", map[string]any{
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
	if err := wantStatus("GET "+path+"/elsewhere, for an object stored in the namespace kube-public", res, body, http.StatusNotFound, "NotFound"); err != nil {
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
	if _, err := srv.create(ctx, "/api/v1/namespaces/kube-public/configmaps", map[string]any{
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
		return fmt.Errorf("GET %s listed %v, and this namespace holds alpha and beta in that order: gamma is in the namespace kube-public, and a list sorted by name is what makes kubectl's output the same twice in a row",
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

// stageEtcd checks the store outlives the process: objects written before a
// restart are there after it, with everything the server stamped on them, and
// the counter carries on from where it was rather than starting again.
//
// The real server keeps none of this itself — etcd does, and the apiserver is
// a stateless process in front of it. What is being graded here is the same
// contract: nothing the server answered for may go missing because it stopped.
func stageEtcd(ctx context.Context, _ *kube.Env, bin string) error {
	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the store: %w", err)
	}
	defer os.RemoveAll(dir)

	const path = "/api/v1/namespaces/default/configmaps"
	before := map[string]map[string]any{}
	err = func() error {
		srv, cleanup, err := serve(ctx, bin, "-data", dir)
		if err != nil {
			return fmt.Errorf("%w\n\nthis stage passes -data <dir>, a directory to keep the objects in: a program that does not take the flag is told to serve on a port and nothing else, and flag parsing stops it before it starts", err)
		}
		defer cleanup()
		for _, name := range []string{"alpha", "beta"} {
			obj, err := srv.create(ctx, path, map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": name},
				"data":       map[string]any{"colour": "blue"},
			})
			if err != nil {
				return err
			}
			before[name] = obj
		}
		// Deleted before the restart, and it has to stay deleted: a store
		// replayed from what was written without what was removed brings the
		// object back, and nothing in a single run would ever show it.
		res, body, err := srv.send(ctx, http.MethodDelete, path+"/beta", nil)
		if err != nil {
			return err
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("DELETE %s/beta answered %d rather than 200, and this stage builds on a delete that works\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
		}
		list, err := srv.getJSON(ctx, path)
		if err != nil {
			return err
		}
		before["#list"] = list
		return nil
	}()
	if err != nil {
		return err
	}

	// A second process, on a new port, over the same directory. This is a
	// restart as everything outside sees one: the address is not what makes it
	// the same server, the data is.
	srv, cleanup, err := serve(ctx, bin, "-data", dir)
	if err != nil {
		return fmt.Errorf("%w\n\nthe program was started a second time over the directory the first one wrote: a store it cannot read back is worse than no store at all, so a restart that fails here would rather stop than serve an empty cluster", err)
	}
	defer cleanup()

	got, err := srv.getJSON(ctx, path+"/alpha")
	if err != nil {
		return fmt.Errorf("%w\n\nalpha was created before the program was restarted over the same -data directory, and it is gone: what this stage is asking for is a store that outlives the process", err)
	}
	for _, field := range []string{"uid", "creationTimestamp", "resourceVersion"} {
		if want, have := metaField(before["alpha"], field), metaField(got, field); want != have {
			return fmt.Errorf("after the restart alpha has metadata.%s = %q, and it was %q before: the object that comes back is the object that was stored, not one rebuilt from the parts of it that were easy to keep",
				field, have, want)
		}
	}
	if data, _ := got["data"].(map[string]any); data["colour"] != "blue" {
		return fmt.Errorf("after the restart alpha has data %v: the whole object is written down, not only its name", got["data"])
	}

	list, err := srv.getJSON(ctx, path)
	if err != nil {
		return err
	}
	items, _ := list["items"].([]any)
	if len(items) != 1 {
		return fmt.Errorf("after the restart the namespace lists %d object(s), and it holds one — alpha: beta was deleted before the restart and has to stay deleted, which is the difference between writing down every change and writing down what is true",
			len(items))
	}

	// The counter is part of what has to survive. A store that comes back at 1
	// hands out versions clients have already seen, and every watcher holding
	// one is now pointing into a past it cannot tell from the future.
	fresh, err := srv.create(ctx, path, map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "gamma"},
	})
	if err != nil {
		return err
	}
	// Every number this store issues is above every number it has issued
	// before, restart or no restart. A client may only ever compare these for
	// equality, but the server that hands them out orders by them — that
	// ordering is what a watch resumes from, and it is the one thing a fresh
	// counter quietly destroys.
	was, err := strconv.ParseInt(metaField(before["#list"], "resourceVersion"), 10, 64)
	if err != nil {
		return fmt.Errorf("the list answered resourceVersion %q before the restart, which is not a number: this store counts its writes, and stage 9 asks a client to send that number back",
			metaField(before["#list"], "resourceVersion"))
	}
	now, err := strconv.ParseInt(metaField(fresh, "resourceVersion"), 10, 64)
	if err != nil {
		return fmt.Errorf("the object created after the restart has resourceVersion %q, which is not a number", metaField(fresh, "resourceVersion"))
	}
	if now <= was {
		return fmt.Errorf("the store was at %d before the restart and a write after it was given %d: the counter is part of what is written down, and one that starts again hands out numbers clients are still holding — every watcher with an older bookmark is then waiting for changes it has already been told about",
			was, now)
	}
	return nil
}

// stageResourceVersion checks what a resourceVersion on a read means, which is
// not what most people assume the first time they see one.
//
// It is a floor on how old the answer may be, not a filter and not a time
// machine: asking for an old version gets the object as it is now, and asking
// for one the server has never issued is a 504, because in a real cluster the
// client may have read it from a replica this one has not caught up with.
func stageResourceVersion(ctx context.Context, _ *kube.Env, bin string) error {
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
	old := metaField(created, "resourceVersion")
	res, body, err := srv.send(ctx, http.MethodPut, path+"/settings", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "settings"},
		"data":       map[string]any{"colour": "green"},
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s/settings answered %d rather than 200, and this stage builds on an update that works\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
	}

	// The version the object had before that update. The object has moved on,
	// and this read still has to be answered — with what is there now.
	got, err := srv.getJSON(ctx, path+"/settings?resourceVersion="+old)
	if err != nil {
		return fmt.Errorf("%w\n\nthe resourceVersion on a read is a floor, not a filter: %q is older than the store, so the server is already fresh enough to answer and does", err, old)
	}
	if data, _ := got["data"].(map[string]any); data["colour"] != "green" {
		return fmt.Errorf("GET %s/settings?resourceVersion=%s answered data %v, and the answer is the object as it is now — the colour is green, because the update happened: this parameter never asks for an old object, and nothing in the API can. Reading the past is what a watch's history is for, and even that expires",
			path, old, got["data"])
	}

	// 0 is the cheap read: "whatever you already have, do not wait for
	// anything". It is what an informer's first list asks for.
	if _, err := srv.getJSON(ctx, path+"/settings?resourceVersion=0"); err != nil {
		return fmt.Errorf("%w\n\nresourceVersion=0 is a client saying it will take whatever the server has, with no freshness requirement at all — the cheapest read there is, and never an error", err)
	}

	// A version this server has not issued. The client is not wrong: in a real
	// cluster it may have read that number from another apiserver a moment
	// ago, so the answer is "ask again", not "no such thing".
	res, body, err = srv.send(ctx, http.MethodGet, path+"/settings?resourceVersion=99999999", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+path+"/settings?resourceVersion=99999999", res, body, http.StatusGatewayTimeout, "Timeout"); err != nil {
		return fmt.Errorf("%w\n\na read asking for a version past the one the store has reached cannot be answered honestly: the client is holding a number from somewhere this server has not caught up to, and 504 Timeout is how it is told to try again", err)
	}

	// Not a number at all. The request itself is malformed, so this one is a
	// 400 rather than the 422 an object the server understood would get.
	res, body, err = srv.send(ctx, http.MethodGet, path+"/settings?resourceVersion=banana", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusBadRequest {
		return fmt.Errorf("GET %s/settings?resourceVersion=banana answered %d, and a parameter that is not a number is a 400: this is the request being wrong rather than the object, and a server that quietly ignores what it cannot parse answers a question nobody asked\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}

	// The same rules on the collection, where they matter most: a list is
	// where every informer starts, and the number it comes back with is what
	// the watch after it continues from.
	list, err := srv.getJSON(ctx, path+"?resourceVersion=0")
	if err != nil {
		return fmt.Errorf("%w\n\nresourceVersion=0 on a list is how an informer does its first read", err)
	}
	// Whatever the list says it is, the server has to be able to answer a read
	// at that version. A list that reports a number it will then reject is the
	// one thing a client cannot work around.
	at := metaField(list, "resourceVersion")
	if at == "" {
		return fmt.Errorf("the list has no metadata.resourceVersion: stage 5 put it there, and this stage is what makes it useful")
	}
	if _, err := srv.getJSON(ctx, path+"?resourceVersion="+at); err != nil {
		return fmt.Errorf("%w\n\nthe list answered with resourceVersion %q and then refused a read at it: a client lists, then asks for everything at or after that number, and a server that will not answer its own number has nowhere for that client to start", err, at)
	}
	for _, q := range []struct {
		query  string
		code   int
		reason string
	}{
		{"?resourceVersion=99999999", http.StatusGatewayTimeout, "Timeout"},
		{"?resourceVersion=-1", http.StatusBadRequest, "BadRequest"},
	} {
		res, body, err := srv.send(ctx, http.MethodGet, path+q.query, nil)
		if err != nil {
			return err
		}
		if err := wantStatus("GET "+path+q.query, res, body, q.code, q.reason); err != nil {
			return fmt.Errorf("%w\n\nthe collection reads the parameter the same way a single object does: one place decides what a resourceVersion on a read means, and every read goes through it", err)
		}
	}
	return nil
}

// stageNamespaces checks the namespace is an object like any other — created,
// listed, deleted, cluster-scoped — and that being in one means something.
//
// Until now a namespace has been a segment of a URL, and any word would do.
// From here a write has to land in a namespace that exists, deleting one takes
// what was inside it, and there is a URL for the whole cluster at once.
func stageNamespaces(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// A cluster is born with these. default especially: it is where every
	// client goes when its kubeconfig names no namespace, so a server without
	// it answers 404 to the simplest request anyone makes.
	for _, name := range []string{"default", "kube-system"} {
		ns, err := srv.getJSON(ctx, "/api/v1/namespaces/"+name)
		if err != nil {
			return fmt.Errorf("%w\n\nthe namespaces a cluster starts with are made by the server at startup, not by whoever uses it: default, kube-system, kube-public and kube-node-lease", err)
		}
		if ns["kind"] != "Namespace" {
			return fmt.Errorf("GET /api/v1/namespaces/%s answered kind %v, and a namespace is an object of kind Namespace", name, ns["kind"])
		}
		if metaField(ns, "uid") == "" {
			return fmt.Errorf("the namespace %q has no metadata.uid: it is stored like anything else, not a name the server keeps in a list somewhere", name)
		}
		if _, ok := ns["metadata"].(map[string]any)["namespace"]; ok {
			return fmt.Errorf("the namespace %q carries a metadata.namespace: a namespace is cluster-scoped — it is what a namespace field refers to, so it cannot be in one", name)
		}
	}

	// Discovery has to say so, because that one field is how a client decides
	// whether to put a namespace in the URL it builds.
	list, err := srv.getJSON(ctx, "/api/v1")
	if err != nil {
		return err
	}
	resources, _ := list["resources"].([]any)
	var namespaces map[string]any
	for _, r := range resources {
		if m, ok := r.(map[string]any); ok && m["name"] == "namespaces" {
			namespaces = m
		}
	}
	if namespaces == nil {
		return fmt.Errorf("GET /api/v1 lists no resource named namespaces: a client that cannot discover it cannot create one, and kubectl hides every command for a resource it was not told about")
	}
	if namespaced, _ := namespaces["namespaced"].(bool); namespaced {
		return fmt.Errorf("the namespaces entry says namespaced: true, and it is the one resource that cannot be: this field is what makes a client build /api/v1/namespaces/<name> rather than /api/v1/namespaces/<ns>/namespaces/<name>")
	}
	if namespaces["kind"] != "Namespace" {
		return fmt.Errorf("the namespaces entry has kind %v rather than Namespace", namespaces["kind"])
	}

	// A write into a namespace nobody made. Allowing it leaves objects in a
	// place no quota covers, no delete reaches and nothing lists.
	res, body, err := srv.send(ctx, http.MethodPost, "/api/v1/namespaces/nowhere/configmaps", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "orphan"},
	})
	if err != nil {
		return err
	}
	if err := wantStatus("POST /api/v1/namespaces/nowhere/configmaps, where nowhere has never been created", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return fmt.Errorf("%w\n\nup to here any word would do as a namespace. Now the namespace has to exist first, and the 404 says which one is missing rather than which object", err)
	}

	// Made, and now it works. This is `kubectl create namespace` and nothing
	// more: one POST to a cluster-scoped collection.
	made, err := srv.create(ctx, "/api/v1/namespaces", map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": "team-a"},
	})
	if err != nil {
		return err
	}
	if status, _ := made["status"].(map[string]any); status["phase"] != "Active" {
		return fmt.Errorf("the created namespace has status %v, and a namespace that is usable is Active: the other phase is Terminating, which is one that has been deleted and is still being emptied — writes into it are refused until it is gone",
			made["status"])
	}
	if _, err := srv.create(ctx, "/api/v1/namespaces/team-a/configmaps", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "settings"},
		"data":       map[string]any{"colour": "blue"},
	}); err != nil {
		return fmt.Errorf("%w\n\nteam-a was created a moment earlier, so a write into it is a write into a namespace that exists", err)
	}

	// The whole cluster at once: the same resource with no namespace in the
	// path. This is kubectl get cm -A, and a client tells the two URLs apart
	// from discovery alone.
	all, err := srv.getJSON(ctx, "/api/v1/configmaps")
	if err != nil {
		return fmt.Errorf("%w\n\nthe collection without a namespace in it is every configmap in the cluster: one resource, two URLs, and the namespaced one is the special case", err)
	}
	if all["kind"] != "ConfigMapList" {
		return fmt.Errorf("GET /api/v1/configmaps answered kind %v, and it is the same kind of answer as the namespaced list: a client decodes both with the same code", all["kind"])
	}
	items, _ := all["items"].([]any)
	found := map[string]bool{}
	for _, item := range items {
		obj, _ := item.(map[string]any)
		found[metaField(obj, "namespace")+"/"+metaField(obj, "name")] = true
	}
	if !found["team-a/settings"] {
		return fmt.Errorf("GET /api/v1/configmaps does not list team-a/settings, and it is the only configmap there is: an object in the answer carries its own namespace, which is the only way a client reading a cluster-wide list can tell two objects with the same name apart")
	}

	// Deleting a namespace is not deleting a folder. The real cluster marks it
	// Terminating and a controller empties it first; either way, nothing
	// inside outlives it.
	res, body, err = srv.send(ctx, http.MethodDelete, "/api/v1/namespaces/team-a", nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE /api/v1/namespaces/team-a answered %d, and deleting a namespace is a delete like any other\nthe body was:\n%s", res.StatusCode, tail(string(body)))
	}
	res, body, err = srv.send(ctx, http.MethodGet, "/api/v1/namespaces/team-a/configmaps/settings", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET the configmap that was in team-a after team-a was deleted", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return fmt.Errorf("%w\n\nwhat was in the namespace goes with it: a configmap that outlives its namespace is reachable at a URL whose namespace does not exist, and the next team-a to be created inherits somebody else's data", err)
	}
	all, err = srv.getJSON(ctx, "/api/v1/configmaps")
	if err != nil {
		return err
	}
	if items, _ := all["items"].([]any); len(items) != 0 {
		return fmt.Errorf("the cluster-wide list still holds %d configmap(s) after the only namespace holding any was deleted: the objects are still in the store, they are only unreachable by name", len(items))
	}

	// And the namespace itself is gone, so a write into it is a 404 again.
	res, body, err = srv.send(ctx, http.MethodGet, "/api/v1/namespaces/team-a", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET /api/v1/namespaces/team-a after deleting it", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return err
	}
	return nil
}

// server is the learner's program, running and reachable.
type server struct {
	p   *runner.Process
	url string
}

// stageFieldSelector checks a list can be narrowed on the server, on the
// fields the server is willing to be asked about.
//
// A selector is not a convenience: it is the difference between a kubelet
// watching the pods assigned to one node and every pod in the cluster arriving
// at every node. The rule worth taking from this stage is that a server which
// cannot answer a selector says so, because a client whose filter was silently
// dropped acts on the whole collection.
func stageFieldSelector(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	for _, name := range []string{"alpha", "beta"} {
		if _, err := srv.create(ctx, "/api/v1/namespaces/default/configmaps", map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": name},
			"data":       map[string]any{"colour": "blue"},
		}); err != nil {
			return err
		}
	}
	if _, err := srv.create(ctx, "/api/v1/namespaces/kube-public/configmaps", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "alpha"},
	}); err != nil {
		return err
	}

	// The selector kubectl sends for `get cm alpha` once it is listing rather
	// than getting: one term, the plain = operator.
	for _, selector := range []string{"metadata.name=alpha", "metadata.name==alpha"} {
		names, list, err := srv.names(ctx, "/api/v1/namespaces/default/configmaps?fieldSelector="+url.QueryEscape(selector))
		if err != nil {
			return err
		}
		if strings.Join(names, ",") != "alpha" {
			return fmt.Errorf("listing default with fieldSelector=%s answered %v, and only alpha matches: default holds alpha and beta\nthe body was:\n%v",
				selector, names, list)
		}
		// Narrowing a list does not change what the answer is. kubectl decodes
		// the reply the same way whether or not it asked for a subset.
		if list["kind"] != "ConfigMapList" || metaField(list, "resourceVersion") == "" {
			return fmt.Errorf("listing with fieldSelector=%s answered kind %v with resourceVersion %q, and a filtered list is still a ConfigMapList carrying the list's own resourceVersion: the selector narrows the items, not the envelope",
				selector, list["kind"], metaField(list, "resourceVersion"))
		}
	}

	// != is the other half of the operator, and a server that reads every
	// operator as equality answers this one exactly backwards.
	names, _, err := srv.names(ctx, "/api/v1/namespaces/default/configmaps?fieldSelector="+url.QueryEscape("metadata.name!=alpha"))
	if err != nil {
		return err
	}
	if strings.Join(names, ",") != "beta" {
		return fmt.Errorf("listing default with fieldSelector=metadata.name!=alpha answered %v, and everything but alpha is beta", names)
	}

	// metadata.namespace on the cluster-wide URL, which is how a client asks
	// one endpoint for one namespace's objects — and the only reason the field
	// is selectable at all.
	names, _, err = srv.names(ctx, "/api/v1/configmaps?fieldSelector="+url.QueryEscape("metadata.namespace=kube-public"))
	if err != nil {
		return err
	}
	if strings.Join(names, ",") != "alpha" {
		return fmt.Errorf("listing /api/v1/configmaps with fieldSelector=metadata.namespace=kube-public answered %v, and kube-public holds one configmap named alpha: the other alpha is in default and is a different object",
			names)
	}

	// Two terms, and the comma between them is an AND. There is no OR in this
	// syntax at all, which is why a client wanting a union sends two requests.
	names, _, err = srv.names(ctx, "/api/v1/configmaps?fieldSelector="+url.QueryEscape("metadata.namespace=default,metadata.name=beta"))
	if err != nil {
		return err
	}
	if strings.Join(names, ",") != "beta" {
		return fmt.Errorf("listing /api/v1/configmaps with fieldSelector=metadata.namespace=default,metadata.name=beta answered %v, and one object is in default and named beta: the comma between two terms is an AND, not an OR",
			names)
	}

	// A selector nothing matches is an empty list, not a 404: the collection
	// is there, and the answer is that none of it was selected.
	empty, err := srv.getJSON(ctx, "/api/v1/namespaces/default/configmaps?fieldSelector="+url.QueryEscape("metadata.name=nothing"))
	if err != nil {
		return fmt.Errorf("%w\n\na selector that matches nothing is a 200 with an empty list: the resource exists, and the filter is the client's question rather than the server's", err)
	}
	if items, ok := empty["items"].([]any); !ok || len(items) != 0 {
		return fmt.Errorf("listing with a selector nothing matches answered items %v rather than an empty array", empty["items"])
	}

	// Cluster-scoped resources take a selector too, on the one field they have.
	names, _, err = srv.names(ctx, "/api/v1/namespaces?fieldSelector="+url.QueryEscape("metadata.name=default"))
	if err != nil {
		return err
	}
	if strings.Join(names, ",") != "default" {
		return fmt.Errorf("listing /api/v1/namespaces with fieldSelector=metadata.name=default answered %v, and one namespace is named default: every resource is selectable on metadata.name, namespaces included",
			names)
	}

	// The field that is not indexed. This is the stage's real assertion: the
	// server refuses rather than ignoring, because a filter that was dropped
	// on the floor sends the whole collection to a client that asked for one.
	for _, selector := range []string{"data.colour=blue", "metadata.labels.team=a"} {
		path := "/api/v1/namespaces/default/configmaps?fieldSelector=" + url.QueryEscape(selector)
		res, body, err := srv.send(ctx, http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		if err := wantStatus("listing with fieldSelector="+selector, res, body, http.StatusBadRequest, "BadRequest"); err != nil {
			return fmt.Errorf("%w\n\na field selector is answered from what the server indexes, so a field it does not index is refused: metadata.name and metadata.namespace are the two every resource supports, and the real server declares the rest per resource — status.phase for pods, spec.nodeName for the one the kubelet watches with",
				err)
		}
	}

	// A term with no operator in it at all is not a selector.
	res, body, err := srv.send(ctx, http.MethodGet, "/api/v1/namespaces/default/configmaps?fieldSelector=metadata.name", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("listing with fieldSelector=metadata.name, which has no operator", res, body, http.StatusBadRequest, "BadRequest"); err != nil {
		return fmt.Errorf("%w\n\nfield selectors have no existence form: every term is a field, an operator and a value", err)
	}
	return nil
}

// stageLabelSelector checks the selector the rest of Kubernetes is built out
// of: labels the client wrote, matched by a syntax richer than the fields.
//
// A Service finds its pods this way, a Deployment owns its ReplicaSets this
// way, and neither of them stores a list of names anywhere. The two negative
// forms are where servers get this wrong — != and notin match an object that
// has no such label at all, and a server that skips those objects answers
// "everything not in production" with the wrong set.
func stageLabelSelector(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	labelled := map[string]map[string]any{
		"web-1": {"app": "web", "tier": "frontend"},
		"web-2": {"app": "web", "tier": "backend"},
		"db-1":  {"app": "db"},
		"plain": nil,
	}
	for _, name := range []string{"db-1", "plain", "web-1", "web-2"} {
		meta := map[string]any{"name": name}
		if labels := labelled[name]; labels != nil {
			meta["labels"] = labels
		}
		if _, err := srv.create(ctx, "/api/v1/namespaces/default/configmaps", map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   meta,
		}); err != nil {
			return err
		}
	}

	const base = "/api/v1/namespaces/default/configmaps?labelSelector="
	for _, c := range []struct {
		selector string
		want     string
		why      string
	}{
		{"app=web", "web-1,web-2", "two objects carry app=web; db-1 carries app=db and plain has no labels at all"},
		{"app==web", "web-1,web-2", "== is the same operator as =, and a server that knows only one of them answers half the clients"},
		{"app=web,tier=frontend", "web-1", "the comma is an AND: both terms have to hold on the same object"},
		{"app!=web", "db-1,plain", "!= matches an object that has no such label at all — plain has none, and it is not app=web"},
		{"app", "db-1,web-1,web-2", "a bare key is an existence test: every object that has the label, whatever its value"},
		{"!app", "plain", "!key is the absence test, and plain is the only object without an app label"},
		{"app in (web,db)", "db-1,web-1,web-2", "set membership: the commas inside the parentheses belong to the set, not to the AND between terms"},
		{"tier notin (frontend)", "db-1,plain,web-2", "notin matches an object with no tier label too: db-1 and plain have none, and neither of them is in frontend"},
		{"app in (web),tier=backend", "web-2", "a set term and an equality term, ANDed — which is what makes splitting on every comma wrong"},
	} {
		names, _, err := srv.names(ctx, base+url.QueryEscape(c.selector))
		if err != nil {
			return err
		}
		if strings.Join(names, ",") != c.want {
			return fmt.Errorf("listing default with labelSelector=%s answered %v, and the answer is %s: %s",
				c.selector, names, c.want, c.why)
		}
	}

	// Both selectors on one request, and an object has to pass both. They
	// narrow the same list rather than one overriding the other.
	names, _, err := srv.names(ctx, base+url.QueryEscape("app=web")+"&fieldSelector="+url.QueryEscape("metadata.name=web-2"))
	if err != nil {
		return err
	}
	if strings.Join(names, ",") != "web-2" {
		return fmt.Errorf("listing with labelSelector=app=web and fieldSelector=metadata.name=web-2 answered %v, and one object is both: a request carrying both selectors is narrowed by both",
			names)
	}

	// Labels are the client's, so any key can be selected on — there is no
	// list of allowed ones the way there is for fields.
	names, _, err = srv.names(ctx, base+url.QueryEscape("tier=frontend"))
	if err != nil {
		return fmt.Errorf("%w\n\nunlike a field selector, a label selector cannot be refused for the key it names: labels are written by whoever created the object, so the server has no set of them to check against", err)
	}
	if strings.Join(names, ",") != "web-1" {
		return fmt.Errorf("listing with labelSelector=tier=frontend answered %v rather than web-1", names)
	}

	// A selector nothing matches is an empty list, as with any other filter.
	empty, err := srv.getJSON(ctx, base+url.QueryEscape("app=nothing"))
	if err != nil {
		return err
	}
	if items, ok := empty["items"].([]any); !ok || len(items) != 0 {
		return fmt.Errorf("listing with a label selector nothing matches answered items %v rather than an empty array", empty["items"])
	}

	// Nonsense is refused rather than read as something else. A trailing comma
	// leaves an empty term, and an empty term is not "match everything".
	for _, selector := range []string{"app=web,", "!", "app inside (web)"} {
		path := base + url.QueryEscape(selector)
		res, body, err := srv.send(ctx, http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		if err := wantStatus("listing with labelSelector="+selector, res, body, http.StatusBadRequest, "BadRequest"); err != nil {
			return fmt.Errorf("%w\n\na selector that cannot be parsed is a 400: a server that treats it as empty hands back the whole collection to a client that asked for part of it", err)
		}
	}
	return nil
}

// stagePagination checks a collection can be read a page at a time, and that
// the pages join up into the collection exactly once.
//
// The failure this prevents is not slowness: it is a server holding every
// object of a large resource in memory, serialising it, and sending it — which
// is how an apiserver dies when somebody runs `kubectl get pods -A` on a big
// cluster. kubectl chunks at 500 for exactly this reason.
func stagePagination(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// Five objects, three of them labelled: enough for three pages of two, and
	// enough to tell selecting-then-paging from paging-then-selecting.
	for _, name := range []string{"c", "a", "e", "b", "d"} {
		meta := map[string]any{"name": name}
		if name == "a" || name == "c" || name == "e" {
			meta["labels"] = map[string]any{"app": "web"}
		}
		if _, err := srv.create(ctx, "/api/v1/namespaces/default/configmaps", map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   meta,
		}); err != nil {
			return err
		}
	}

	const path = "/api/v1/namespaces/default/configmaps"

	// Page one. The answer carries the cursor to the next page, and the count
	// of what did not fit.
	names, list, err := srv.names(ctx, path+"?limit=2")
	if err != nil {
		return err
	}
	if strings.Join(names, ",") != "a,b" {
		return fmt.Errorf("GET %s?limit=2 answered %v, and the first two of a,b,c,d,e are a,b: a page is the front of the same order an unpaged list comes back in", path, names)
	}
	cursor := metaField(list, "continue")
	if cursor == "" {
		return fmt.Errorf("GET %s?limit=2 has no metadata.continue, and three of the five objects did not fit in it: the cursor is the only way a client can ask for the rest, and a page without one says the collection ended here",
			path)
	}
	if remaining, ok := list["metadata"].(map[string]any)["remainingItemCount"]; !ok {
		return fmt.Errorf("GET %s?limit=2 has no metadata.remainingItemCount: it is what kubectl prints as the number still to come", path)
	} else if remaining != float64(3) {
		return fmt.Errorf("GET %s?limit=2 says remainingItemCount %v, and three of the five objects are still to come", path, remaining)
	}
	first := metaField(list, "resourceVersion")
	if first == "" {
		return fmt.Errorf("GET %s?limit=2 has no metadata.resourceVersion on the list itself", path)
	}

	// A write somewhere else in the cluster, between two pages of this list.
	// It moves the store's counter and not this collection, which is what
	// makes the pages' own resourceVersion worth checking: it comes from the
	// cursor, not from wherever the store has got to by the time page two is
	// asked for.
	if _, err := srv.create(ctx, "/api/v1/namespaces/kube-public/configmaps", map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "unrelated"},
	}); err != nil {
		return err
	}

	// The rest of the collection, one page at a time, ending when the server
	// stops handing back a cursor.
	got := names
	for page := 2; cursor != ""; page++ {
		if page > 5 {
			return fmt.Errorf("GET %s?limit=2 was still handing back a continue token after five pages of a five-object collection: the last page carries no cursor, and a client that is always given one pages for ever",
				path)
		}
		names, list, err = srv.names(ctx, path+"?limit=2&continue="+url.QueryEscape(cursor))
		if err != nil {
			return fmt.Errorf("%w\n\nthe continue token from the previous page is sent back exactly as it was received: it is the server's own cursor, and nothing but this server reads it", err)
		}
		// Every page of one list describes the same point in the store's
		// history. A client paging through a collection is reading one answer
		// in instalments, not several answers in a row.
		if rv := metaField(list, "resourceVersion"); rv != first {
			return fmt.Errorf("page %d of %s?limit=2 has resourceVersion %q where the first page had %q: the pages of one list are one answer, taken at one point in the store's history — the cursor carries that version so the later pages can report it",
				page, path, rv, first)
		}
		got = append(got, names...)
		cursor = metaField(list, "continue")
	}
	if strings.Join(got, ",") != "a,b,c,d,e" {
		return fmt.Errorf("paging through %s two at a time produced %v, and the collection is a,b,c,d,e: every object appears once, in order, and the cursor starts the next page after the last one sent — not at the item it names",
			path, got)
	}

	// A limit bigger than what is there is not a page: there is no more, so
	// there is no cursor.
	_, list, err = srv.names(ctx, path+"?limit=50")
	if err != nil {
		return err
	}
	if metaField(list, "continue") != "" {
		return fmt.Errorf("GET %s?limit=50 answered all five objects and still carried a continue token: a cursor means there is more, and a client that follows this one asks for a sixth object that was never there",
			path)
	}

	// Selecting happens first, paging second. A limit is how much of the
	// answer to send, not how much of the store to look at.
	names, list, err = srv.names(ctx, path+"?limit=2&labelSelector="+url.QueryEscape("app=web"))
	if err != nil {
		return err
	}
	if strings.Join(names, ",") != "a,c" {
		return fmt.Errorf("GET %s?limit=2&labelSelector=app=web answered %v, and the labelled objects are a, c and e: the selector narrows the collection and the limit cuts what is left, in that order — the other way round pages a client through answers that are mostly empty",
			path, names)
	}
	names, _, err = srv.names(ctx, path+"?limit=2&labelSelector="+url.QueryEscape("app=web")+"&continue="+url.QueryEscape(metaField(list, "continue")))
	if err != nil {
		return err
	}
	if strings.Join(names, ",") != "e" {
		return fmt.Errorf("the second page of %s?limit=2&labelSelector=app=web answered %v, and the third labelled object is e: the selector is sent again with the cursor, and both still apply",
			path, names)
	}

	// A limit that is not a number is refused rather than ignored: a client
	// that asked for a page and was sent a cluster is the failure this whole
	// stage exists to prevent.
	for _, limit := range []string{"abc", "-1"} {
		res, body, err := srv.send(ctx, http.MethodGet, path+"?limit="+url.QueryEscape(limit), nil)
		if err != nil {
			return err
		}
		if err := wantStatus("GET "+path+"?limit="+limit, res, body, http.StatusBadRequest, "BadRequest"); err != nil {
			return err
		}
	}

	// A cursor this server did not make points nowhere. Reading it as "start
	// again" hands a paging client the first page for ever.
	res, body, err := srv.send(ctx, http.MethodGet, path+"?limit=2&continue=not-a-real-cursor", nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+path+"?limit=2&continue=not-a-real-cursor", res, body, http.StatusBadRequest, "BadRequest"); err != nil {
		return fmt.Errorf("%w\n\nthe continue token is opaque to the client but not to the server: one it cannot read is a 400, and the real server answers 410 Gone with reason Expired for one it can read but whose place in etcd's history has since been compacted away",
			err)
	}
	return nil
}

// serve starts the program on a free port of the harness's choosing, with any
// further flags a stage needs, and waits for it to answer. It returns the
// server and the cleanup that stops it.
func serve(ctx context.Context, bin string, args ...string) (*server, func(), error) {
	addr, err := freeAddr()
	if err != nil {
		return nil, nil, err
	}
	p, err := runner.Start(ctx, bin, nil, append([]string{"-addr", addr}, args...)...)
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

// stageWatch checks the endpoint the rest of Kubernetes is built on: one
// request that stays open and reports every change as it happens.
//
// Every controller, the scheduler, the kubelet and kube-proxy are a cache
// filled from one of these and kept in step by it. Nothing polls, which is the
// only reason a cluster of any size works — and it is why the two things
// graded hardest here are that the events are written as they happen rather
// than when a buffer fills, and that an open watch does not stop the server
// answering anybody else.
func stageWatch(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const path = "/api/v1/namespaces/default/configmaps"
	configmap := func(name string, labels map[string]any) map[string]any {
		meta := map[string]any{"name": name}
		if labels != nil {
			meta["labels"] = labels
		}
		return map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta, "data": map[string]any{"colour": "blue"}}
	}

	// Stored before anyone is watching, so it can only reach the watch as the
	// state it starts from.
	if _, err := srv.create(ctx, path, configmap("alpha", nil)); err != nil {
		return err
	}

	w, err := srv.watch(ctx, path+"?watch=true")
	if err != nil {
		return fmt.Errorf("%w\n\na watch is the list URL with ?watch=true on it: same resource, same namespace, a different shape of answer", err)
	}
	defer w.stop()

	// A watch that was told nothing about what the client already has starts
	// by telling it everything: the state when the watch opened, as ADDED.
	kind, obj, err := w.next(ctx, "the state the watch starts from")
	if err != nil {
		return err
	}
	if kind != "ADDED" || metaField(obj, "name") != "alpha" {
		return fmt.Errorf("the first event on a watch with no resourceVersion was %s %s, and it is ADDED alpha: a client that said nothing about what it has holds nothing, so everything already stored is new to it",
			kind, metaField(obj, "name"))
	}
	// Whole objects, not names: a client builds its entire cache out of these
	// and never gets an object individually.
	for _, field := range []string{"uid", "resourceVersion"} {
		if metaField(obj, field) == "" {
			return fmt.Errorf("the object in the ADDED event has no metadata.%s: the object in an event is the stored object, complete", field)
		}
	}
	firstVersion := metaField(obj, "resourceVersion")

	// The three kinds of change, in the order they were made.
	if _, err := srv.create(ctx, path, configmap("beta", nil)); err != nil {
		return err
	}
	if err := w.want(ctx, "ADDED", "beta"); err != nil {
		return err
	}

	res, body, err := srv.send(ctx, http.MethodPut, path+"/alpha", configmap("alpha", nil))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s/alpha answered %d rather than 200\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
	}
	kind, obj, err = w.next(ctx, "MODIFIED alpha")
	if err != nil {
		return err
	}
	if kind != "MODIFIED" || metaField(obj, "name") != "alpha" {
		return fmt.Errorf("an update sent %s %s, and a write to an object that was already there is MODIFIED: a client that is told ADDED twice for one object has no way to know whether it missed a delete",
			kind, metaField(obj, "name"))
	}
	if metaField(obj, "resourceVersion") == firstVersion {
		return fmt.Errorf("the MODIFIED event carries resourceVersion %q, the same as the ADDED event before it: the object in an event is the object as it now is, and its version is the one that write got",
			firstVersion)
	}

	if res, body, err = srv.send(ctx, http.MethodDelete, path+"/beta", nil); err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE %s/beta answered %d rather than 200\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
	}
	kind, obj, err = w.next(ctx, "DELETED beta")
	if err != nil {
		return err
	}
	if kind != "DELETED" || metaField(obj, "name") != "beta" {
		return fmt.Errorf("a delete sent %s %s, and it is DELETED beta carrying the object as it last was: a client is told what went, not that something went",
			kind, metaField(obj, "name"))
	}

	// A write somewhere else in the cluster. A watch is scoped like the list
	// it was opened on, so this one must not see it — and the write after it,
	// in the watched namespace, is what proves the stream is still live rather
	// than simply slow.
	if _, err := srv.create(ctx, "/api/v1/namespaces/kube-public/configmaps", configmap("elsewhere", nil)); err != nil {
		return err
	}
	if _, err := srv.create(ctx, path, configmap("gamma", nil)); err != nil {
		return err
	}
	kind, obj, err = w.next(ctx, "ADDED gamma")
	if err != nil {
		return err
	}
	if metaField(obj, "name") == "elsewhere" {
		return fmt.Errorf("a watch on %s was sent the object elsewhere, which was created in kube-public: a watch is scoped exactly like the list it was opened on, and a client subscribed to one namespace that receives the cluster is the failure this endpoint exists to avoid",
			path)
	}
	if kind != "ADDED" || metaField(obj, "name") != "gamma" {
		return fmt.Errorf("the next event was %s %s rather than ADDED gamma", kind, metaField(obj, "name"))
	}

	// The server is still a server. A watch that is holding a lock the writes
	// need, or a process that serves one request at a time, fails here.
	if _, _, err := srv.names(ctx, path); err != nil {
		return fmt.Errorf("%w\n\nthis list was made while a watch was open on the same collection: an open watch must not stop the server answering anything else, and a lock held for the life of a stream stops everything", err)
	}

	// Selectors narrow a watch exactly as they narrow a list. This is what
	// makes the kubelet's watch proportional to its node rather than to the
	// cluster.
	//
	// Nothing matches this selector yet, so the watch opens with nothing to
	// send — and it still has to open. This is where a server that leaves its
	// header in Go's buffer until the first event is caught: every client of
	// it blocks on a response that has not started.
	selected, err := srv.watch(ctx, path+"?watch=true&labelSelector="+url.QueryEscape("app=web"))
	if err != nil {
		return err
	}
	defer selected.stop()
	if _, err := srv.create(ctx, path, configmap("unlabelled", nil)); err != nil {
		return err
	}
	if _, err := srv.create(ctx, path, configmap("labelled", map[string]any{"app": "web"})); err != nil {
		return err
	}
	kind, obj, err = selected.next(ctx, "ADDED labelled")
	if err != nil {
		return err
	}
	if metaField(obj, "name") != "labelled" {
		return fmt.Errorf("a watch with labelSelector=app=web was sent %s %s: the selectors on a watch are the selectors on a list, applied to every event before it is written",
			kind, metaField(obj, "name"))
	}
	return nil
}

// stageWatchFromRV checks the half of a watch that makes a cache possible: a
// client saying what it already has, and being told only what changed since.
//
// List, then watch from that list's own resourceVersion. Nothing repeated,
// nothing missed, and no window in between — this pair is every informer in
// Kubernetes, and the list carried a version of its own from stage 5 onwards
// for exactly this moment.
func stageWatchFromRV(ctx context.Context, _ *kube.Env, bin string) error {
	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the store: %w", err)
	}
	defer os.RemoveAll(dir)

	const path = "/api/v1/namespaces/default/configmaps"
	configmap := func(name string) map[string]any {
		return map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": name},
			"data":       map[string]any{"colour": "blue"},
		}
	}

	var oldVersion string
	err = func() error {
		srv, cleanup, err := serve(ctx, bin, "-data", dir)
		if err != nil {
			return err
		}
		defer cleanup()

		for _, name := range []string{"alpha", "beta"} {
			if _, err := srv.create(ctx, path, configmap(name)); err != nil {
				return err
			}
		}
		// The list a client builds its cache from, and the version that list
		// describes. Everything after this point is what a watch resuming
		// from it has to be told.
		_, list, err := srv.names(ctx, path)
		if err != nil {
			return err
		}
		version := metaField(list, "resourceVersion")
		oldVersion = version

		if _, err := srv.create(ctx, path, configmap("gamma")); err != nil {
			return err
		}
		if res, body, err := srv.send(ctx, http.MethodPut, path+"/alpha", configmap("alpha")); err != nil {
			return err
		} else if res.StatusCode != http.StatusOK {
			return fmt.Errorf("PUT %s/alpha answered %d rather than 200\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
		}
		if res, body, err := srv.send(ctx, http.MethodDelete, path+"/beta", nil); err != nil {
			return err
		} else if res.StatusCode != http.StatusOK {
			return fmt.Errorf("DELETE %s/beta answered %d rather than 200\nthe body was:\n%s", path, res.StatusCode, tail(string(body)))
		}

		// Three changes happened after that list. A watch resuming from it
		// gets those three, in order, and nothing else: alpha and beta were
		// in the list, so re-sending them as ADDED would have the client
		// treat objects it is already caching as new.
		w, err := srv.watch(ctx, path+"?watch=true&resourceVersion="+url.QueryEscape(version))
		if err != nil {
			return fmt.Errorf("%w\n\nthe resourceVersion on a watch is the one the list answered with: it is what the client has, and the watch is everything after it", err)
		}
		defer w.stop()
		for _, want := range []struct{ kind, name string }{
			{"ADDED", "gamma"}, {"MODIFIED", "alpha"}, {"DELETED", "beta"},
		} {
			kind, obj, err := w.next(ctx, want.kind+" "+want.name)
			if err != nil {
				return fmt.Errorf("%w\n\nthree changes were made after the list this watch resumed from: gamma created, alpha updated, beta deleted", err)
			}
			if kind != want.kind || metaField(obj, "name") != want.name {
				return fmt.Errorf("a watch resumed from the list's resourceVersion sent %s %s where %s %s was expected: a resuming watch replays the changes since that version, in the order they were made, and sends no synthetic ADDED for what the client already had",
					kind, metaField(obj, "name"), want.kind, want.name)
			}
		}

		// Caught up, and then live: the replay runs into the stream with no
		// gap between them.
		if _, err := srv.create(ctx, path, configmap("delta")); err != nil {
			return err
		}
		if err := w.want(ctx, "ADDED", "delta"); err != nil {
			return fmt.Errorf("%w\n\nafter the missed changes have been replayed the same connection carries the live ones: a client that had to reconnect for those would have a window it sees nothing through", err)
		}

		// resourceVersion=0 is not version zero. It means "whatever you have
		// already, and do not make me wait for it", so the current state
		// comes back as ADDED just as it does with no version at all.
		zero, err := srv.watch(ctx, path+"?watch=true&resourceVersion=0")
		if err != nil {
			return err
		}
		defer zero.stop()
		// The state is alpha, delta and gamma — beta was deleted — so those
		// three arrive as ADDED and nothing else does. A server that read the
		// 0 as a version to resume after would replay the history instead,
		// and the client would be told about beta, which no longer exists.
		for _, want := range []string{"alpha", "delta", "gamma"} {
			kind, obj, err := zero.next(ctx, "ADDED "+want)
			if err != nil {
				return fmt.Errorf("%w\n\nresourceVersion=0 on a watch is the one value that is not a version: it says the client has nothing and will take the state as it is, which is how an informer does its cheap first pass", err)
			}
			if kind != "ADDED" || metaField(obj, "name") != want {
				return fmt.Errorf("a watch with resourceVersion=0 sent %s %s where ADDED %s was expected: 0 means \"I have nothing, send me what there is\" — not \"resume after version zero\", which would replay every change ever made including the delete of an object that is gone",
					kind, metaField(obj, "name"), want)
			}
		}

		// A version this server has never reached. It may have come from
		// another apiserver a moment ago, so the answer is "ask again", not
		// "you are mistaken" — the same 504 a read gets in stage 9.
		res, body, err := srv.send(ctx, http.MethodGet, path+"?watch=true&resourceVersion=999999", nil)
		if err != nil {
			return err
		}
		if err := wantStatus("a watch from resourceVersion=999999", res, body, http.StatusGatewayTimeout, "Timeout"); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		return err
	}

	// A restart. The objects were written down; the changes were not, so
	// nothing before this process started can be replayed.
	srv, cleanup, err := serve(ctx, bin, "-data", dir)
	if err != nil {
		return err
	}
	defer cleanup()

	// The version a client held from before the restart is now too old. This
	// is the error every informer in Kubernetes is written to handle — the
	// real server gives it once etcd has compacted the revision away, and a
	// client that gets it throws its cache away, lists, and watches from
	// there. Answering it with the current state instead is the dangerous
	// failure: the client would believe it had missed nothing.
	res, body, err := srv.send(ctx, http.MethodGet, path+"?watch=true&resourceVersion="+url.QueryEscape(oldVersion), nil)
	if err != nil {
		return err
	}
	if err := wantStatus("a watch resumed from a version from before the restart", res, body, http.StatusGone, "Expired"); err != nil {
		return fmt.Errorf("%w\n\nthe store came back from disk but the history of changes did not, so this version can no longer be caught up from: 410 Gone with reason Expired is how a client is told to list again, and it is the one answer that cannot be faked by sending the current state",
			err)
	}

	// The server itself is fine, and a watch that asks for nothing in
	// particular still works after the restart.
	w, err := srv.watch(ctx, path+"?watch=true")
	if err != nil {
		return err
	}
	defer w.stop()
	if _, _, err := w.next(ctx, "the state the watch starts from"); err != nil {
		return fmt.Errorf("%w\n\na watch with no resourceVersion still starts from the current state after a restart: only resuming from a version older than this process is refused", err)
	}
	return nil
}

// stageBookmarks checks the event that carries no object: a periodic "you have
// seen everything up to here" for a watch where nothing is happening.
//
// Without it, a client watching a quiet resource holds a resourceVersion that
// ages while the cluster moves on around it. When it eventually reconnects,
// that version has fallen off the end of the history and the answer is 410 and
// a full relist — of everything, by every idle watcher, at once. A bookmark is
// a few bytes that stop that.
//
// The interval is this course's own: a server here sends one within five
// seconds of the store moving. The real one is roughly a minute, jittered.
func stageBookmarks(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const path = "/api/v1/namespaces/default/configmaps"
	configmap := func(name string) map[string]any {
		return map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": name},
		}
	}
	if _, err := srv.create(ctx, path, configmap("alpha")); err != nil {
		return err
	}

	w, err := srv.watch(ctx, path+"?watch=true&allowWatchBookmarks=true")
	if err != nil {
		return err
	}
	defer w.stop()
	kind, obj, err := w.next(ctx, "the state the watch starts from")
	if err != nil {
		return err
	}
	if kind != "ADDED" {
		return fmt.Errorf("the first event was %s rather than ADDED: allowing bookmarks does not change how a watch starts", kind)
	}
	seen := metaField(obj, "resourceVersion")

	// Writes this watch will never be told about: a different namespace, so
	// the store's version climbs and this client hears nothing.
	for _, name := range []string{"one", "two", "three"} {
		if _, err := srv.create(ctx, "/api/v1/namespaces/kube-public/configmaps", configmap(name)); err != nil {
			return err
		}
	}

	kind, obj, err = w.next(ctx, "a BOOKMARK")
	if err != nil {
		return fmt.Errorf("%w\n\nthree objects were created elsewhere in the cluster while this watch was idle, and a client that allowed bookmarks is told how far the store has got: a server here sends one within five seconds of the store moving past what this watch has been sent",
			err)
	}
	if kind != "BOOKMARK" {
		return fmt.Errorf("the next event on an idle watch was %s %s, and it should be a BOOKMARK: the writes were in another namespace, so this watch must not be sent them — a watch that leaks objects from outside its scope is the failure stage 14 graded",
			kind, metaField(obj, "name"))
	}
	if name := metaField(obj, "name"); name != "" {
		return fmt.Errorf("the BOOKMARK carried an object named %q, and a bookmark has no object: it is a resourceVersion and nothing else — the promise that everything up to that number has been sent, not a change to anything",
			name)
	}
	version := metaField(obj, "resourceVersion")
	if version == "" {
		return fmt.Errorf("the BOOKMARK has no metadata.resourceVersion, which is the only thing it carries and the only reason it exists: a client stores it and resumes from it after a reconnect")
	}
	// Compared as numbers: "10" is less than "9" as text, and a version is a
	// counter whatever the wire says.
	ahead, err1 := strconv.ParseInt(version, 10, 64)
	behind, err2 := strconv.ParseInt(seen, 10, 64)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("a resourceVersion of %q or %q is not a number: it is a string on the wire and opaque to clients, but this server's own are the store's counter", version, seen)
	}
	if ahead <= behind {
		return fmt.Errorf("the BOOKMARK carries resourceVersion %q where the last event carried %q: a bookmark says how far the store has got, so it is ahead of everything this watch has been sent",
			version, seen)
	}
	if obj["kind"] != "ConfigMap" {
		return fmt.Errorf("the BOOKMARK's object has kind %v, and it is the kind being watched — ConfigMap here: a client decodes it like any other event", obj["kind"])
	}

	// Still a watch. The bookmarks run alongside the events rather than
	// instead of them.
	if _, err := srv.create(ctx, path, configmap("beta")); err != nil {
		return err
	}
	for {
		kind, obj, err = w.next(ctx, "ADDED beta")
		if err != nil {
			return err
		}
		if kind == "BOOKMARK" {
			continue
		}
		if kind != "ADDED" || metaField(obj, "name") != "beta" {
			return fmt.Errorf("after a bookmark the watch sent %s %s rather than ADDED beta: bookmarks are sent between events, not instead of them", kind, metaField(obj, "name"))
		}
		break
	}

	// A client that did not ask for bookmarks must never be sent one. Its
	// library does not know the type, and an event it cannot decode is worse
	// than no event at all — which is why this is opt-in rather than on.
	plain, err := srv.watch(ctx, path+"?watch=true")
	if err != nil {
		return err
	}
	defer plain.stop()
	for _, want := range []string{"alpha", "beta"} {
		if err := plain.want(ctx, "ADDED", want); err != nil {
			return err
		}
	}
	for _, name := range []string{"four", "five"} {
		if _, err := srv.create(ctx, "/api/v1/namespaces/kube-public/configmaps", configmap(name)); err != nil {
			return err
		}
	}
	// Long enough that a server sending bookmarks to everybody would have
	// sent this watch one by now. The next thing it hears is the next write
	// in its own namespace.
	time.Sleep(6 * time.Second)
	if _, err := srv.create(ctx, path, configmap("gamma")); err != nil {
		return err
	}
	kind, obj, err = plain.next(ctx, "ADDED gamma")
	if err != nil {
		return err
	}
	if kind == "BOOKMARK" {
		return fmt.Errorf("a watch that did not send allowWatchBookmarks=true was sent a BOOKMARK: bookmarks are opt-in, and a client whose decoder does not know the type sees an event it cannot read")
	}
	if kind != "ADDED" || metaField(obj, "name") != "gamma" {
		return fmt.Errorf("the next event on the plain watch was %s %s rather than ADDED gamma", kind, metaField(obj, "name"))
	}
	return nil
}

// watchStream is one open watch, decoded event by event in the background so
// a stage can say "the next thing that happens should be this, within a few
// seconds" rather than blocking for ever on a server that sends nothing.
type watchStream struct {
	events chan map[string]any
	fail   chan error
	cancel context.CancelFunc
	body   io.Closer
	path   string
}

// watch opens a watch and insists it starts: the status and the headers come
// back before the first event, because the client is waiting to learn the
// watch is open rather than queued behind something.
func (s *server) watch(ctx context.Context, path string) (*watchStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url+path, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	// No client timeout: a watch that answered and then went quiet is correct,
	// and the context is what closes this one. The opening of it is bounded
	// separately, below, because a program that never flushes its header
	// leaves this call blocked for ever rather than failing.
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
		res, err = got.res, got.err
	case <-time.After(30 * time.Second):
		cancel()
		return nil, fmt.Errorf("GET %s did not answer within 30s: a watch sends its status and headers as soon as it is open, before any event — Go holds the header back until the response is flushed, so a watch that writes nothing until the first change leaves every client waiting on a response that has not started\nthe program said:\n%s",
			path, tail(s.p.Stdout()))
	}
	if err != nil {
		cancel()
		return nil, fmt.Errorf("GET %s: %w\nthe program said:\n%s", path, err, tail(s.p.Stdout()))
	}
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
		res.Body.Close()
		cancel()
		return nil, fmt.Errorf("GET %s answered %d rather than 200: a watch answers its status straight away, and only then starts sending events\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		res.Body.Close()
		cancel()
		return nil, fmt.Errorf("GET %s answered Content-Type %q, and a watch is a stream of JSON events", path, ct)
	}

	w := &watchStream{events: make(chan map[string]any, 64), fail: make(chan error, 1), cancel: cancel, body: res.Body, path: path}
	go func() {
		defer close(w.events)
		dec := json.NewDecoder(res.Body)
		for {
			var event map[string]any
			if err := dec.Decode(&event); err != nil {
				w.fail <- err
				return
			}
			w.events <- event
		}
	}()
	return w, nil
}

// next waits for one event and reports its type and object. The wait is
// generous because what is being graded is that the event arrives at all, and
// short enough that a watch which never sends is a failure rather than a hang.
func (w *watchStream) next(ctx context.Context, what string) (string, map[string]any, error) {
	select {
	case event, open := <-w.events:
		if !open {
			return "", nil, fmt.Errorf("the watch on %s ended while waiting for %s: %v\n\nan open watch stays open — a server that answers the request and closes it has turned a watch into an expensive get",
				w.path, what, <-w.fail)
		}
		kind, _ := event["type"].(string)
		obj, _ := event["object"].(map[string]any)
		if kind == "" || obj == nil {
			return "", nil, fmt.Errorf("the watch on %s sent %v, and an event is {\"type\": ..., \"object\": ...}: the type is ADDED, MODIFIED, DELETED or BOOKMARK, and the object is the whole object as it now is",
				w.path, event)
		}
		return kind, obj, nil
	case <-time.After(20 * time.Second):
		return "", nil, fmt.Errorf("nothing arrived on the watch on %s within 20s, waiting for %s: an event has to be written and flushed as it happens — one left in a buffer until the buffer fills is an event the client has not been told about",
			w.path, what)
	case <-ctx.Done():
		return "", nil, ctx.Err()
	}
}

// want reads the next event and insists it is the one expected.
func (w *watchStream) want(ctx context.Context, kind, name string) error {
	what := fmt.Sprintf("%s %s", kind, name)
	gotKind, obj, err := w.next(ctx, what)
	if err != nil {
		return err
	}
	if gotKind != kind || metaField(obj, "name") != name {
		return fmt.Errorf("the watch on %s sent %s %s where %s was expected: the events are the writes, in the order the store applied them",
			w.path, gotKind, metaField(obj, "name"), what)
	}
	return nil
}

// stop closes the watch, which is how a client leaves: the request is
// cancelled and the server notices.
func (w *watchStream) stop() {
	w.cancel()
	w.body.Close()
}

// names reads a collection and returns what is in it, in the order the server
// listed it, together with the list itself for anything else a stage asks.
func (s *server) names(ctx context.Context, path string) ([]string, map[string]any, error) {
	list, err := s.getJSON(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	items, ok := list["items"].([]any)
	if !ok {
		return nil, nil, fmt.Errorf("GET %s has no items array: a collection holds its objects under items, beside the list's own metadata\nthe body was:\n%v", path, list)
	}
	names := []string{}
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("GET %s has an item that is not an object: a list holds whole objects, not names", path)
		}
		names = append(names, metaField(obj, "name"))
	}
	return names, list, nil
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
