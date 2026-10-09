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
	"path/filepath"
	"reflect"
	"slices"
	"sort"
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
	register(Stage{Slug: "patch-merge", Run: stagePatchMerge})
	register(Stage{Slug: "patch-json", Run: stagePatchJSON})
	register(Stage{Slug: "patch-strategic", Run: stagePatchStrategic})
	register(Stage{Slug: "apply-ssa", Run: stageApplySSA})
	register(Stage{Slug: "apply-conflict", Run: stageApplyConflict})
	register(Stage{Slug: "subresource-status", Run: stageSubresourceStatus})
	register(Stage{Slug: "subresource-scale", Run: stageSubresourceScale})
	register(Stage{Slug: "openapi", Run: stageOpenAPI})
	register(Stage{Slug: "authn-token", Run: stageAuthnToken})
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
		if err == nil && res.StatusCode == http.StatusUnauthorized {
			cleanup()
			return nil, nil, fmt.Errorf("GET /healthz on %s answered 401: the health endpoints answer whoever asks, token or not — whatever restarts a server has no credentials, and one that cannot say it is alive is restarted for ever", addr)
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

	// A real change: a write that leaves the object as it was is not a write,
	// and a server is right to send nothing for it.
	changed := configmap("alpha", nil)
	changed["data"] = map[string]any{"colour": "green"}
	res, body, err := srv.send(ctx, http.MethodPut, path+"/alpha", changed)
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
		changed := configmap("alpha")
		changed["data"] = map[string]any{"colour": "green"}
		if res, body, err := srv.send(ctx, http.MethodPut, path+"/alpha", changed); err != nil {
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

// stagePatchMerge checks a PATCH that says only what changed, in the
// simplest of the three dialects: a JSON merge patch, RFC 7386.
//
// The two things graded hardest are the ones a hand-rolled merge gets wrong.
// null is how a key is removed, not a value to store; and a patch is applied
// to what is stored at the moment of the write, under the same lock, so two
// clients patching different keys of one object both keep what they wrote.
// That second property is the reason PATCH exists at all — a PUT built from a
// read loses a race, and a patch has nothing stale in it to lose with.
func stagePatchMerge(ctx context.Context, _ *kube.Env, bin string) error {
	// On disk, so every write holds the store for as long as a real one waits
	// on etcd: that is the gap a read taken outside the write falls into.
	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the store: %w", err)
	}
	defer os.RemoveAll(dir)
	srv, cleanup, err := serve(ctx, bin, "-data", dir)
	if err != nil {
		return err
	}
	defer cleanup()

	const (
		path  = "/api/v1/namespaces/default/configmaps"
		merge = "application/merge-patch+json"
	)
	created, err := srv.create(ctx, path, map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":       "settings",
			"labels":     map[string]any{"app": "web"},
			"finalizers": []any{"example.com/one", "example.com/two"},
		},
		"data": map[string]any{"colour": "blue", "size": "large"},
	})
	if err != nil {
		return err
	}

	// patchOK sends one merge patch that has to succeed, and returns the
	// object as the reply and a fresh read both describe it.
	patchOK := func(what string, body any) (map[string]any, error) {
		res, raw, err := srv.patch(ctx, path+"/settings", merge, body)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("PATCH %s/settings (%s) answered %d rather than 200\nthe body was:\n%s", path, what, res.StatusCode, tail(string(raw)))
		}
		if _, err := decode(raw); err != nil {
			return nil, fmt.Errorf("the reply to PATCH %s/settings (%s) is not JSON (%w)\nthe body was:\n%s", path, what, err, tail(string(raw)))
		}
		return srv.getJSON(ctx, path+"/settings")
	}
	dataOf := func(obj map[string]any) map[string]any {
		data, _ := obj["data"].(map[string]any)
		return data
	}

	got, err := patchOK("one key of data", map[string]any{"data": map[string]any{"colour": "green"}})
	if err != nil {
		return err
	}
	if data := dataOf(got); data["colour"] != "green" || data["size"] != "large" {
		return fmt.Errorf("after a merge patch of data.colour alone the object's data is %v: the keys a patch does not mention are kept, which is the difference between a patch and a PUT", got["data"])
	}
	if labels, _ := got["metadata"].(map[string]any)["labels"].(map[string]any); labels["app"] != "web" {
		return fmt.Errorf("after a merge patch that only touched data, metadata.labels is %v: a merge recurses into objects, and a patch that did not mention metadata leaves all of it alone", labels)
	}
	if metaField(got, "resourceVersion") == metaField(created, "resourceVersion") {
		return fmt.Errorf("the object still has resourceVersion %q after a patch: a patch is a write like any other, and moves the store forward", metaField(got, "resourceVersion"))
	}
	if metaField(got, "uid") != metaField(created, "uid") {
		return fmt.Errorf("the patch changed metadata.uid from %q to %q: the object was edited, not replaced by a new one", metaField(created, "uid"), metaField(got, "uid"))
	}

	// null is not a value in a merge patch, it is a delete. Storing it leaves
	// a key every reader has to know to skip.
	got, err = patchOK(`data.size set to null`, map[string]any{"data": map[string]any{"size": nil}})
	if err != nil {
		return err
	}
	if size, there := dataOf(got)["size"]; there {
		return fmt.Errorf("after a merge patch setting data.size to null the object still has data.size = %v: in a merge patch null removes the key, and it is the only way this dialect has of removing anything", size)
	}

	// A list is a value, not something merged into. This is the rule that
	// makes a merge patch unfit for lists other people also write to — and it
	// is why the strategic dialect exists.
	got, err = patchOK("metadata.finalizers", map[string]any{"metadata": map[string]any{"finalizers": []any{"example.com/three"}}})
	if err != nil {
		return err
	}
	finalizers, _ := got["metadata"].(map[string]any)["finalizers"].([]any)
	if len(finalizers) != 1 || finalizers[0] != "example.com/three" {
		return fmt.Errorf("after a merge patch of metadata.finalizers to [example.com/three] the list is %v: a merge patch replaces a list whole — it has no way to say which element is which, so it does not try", finalizers)
	}

	// A resourceVersion in a patch is a precondition, exactly as it is in a
	// PUT: the client is saying it built this change on that version.
	res, raw, err := srv.patch(ctx, path+"/settings", merge, map[string]any{
		"metadata": map[string]any{"resourceVersion": metaField(created, "resourceVersion")},
		"data":     map[string]any{"colour": "red"},
	})
	if err != nil {
		return err
	}
	if err := wantStatus("PATCH "+path+"/settings carrying the resourceVersion from before three writes", res, raw, http.StatusConflict, "Conflict"); err != nil {
		return fmt.Errorf("%w\n\na patch with no resourceVersion is applied to whatever is there; one that names a version is a client asking for exactly that version to be the one it changes", err)
	}
	if got, err = srv.getJSON(ctx, path+"/settings"); err != nil {
		return err
	}
	if dataOf(got)["colour"] != "green" {
		return fmt.Errorf("the refused patch changed the object anyway: data.colour is now %v", dataOf(got)["colour"])
	}

	// Many clients at once, each adding its own key. A server that reads the
	// object, merges outside the lock and writes back loses some of them, or
	// answers 409 to a client that sent no precondition at all.
	const writers = 50
	failures := make(chan error, writers)
	for i := range writers {
		go func() {
			key := "writer-" + strconv.Itoa(i)
			res, raw, err := srv.patch(ctx, path+"/settings", merge, map[string]any{"data": map[string]any{key: "here"}})
			switch {
			case err != nil:
				failures <- err
			case res.StatusCode != http.StatusOK:
				failures <- fmt.Errorf("one of %d concurrent patches, each to its own key, answered %d: a patch with no resourceVersion has no precondition to fail, so the server has to apply it to whatever is there when it gets the lock rather than to what it read before\nthe body was:\n%s", writers, res.StatusCode, tail(string(raw)))
			default:
				failures <- nil
			}
		}()
	}
	for range writers {
		if err := <-failures; err != nil {
			return err
		}
	}
	if got, err = srv.getJSON(ctx, path+"/settings"); err != nil {
		return err
	}
	for i := range writers {
		if key := "writer-" + strconv.Itoa(i); dataOf(got)[key] != "here" {
			return fmt.Errorf("%d concurrent patches each added one key and all answered 200, but data.%s is missing afterwards: a merge computed from a read taken before the lock is a lost update — the patch has to be applied to the stored object inside the write", writers, key)
		}
	}

	// A patch is only as meaningful as its dialect, and the body alone does
	// not say which one it is in. application/json is not a patch type.
	res, raw, err = srv.patch(ctx, path+"/settings", "application/json", map[string]any{"data": map[string]any{"colour": "red"}})
	if err != nil {
		return err
	}
	if err := wantStatus("PATCH "+path+"/settings with Content-Type application/json", res, raw, http.StatusUnsupportedMediaType, "UnsupportedMediaType"); err != nil {
		return fmt.Errorf("%w\n\nthe Content-Type is what says how to read a patch — the same body means different things as a merge patch and as a strategic one — so a type the server does not handle is refused rather than guessed at", err)
	}

	res, raw, err = srv.patch(ctx, path+"/absent", merge, map[string]any{"data": map[string]any{"colour": "red"}})
	if err != nil {
		return err
	}
	if err := wantStatus("PATCH "+path+"/absent", res, raw, http.StatusNotFound, "NotFound"); err != nil {
		return fmt.Errorf("%w\n\na patch describes a change to something, and there is nothing here to change", err)
	}

	// kubectl patch and kubectl apply both look for the verb before they send
	// anything.
	list, err := srv.getJSON(ctx, "/api/v1")
	if err != nil {
		return err
	}
	resources, _ := list["resources"].([]any)
	for _, r := range resources {
		if m, ok := r.(map[string]any); ok && m["name"] == "configmaps" {
			if verbs, _ := m["verbs"].([]any); !contains(verbs, "patch") {
				return fmt.Errorf("the configmaps entry in GET /api/v1 does not offer the verb \"patch\" (it offers %v): kubectl reads discovery before it sends a PATCH, and refuses one the server does not list", m["verbs"])
			}
		}
	}
	return nil
}

// stagePatchJSON checks the second patch dialect: a JSON patch, RFC 6902, a
// list of operations addressed by JSON pointer.
//
// It is the only dialect that can name one element of a list, remove a key
// without a null, or refuse to run unless a value is what the client expects.
// The two properties graded hardest are the pointer escaping — a / in a key is
// ~1, and every label with a domain prefix has one — and that a patch applies
// whole or not at all: an operation that fails undoes the ones before it.
func stagePatchJSON(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const (
		path = "/api/v1/namespaces/default/configmaps"
		jp   = "application/json-patch+json"
	)
	if _, err := srv.create(ctx, path, map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":       "settings",
			"labels":     map[string]any{"app.kubernetes.io/name": "web"},
			"finalizers": []any{"example.com/one", "example.com/two", "example.com/three"},
		},
		"data": map[string]any{"a": "1", "b": "2"},
	}); err != nil {
		return err
	}

	type op = map[string]any
	// patchOK sends one JSON patch that has to succeed, and returns the
	// object as a fresh read describes it.
	patchOK := func(what string, ops ...op) (map[string]any, error) {
		res, raw, err := srv.patch(ctx, path+"/settings", jp, ops)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("PATCH %s/settings with a JSON patch that %s answered %d rather than 200\nthe patch was: %v\nthe body was:\n%s",
				path, what, res.StatusCode, ops, tail(string(raw)))
		}
		return srv.getJSON(ctx, path+"/settings")
	}
	dataOf := func(obj map[string]any) map[string]any {
		data, _ := obj["data"].(map[string]any)
		return data
	}
	metaOf := func(obj map[string]any) map[string]any {
		meta, _ := obj["metadata"].(map[string]any)
		return meta
	}

	got, err := patchOK("tests, replaces, adds and removes keys of data",
		op{"op": "test", "path": "/data/a", "value": "1"},
		op{"op": "replace", "path": "/data/a", "value": "10"},
		op{"op": "add", "path": "/data/c", "value": "3"},
		op{"op": "remove", "path": "/data/b"},
	)
	if err != nil {
		return err
	}
	if data := dataOf(got); len(data) != 2 || data["a"] != "10" || data["c"] != "3" {
		return fmt.Errorf("after test /data/a, replace /data/a with 10, add /data/c and remove /data/b the data is %v, and it should be exactly {a:10 c:3}: the operations run in order, each on the result of the one before", got["data"])
	}

	// A list is addressed by index, and - is the end of it. This is what a
	// merge patch cannot do.
	got, err = patchOK("works on a list by index",
		op{"op": "remove", "path": "/metadata/finalizers/1"},
		op{"op": "add", "path": "/metadata/finalizers/-", "value": "example.com/four"},
		op{"op": "add", "path": "/metadata/finalizers/0", "value": "example.com/zero"},
	)
	if err != nil {
		return err
	}
	want := []any{"example.com/zero", "example.com/one", "example.com/three", "example.com/four"}
	if finalizers, _ := metaOf(got)["finalizers"].([]any); !reflect.DeepEqual(finalizers, want) {
		return fmt.Errorf("after remove /metadata/finalizers/1, add at /- and add at /0 the finalizers are %v, not %v: remove takes the element at that index out, add at an index inserts before it, and - is one past the end", finalizers, want)
	}

	// The key has a / in it, and in a pointer that is written ~1. Read
	// literally, the pointer would walk into a key app.kubernetes.io and then
	// look for name inside it.
	got, err = patchOK("replaces a label whose key contains a /",
		op{"op": "replace", "path": "/metadata/labels/app.kubernetes.io~1name", "value": "api"})
	if err != nil {
		return fmt.Errorf("%w\n\nin a JSON pointer / separates the tokens, so a / inside a key is escaped as ~1 (and a ~ as ~0), and has to be unescaped after the split rather than before", err)
	}
	labels, _ := metaOf(got)["labels"].(map[string]any)
	if len(labels) != 1 || labels["app.kubernetes.io/name"] != "api" {
		return fmt.Errorf("after replace /metadata/labels/app.kubernetes.io~1name the labels are %v: ~1 is a / inside the key, so the label app.kubernetes.io/name should now be api and nothing else should be there", labels)
	}

	got, err = patchOK("copies and moves keys",
		op{"op": "copy", "from": "/data/a", "path": "/data/a-copy"},
		op{"op": "move", "from": "/data/c", "path": "/data/d"},
	)
	if err != nil {
		return err
	}
	if data := dataOf(got); len(data) != 3 || data["a"] != "10" || data["a-copy"] != "10" || data["d"] != "3" {
		return fmt.Errorf("after copy /data/a to /data/a-copy and move /data/c to /data/d the data is %v, and it should be exactly {a:10 a-copy:10 d:3}: copy leaves the source where it was, move takes it away", got["data"])
	}

	// test is how a JSON patch carries a precondition, on any field at all:
	// "only if the resourceVersion is still this" is the common one.
	got, err = patchOK("tests the resourceVersion it was built from",
		op{"op": "test", "path": "/metadata/resourceVersion", "value": metaField(got, "resourceVersion")},
		op{"op": "replace", "path": "/data/a", "value": "11"},
	)
	if err != nil {
		return err
	}
	before := got

	// All or nothing. The replace succeeds on its own; the test after it
	// fails, and the replace has to go with it.
	res, raw, err := srv.patch(ctx, path+"/settings", jp, []op{
		{"op": "replace", "path": "/data/a", "value": "99"},
		{"op": "test", "path": "/data/d", "value": "not what is there"},
	})
	if err != nil {
		return err
	}
	if err := wantStatus("PATCH "+path+"/settings with a JSON patch whose test fails", res, raw, http.StatusUnprocessableEntity, "Invalid"); err != nil {
		return fmt.Errorf("%w\n\na patch that cannot be applied is a 422 — the request was understood, and the object it would produce is the problem", err)
	}
	if got, err = srv.getJSON(ctx, path+"/settings"); err != nil {
		return err
	}
	if dataOf(got)["a"] != "11" || metaField(got, "resourceVersion") != metaField(before, "resourceVersion") {
		return fmt.Errorf("a JSON patch whose second operation failed left data.a as %v and resourceVersion %s (they were 11 and %s): the operations before the failure have to be undone with it — a patch applies whole or not at all",
			dataOf(got)["a"], metaField(got, "resourceVersion"), metaField(before, "resourceVersion"))
	}

	res, raw, err = srv.patch(ctx, path+"/settings", jp, []op{{"op": "remove", "path": "/data/absent"}})
	if err != nil {
		return err
	}
	if err := wantStatus("PATCH "+path+"/settings removing /data/absent", res, raw, http.StatusUnprocessableEntity, "Invalid"); err != nil {
		return fmt.Errorf("%w\n\nremove, replace and test all need the target to exist; only add creates", err)
	}

	// An object where a list of operations should be is a request the server
	// cannot read at all, which is a 400 rather than a 422.
	res, raw, err = srv.patch(ctx, path+"/settings", jp, map[string]any{"data": map[string]any{"a": "12"}})
	if err != nil {
		return err
	}
	if err := wantStatus("PATCH "+path+"/settings with a JSON patch that is an object rather than a list", res, raw, http.StatusBadRequest, "BadRequest"); err != nil {
		return fmt.Errorf("%w\n\nthat body is a merge patch sent under the wrong Content-Type, and applying it as one would be guessing", err)
	}
	return nil
}

// stagePatchStrategic checks the dialect kubectl patch sends by default: the
// strategic merge patch, a merge patch that knows which lists are sets and
// which are keyed by a field.
//
// The point of it is shared lists. Several controllers each own one finalizer
// on an object, several owners each have one ownerReference, and a merge patch
// can only replace the whole list — so every writer erases the others. A
// strategic patch merges into those lists instead, and says what to take out
// with directives, which are instructions to the server and never stored.
func stagePatchStrategic(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const (
		path = "/api/v1/namespaces/default/configmaps"
		smp  = "application/strategic-merge-patch+json"
	)
	if _, err := srv.create(ctx, path, map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":       "settings",
			"finalizers": []any{"example.com/a", "example.com/b"},
			"ownerReferences": []any{
				map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": "parent-one", "uid": "uid-one"},
			},
		},
		"data": map[string]any{"colour": "blue", "size": "large"},
	}); err != nil {
		return err
	}

	patchOK := func(what string, body any) (map[string]any, error) {
		res, raw, err := srv.patch(ctx, path+"/settings", smp, body)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("PATCH %s/settings with a strategic merge patch that %s answered %d rather than 200\nthe patch was: %v\nthe body was:\n%s",
				path, what, res.StatusCode, body, tail(string(raw)))
		}
		return srv.getJSON(ctx, path+"/settings")
	}
	metaOf := func(obj map[string]any) map[string]any {
		meta, _ := obj["metadata"].(map[string]any)
		return meta
	}
	owners := func(obj map[string]any) map[string]map[string]any {
		out := map[string]map[string]any{}
		refs, _ := metaOf(obj)["ownerReferences"].([]any)
		for _, ref := range refs {
			if m, ok := ref.(map[string]any); ok {
				uid, _ := m["uid"].(string)
				out[uid] = m
			}
		}
		return out
	}

	// Where nothing is declared, it is a merge patch: objects merge, null
	// deletes.
	got, err := patchOK("changes data", map[string]any{"data": map[string]any{"colour": "green", "size": nil}})
	if err != nil {
		return err
	}
	if data, _ := got["data"].(map[string]any); len(data) != 1 || data["colour"] != "green" {
		return fmt.Errorf("after a strategic patch of data {colour: green, size: null} the data is %v: for a map a strategic patch is a merge patch — keys merge, null removes", got["data"])
	}

	// finalizers is a set: what the patch lists is added, once, and what it
	// does not list stays. $setElementOrder is what kubectl sends alongside to
	// say what order it wants; whatever a server does with it, it is not a
	// field of the object.
	got, err = patchOK("adds to metadata.finalizers", map[string]any{"metadata": map[string]any{
		"finalizers":                  []any{"example.com/c", "example.com/a"},
		"$setElementOrder/finalizers": []any{"example.com/a", "example.com/b", "example.com/c"},
	}})
	if err != nil {
		return err
	}
	want := []any{"example.com/a", "example.com/b", "example.com/c"}
	if finalizers, _ := metaOf(got)["finalizers"].([]any); !reflect.DeepEqual(finalizers, want) {
		return fmt.Errorf("after a strategic patch listing finalizers [example.com/c example.com/a] onto [example.com/a example.com/b] the list is %v, not %v: metadata.finalizers is merged as a set — new values are added once, and the ones the patch did not mention are some other controller's and stay. That is the whole difference from a merge patch, which would have replaced the list", finalizers, want)
	}

	got, err = patchOK("deletes a finalizer", map[string]any{"metadata": map[string]any{
		"$deleteFromPrimitiveList/finalizers": []any{"example.com/a"},
	}})
	if err != nil {
		return err
	}
	want = []any{"example.com/b", "example.com/c"}
	if finalizers, _ := metaOf(got)["finalizers"].([]any); !reflect.DeepEqual(finalizers, want) {
		return fmt.Errorf("after $deleteFromPrimitiveList/finalizers: [example.com/a] the finalizers are %v, not %v: in a list that merges, leaving a value out keeps it, so removing one needs a directive of its own", finalizers, want)
	}

	// ownerReferences is keyed by uid: an element whose uid is new is added,
	// one whose uid is there is merged into that element.
	got, err = patchOK("adds an ownerReference", map[string]any{"metadata": map[string]any{
		"ownerReferences": []any{map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": "parent-two", "uid": "uid-two"}},
	}})
	if err != nil {
		return err
	}
	if refs := owners(got); len(refs) != 2 || refs["uid-one"] == nil || refs["uid-two"] == nil {
		return fmt.Errorf("after a strategic patch adding an ownerReference with uid uid-two the owners are %v: metadata.ownerReferences is merged by uid, so a new uid is added beside the ones already there", metaOf(got)["ownerReferences"])
	}
	got, err = patchOK("changes one ownerReference by uid", map[string]any{"metadata": map[string]any{
		"ownerReferences": []any{map[string]any{"uid": "uid-one", "controller": true}},
	}})
	if err != nil {
		return err
	}
	refs := owners(got)
	if len(refs) != 2 || refs["uid-one"]["controller"] != true || refs["uid-one"]["name"] != "parent-one" || refs["uid-two"]["controller"] != nil {
		return fmt.Errorf("after a strategic patch of ownerReferences [{uid: uid-one, controller: true}] the owners are %v: the element whose uid matches is merged into — it gains controller and keeps its name — and the others are left alone", metaOf(got)["ownerReferences"])
	}
	got, err = patchOK("deletes one ownerReference by uid", map[string]any{"metadata": map[string]any{
		"ownerReferences": []any{map[string]any{"uid": "uid-two", "$patch": "delete"}},
	}})
	if err != nil {
		return err
	}
	if refs := owners(got); len(refs) != 1 || refs["uid-one"] == nil {
		return fmt.Errorf("after a strategic patch of ownerReferences [{uid: uid-two, $patch: delete}] the owners are %v: $patch: delete removes the element with that uid, and only that one", metaOf(got)["ownerReferences"])
	}

	// Directives are instructions, not data. Storing one leaves a field in
	// the object no client knows how to read, and that the next patch would
	// try to obey.
	var directive func(node any, at string) string
	directive = func(node any, at string) string {
		switch n := node.(type) {
		case map[string]any:
			for key, value := range n {
				if strings.HasPrefix(key, "$") {
					return at + "." + key
				}
				if found := directive(value, at+"."+key); found != "" {
					return found
				}
			}
		case []any:
			for i, value := range n {
				if found := directive(value, at+"["+strconv.Itoa(i)+"]"); found != "" {
					return found
				}
			}
		}
		return ""
	}
	if found := directive(got, ""); found != "" {
		return fmt.Errorf("the stored object has a field %s: keys starting with $ in a strategic patch are directives to the server — $patch, $deleteFromPrimitiveList, $setElementOrder — and are never stored", found)
	}

	// An element of a keyed list has to carry its key, or there is no telling
	// which element it means.
	res, raw, err := srv.patch(ctx, path+"/settings", smp, map[string]any{"metadata": map[string]any{
		"ownerReferences": []any{map[string]any{"name": "no-uid"}},
	}})
	if err != nil {
		return err
	}
	if err := wantStatus("PATCH "+path+"/settings with an ownerReference that has no uid", res, raw, http.StatusUnprocessableEntity, "Invalid"); err != nil {
		return fmt.Errorf("%w\n\nownerReferences are merged by uid, and an element without one names no element: adding it would be guessing, and so would merging it into one", err)
	}
	return nil
}

// stageApplySSA checks server-side apply: a PATCH that carries the whole of
// what one client wants the object to look like, and a server that remembers,
// field by field, which client said what.
//
// The memory is the point. A merge patch can only add and change; to remove a
// field a client has to know it is there and say null. An apply removes what
// the client stopped mentioning — but only what that client set and nobody
// else also set, which the server can tell only because managedFields records
// every manager's fields. Graded hardest: the removal rule, and that applying
// the same thing twice is not a write.
func stageApplySSA(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const (
		path  = "/api/v1/namespaces/default/configmaps"
		apply = "application/apply-patch+yaml"
	)
	applyAs := func(manager string, body any) (*http.Response, []byte, error) {
		return srv.patch(ctx, path+"/settings?fieldManager="+manager, apply, body)
	}
	// applyOK sends one apply that has to answer 200 and returns the object
	// as a fresh read describes it.
	applyOK := func(manager, what string, body map[string]any) (map[string]any, error) {
		res, raw, err := applyAs(manager, body)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("an apply by %q that %s answered %d rather than 200: the object exists, so an apply changes it\nthe config was: %v\nthe body was:\n%s",
				manager, what, res.StatusCode, body, tail(string(raw)))
		}
		return srv.getJSON(ctx, path+"/settings")
	}
	config := func(metadata, data map[string]any) map[string]any {
		meta := map[string]any{"name": "settings"}
		for k, v := range metadata {
			meta[k] = v
		}
		obj := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta}
		if data != nil {
			obj["data"] = data
		}
		return obj
	}
	dataOf := func(obj map[string]any) map[string]any {
		data, _ := obj["data"].(map[string]any)
		return data
	}

	// Every refusal comes before the object exists, so a server that refuses
	// and creates anyway is caught by the 404 after them.
	refusals := []struct {
		what, query string
		body        any
		why         string
	}{
		{"with no fieldManager", "", config(nil, map[string]any{"colour": "blue"}),
			"an apply is a manager declaring the fields it owns, and without a name there is nobody to record them against — and nothing to compare with next time to see what it dropped"},
		{"with fieldManager empty", "?fieldManager=", config(nil, map[string]any{"colour": "blue"}),
			"an empty name is no name: the next apply could not be matched to this one"},
		{"whose body is a list", "?fieldManager=alpha", []any{config(nil, nil)},
			"an apply is the object as this manager wants it, and an object is a JSON object"},
		{"with no apiVersion", "?fieldManager=alpha", map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"name": "settings"}},
			"an apply is a whole configuration, and a configuration says what it is: apiVersion and kind are what the server checks it against"},
		{"with no kind", "?fieldManager=alpha", map[string]any{"apiVersion": "v1", "metadata": map[string]any{"name": "settings"}},
			"an apply is a whole configuration, and a configuration says what it is: apiVersion and kind are what the server checks it against"},
		{"naming a different namespace in metadata.namespace", "?fieldManager=alpha", config(map[string]any{"namespace": "other"}, nil),
			"the URL says default and the body says other; the object cannot live in both"},
		{"naming a different object in metadata.name", "?fieldManager=alpha", config(map[string]any{"name": "other"}, nil),
			"the URL says settings and the body says other; applying either one is guessing which the client meant"},
	}
	for _, r := range refusals {
		res, raw, err := srv.patch(ctx, path+"/settings"+r.query, apply, r.body)
		if err != nil {
			return err
		}
		if err := wantStatus("an apply to "+path+"/settings "+r.what, res, raw, http.StatusBadRequest, "BadRequest"); err != nil {
			return fmt.Errorf("%w\n\n%s", err, r.why)
		}
	}
	if res, raw, err := srv.send(ctx, http.MethodGet, path+"/settings", nil); err != nil {
		return err
	} else if res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("after seven applies that were all refused, GET %s/settings answered %d rather than 404: a refused request changes nothing, and that includes not creating the object\nthe body was:\n%s",
			path, res.StatusCode, tail(string(raw)))
	}

	res, raw, err := srv.patch(ctx, "/api/v1/namespaces/nowhere/configmaps/settings?fieldManager=alpha", apply, config(nil, nil))
	if err != nil {
		return err
	}
	if err := wantStatus("an apply to a configmap in namespace nowhere, which does not exist", res, raw, http.StatusNotFound, "NotFound"); err != nil {
		return fmt.Errorf("%w\n\nan apply creates the object if it is missing, but not the namespace it lives in — stage 10's rule holds for every way of creating", err)
	}

	// An apply of something that is not there creates it: the client
	// describes what it wants, not whether it exists yet.
	first := config(
		map[string]any{"labels": map[string]any{"app": "web"}, "finalizers": []any{"example.com/a", "example.com/b"}},
		map[string]any{"colour": "blue", "size": "large"},
	)
	res, raw, err = applyAs("alpha", first)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("the first apply to %s/settings, which does not exist yet, answered %d rather than 201: apply is create-or-update — the client sends what it wants, and the server works out which of the two that is\nthe body was:\n%s",
			path, res.StatusCode, tail(string(raw)))
	}
	reply, err := decode(raw)
	if err != nil {
		return fmt.Errorf("the 201 for the first apply is not JSON (%w)\nthe body was:\n%s", err, tail(string(raw)))
	}
	for _, field := range []string{"uid", "resourceVersion", "creationTimestamp"} {
		if metaField(reply, field) == "" {
			return fmt.Errorf("the object the first apply created has no metadata.%s: a create by apply is a create, and the server fills in what it fills in on a POST", field)
		}
	}
	got, err := srv.getJSON(ctx, path+"/settings")
	if err != nil {
		return err
	}
	if data := dataOf(got); data["colour"] != "blue" || data["size"] != "large" {
		return fmt.Errorf("after an apply creating settings with data {colour: blue, size: large} the stored data is %v", got["data"])
	}

	alphaFirst := []string{"data.colour", "data.size", "metadata.finalizers", "metadata.labels.app"}
	if err := ssaOwns(got, "alpha", alphaFirst); err != nil {
		return fmt.Errorf("%w\n\nthe config was %v: every key it sets is a field alpha now owns, maps are walked into, and a list is one field — never apiVersion, kind or metadata.name, which say which object this is rather than what it holds", err, first)
	}
	if entries, _ := got["metadata"].(map[string]any)["managedFields"].([]any); len(entries) != 1 {
		return fmt.Errorf("after one apply by one manager metadata.managedFields has %d entries, and it has one: an entry per manager and operation", len(entries))
	}
	entry := ssaEntry(got, "alpha")
	for key, want := range map[string]any{"apiVersion": "v1", "fieldsType": "FieldsV1"} {
		if entry[key] != want {
			return fmt.Errorf("alpha's managedFields entry has %s %v rather than %v: kubectl and client-go read these entries, and they are the shape the real server writes\nthe entry was: %v", key, entry[key], want, entry)
		}
	}
	if stamp, _ := entry["time"].(string); stamp == "" {
		return fmt.Errorf("alpha's managedFields entry has no time: it is when this manager last changed what it owns, as RFC 3339\nthe entry was: %v", entry)
	} else if _, err := time.Parse(time.RFC3339, stamp); err != nil {
		return fmt.Errorf("alpha's managedFields time %q is not RFC 3339 (%v)", stamp, err)
	}

	// Applying the same config again is how every apply-based tool runs: on a
	// loop, whether or not anything changed. If that were a write, every
	// watcher in the cluster would be woken for nothing, every time.
	// Past a second boundary, so an entry time refreshed on a no-op apply shows.
	time.Sleep(1100 * time.Millisecond)
	before, err := applyOK("alpha", "is identical to the last one", first)
	if err != nil {
		return err
	}
	if metaField(before, "resourceVersion") != metaField(got, "resourceVersion") {
		return fmt.Errorf("re-applying exactly the config alpha already applied moved resourceVersion from %s to %s: a write whose result is what is already stored is not a write — tools apply on a loop, and each one would wake every watcher",
			metaField(got, "resourceVersion"), metaField(before, "resourceVersion"))
	}
	if !reflect.DeepEqual(before["metadata"].(map[string]any)["managedFields"], got["metadata"].(map[string]any)["managedFields"]) {
		return fmt.Errorf("re-applying an identical config changed managedFields from %v to %v: nothing changed, and that includes when alpha last changed something",
			got["metadata"].(map[string]any)["managedFields"], before["metadata"].(map[string]any)["managedFields"])
	}

	w, err := srv.watch(ctx, path+"?watch=true&resourceVersion="+metaField(before, "resourceVersion"))
	if err != nil {
		return err
	}
	defer w.stop()
	if _, err := applyOK("alpha", "is identical to the last one", first); err != nil {
		return err
	}
	if err := ssaQuiet(w, 2*time.Second); err != nil {
		return fmt.Errorf("%w\n\nthat event followed an apply identical to the one before it: no change, so no new version, and nothing for a watcher to be told", err)
	}

	// A second manager. It sets colour to the value already there, so the two
	// share that field rather than fight over it, and adds a key of its own.
	got, err = applyOK("beta", "adds data.shape and sets data.colour to the value it already has",
		config(nil, map[string]any{"colour": "blue", "shape": "round"}))
	if err != nil {
		return err
	}
	if data := dataOf(got); len(data) != 3 || data["colour"] != "blue" || data["size"] != "large" || data["shape"] != "round" {
		return fmt.Errorf("after beta applied data {colour: blue, shape: round} onto {colour: blue, size: large} the data is %v, and it should be all three keys: an apply is merged into the object — beta not mentioning size is not beta asking for it to go, because beta never owned it", got["data"])
	}
	if labels, _ := got["metadata"].(map[string]any)["labels"].(map[string]any); labels["app"] != "web" {
		return fmt.Errorf("after beta's apply, which said nothing of labels, metadata.labels is %v: the merge leaves what a config does not mention alone", labels)
	}
	if err := ssaOwns(got, "beta", []string{"data.colour", "data.shape"}); err != nil {
		return err
	}
	if err := ssaOwns(got, "alpha", alphaFirst); err != nil {
		return fmt.Errorf("%w\n\nbeta applied, not alpha: one manager's apply replaces that manager's entry and nobody else's", err)
	}
	kind, obj, err := w.next(ctx, "MODIFIED settings, for beta's apply")
	if err != nil {
		return err
	}
	if kind != "MODIFIED" || metaField(obj, "resourceVersion") != metaField(got, "resourceVersion") {
		return fmt.Errorf("the watch's next event was %s at resourceVersion %s, where MODIFIED at %s was expected for beta's apply: an apply that changes something is a write like any other, and the one before it changed nothing",
			kind, metaField(obj, "resourceVersion"), metaField(got, "resourceVersion"))
	}

	// alpha stops mentioning size and colour, and changes its finalizers.
	// size was alpha's alone, so it goes; colour is beta's too, so it stays.
	got, err = applyOK("alpha", "drops data.size and data.colour and replaces metadata.finalizers",
		config(map[string]any{"labels": map[string]any{"app": "web"}, "finalizers": []any{"example.com/c"}}, nil))
	if err != nil {
		return err
	}
	data := dataOf(got)
	if _, there := data["size"]; there {
		return fmt.Errorf("alpha applied a config without data.size, which only alpha had ever set, and data.size is still %v: an apply is the whole of what a manager wants, so a field it owned and no longer mentions is one it wants gone — that is how apply removes things without a null", data["size"])
	}
	if data["colour"] != "blue" {
		return fmt.Errorf("alpha dropped data.colour from its config, and data.colour is now %v rather than blue: beta set it too, and beta still wants it — a field is removed only when no manager owns it any more", data["colour"])
	}
	if data["shape"] != "round" {
		return fmt.Errorf("after alpha's apply data.shape is %v rather than round: alpha never owned it, so alpha leaving it out says nothing about it", data["shape"])
	}
	if finalizers, _ := got["metadata"].(map[string]any)["finalizers"].([]any); !reflect.DeepEqual(finalizers, []any{"example.com/c"}) {
		return fmt.Errorf("after alpha applied finalizers [example.com/c] over [example.com/a example.com/b] the list is %v: in an apply, as in a merge patch, a list is one value and is replaced whole", finalizers)
	}
	if err := ssaOwns(got, "alpha", []string{"metadata.finalizers", "metadata.labels.app"}); err != nil {
		return fmt.Errorf("%w\n\na manager's entry is exactly the fields of its latest config: what it dropped, it no longer owns", err)
	}
	if err := ssaOwns(got, "beta", []string{"data.colour", "data.shape"}); err != nil {
		return err
	}

	// An apply can carry a resourceVersion, and then it is a precondition.
	res, raw, err = applyAs("alpha", config(map[string]any{"resourceVersion": metaField(reply, "resourceVersion"), "labels": map[string]any{"app": "api"}}, nil))
	if err != nil {
		return err
	}
	if err := wantStatus("an apply by alpha carrying the resourceVersion from when settings was created", res, raw, http.StatusConflict, "Conflict"); err != nil {
		return fmt.Errorf("%w\n\nmost applies leave resourceVersion out and mean \"whatever is there\"; one that names a version is asking for exactly that version, as a PUT does", err)
	}
	current, err := srv.getJSON(ctx, path+"/settings")
	if err != nil {
		return err
	}
	if metaField(current, "resourceVersion") != metaField(got, "resourceVersion") {
		return fmt.Errorf("the refused apply moved resourceVersion from %s to %s: a 409 changes nothing", metaField(got, "resourceVersion"), metaField(current, "resourceVersion"))
	}

	// A PUT from a client that has never heard of managedFields. It read the
	// object into a type without the field, and writes back what it has.
	put := current
	meta := put["metadata"].(map[string]any)
	delete(meta, "managedFields")
	dataOf(put)["extra"] = "from a PUT"
	res, raw, err = srv.send(ctx, http.MethodPut, path+"/settings", put)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s/settings with the current resourceVersion and no managedFields answered %d rather than 200\nthe body was:\n%s", path, res.StatusCode, tail(string(raw)))
	}
	if got, err = srv.getJSON(ctx, path+"/settings"); err != nil {
		return err
	}
	for manager, want := range map[string][]string{"alpha": {"metadata.finalizers", "metadata.labels.app"}, "beta": {"data.colour", "data.shape"}} {
		if err := ssaOwns(got, manager, want); err != nil {
			return fmt.Errorf("%w\n\nthat followed a PUT whose body had no metadata.managedFields: most clients decode into types that do not have the field, so a write that leaves it out keeps what is stored — otherwise any old client erases every manager's record", err)
		}
	}

	// beta applies a config that sets nothing. Everything beta owned alone
	// goes, and an entry that owns nothing is not kept.
	if got, err = applyOK("beta", "sets no fields at all", config(nil, nil)); err != nil {
		return err
	}
	data = dataOf(got)
	for _, key := range []string{"colour", "shape"} {
		if _, there := data[key]; there {
			return fmt.Errorf("beta applied a config with no data, and data.%s is still there: beta was its only owner — alpha dropped colour two applies ago — so leaving it out removes it", key)
		}
	}
	if data["extra"] != "from a PUT" {
		return fmt.Errorf("after beta's empty apply data.extra is %v: beta never set it, so beta's apply cannot remove it", data["extra"])
	}
	if entry := ssaEntry(got, "beta"); entry != nil {
		return fmt.Errorf("beta's Apply entry is still in managedFields after beta applied a config that sets nothing: an entry that owns no fields says nothing, and is dropped\nthe entry was: %v", entry)
	}
	return nil
}

// ssaEntry is the manager's Apply entry in managedFields, or nil.
func ssaEntry(obj map[string]any, manager string) map[string]any {
	meta, _ := obj["metadata"].(map[string]any)
	entries, _ := meta["managedFields"].([]any)
	for _, e := range entries {
		if m, ok := e.(map[string]any); ok && m["manager"] == manager && m["operation"] == "Apply" {
			return m
		}
	}
	return nil
}

// ssaOwns insists the manager's Apply entry owns exactly these leaf paths.
// Compared as a set, because fieldsV1 is a tree and key order in it is not
// meaningful.
func ssaOwns(obj map[string]any, manager string, want []string) error {
	meta, _ := obj["metadata"].(map[string]any)
	if _, ok := meta["managedFields"].([]any); !ok {
		return fmt.Errorf("the object has no metadata.managedFields list (it has %v): every apply records which fields its manager now owns, and the next apply by that manager is compared with it", meta["managedFields"])
	}
	entry := ssaEntry(obj, manager)
	if entry == nil {
		return fmt.Errorf("metadata.managedFields has no entry with manager %q and operation Apply\nmanagedFields was: %v", manager, meta["managedFields"])
	}
	got, err := ssaLeaves(entry["fieldsV1"], "")
	if err != nil {
		return fmt.Errorf("%w\nthe entry was: %v", err, entry)
	}
	slices.Sort(got)
	want = slices.Sorted(slices.Values(want))
	if !slices.Equal(got, want) {
		return fmt.Errorf("%s's Apply entry owns %v, and it should own exactly %v\nfieldsV1 was: %v", manager, got, want, entry["fieldsV1"])
	}
	return nil
}

// ssaLeaves flattens a fieldsV1 tree into dotted leaf paths. A leaf is an
// empty object: the field itself, whatever its value is.
func ssaLeaves(node any, at string) ([]string, error) {
	tree, ok := node.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("fieldsV1 at %q is %v, and every node of it is an object: a field that holds a map is an object of its keys, and any other field is {}", at, node)
	}
	var out []string
	for key, child := range tree {
		name, found := strings.CutPrefix(key, "f:")
		if !found {
			return nil, fmt.Errorf("fieldsV1 has a key %q under %q, and every field key is written f:<name>", key, at)
		}
		path := name
		if at != "" {
			path = at + "." + name
		}
		if sub, _ := child.(map[string]any); len(sub) == 0 {
			out = append(out, path)
			continue
		}
		more, err := ssaLeaves(child, path)
		if err != nil {
			return nil, err
		}
		out = append(out, more...)
	}
	return out, nil
}

// ssaQuiet insists nothing arrives on the watch for a while. A BOOKMARK is
// not a change, so it is let through.
func ssaQuiet(w *watchStream, quiet time.Duration) error {
	deadline := time.After(quiet)
	for {
		select {
		case event, open := <-w.events:
			if !open {
				return fmt.Errorf("the watch on %s ended: %v", w.path, <-w.fail)
			}
			kind, _ := event["type"].(string)
			if kind == "BOOKMARK" {
				continue
			}
			obj, _ := event["object"].(map[string]any)
			return fmt.Errorf("the watch on %s sent %s %s at resourceVersion %s", w.path, kind, metaField(obj, "name"), metaField(obj, "resourceVersion"))
		case <-deadline:
			return nil
		}
	}
}

// stageApplyConflict checks what server-side apply is for: two writers who
// want different values in one field are told so, rather than taking turns
// silently undoing each other.
//
// Ownership is recorded per field in metadata.managedFields. An apply that
// would change a field someone else owns is refused with a 409 naming them;
// one that agrees with them shares the field; force takes it. Plain writes —
// PUT and the three patch dialects — never conflict, but they do take
// ownership of what they changed, which is how an apply learns that somebody
// edited its field by hand.
func stageApplyConflict(ctx context.Context, _ *kube.Env, bin string) error {
	// On disk, so the conflict check and the write are far enough apart for a
	// check made outside the lock to be caught.
	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the store: %w", err)
	}
	defer os.RemoveAll(dir)
	srv, cleanup, err := serve(ctx, bin, "-data", dir)
	if err != nil {
		return err
	}
	defer cleanup()

	const path = "/api/v1/namespaces/default/configmaps"

	applyOK := func(name, manager string, force bool, config map[string]any) (map[string]any, error) {
		res, raw, err := conflictApply(ctx, srv, path, name, manager, force, config)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
			return nil, fmt.Errorf("an apply by %q (force=%v) to %s/%s answered %d rather than 200 or 201\nthe config was: %v\nthe body was:\n%s",
				manager, force, path, name, res.StatusCode, config, tail(string(raw)))
		}
		return srv.getJSON(ctx, path+"/"+name)
	}
	// refused insists an apply is a conflict with manager over field, and that
	// the object is exactly as it was.
	refused := func(name, manager string, config map[string]any, other, field string) error {
		before, err := srv.getJSON(ctx, path+"/"+name)
		if err != nil {
			return err
		}
		res, raw, err := conflictApply(ctx, srv, path, name, manager, false, config)
		if err != nil {
			return err
		}
		what := fmt.Sprintf("an apply by %q setting %s to a value %q owns and holds differently", manager, field, other)
		if err := wantStatus(what, res, raw, http.StatusConflict, "Conflict"); err != nil {
			return fmt.Errorf("%w\n\na field another manager owns is theirs: an apply that would change it is refused, so that two writers who disagree find out rather than overwriting each other on every reconcile\nmanagedFields before the apply: %s",
				err, conflictDescribe(before))
		}
		if err := conflictWantNamed(what, raw, other, field); err != nil {
			return err
		}
		after, err := srv.getJSON(ctx, path+"/"+name)
		if err != nil {
			return err
		}
		if metaField(after, "resourceVersion") != metaField(before, "resourceVersion") {
			return fmt.Errorf("%s answered 409, but resourceVersion moved from %q to %q: a refused apply is refused whole — nothing of it is written, not the fields that did not conflict and not the managedFields",
				what, metaField(before, "resourceVersion"), metaField(after, "resourceVersion"))
		}
		return nil
	}

	// alpha applies first and owns everything it set.
	got, err := applyOK("settings", "alpha", false, conflictConfig("settings",
		map[string]any{"colour": "blue", "size": "large"}, map[string]any{"team": "a"}))
	if err != nil {
		return err
	}
	if err := conflictWantOwns(got, "alpha", "Apply", ".data.colour", ".data.size", ".metadata.labels.team"); err != nil {
		return err
	}

	// A manager changing a field only it owns is not a conflict with itself.
	if _, err = applyOK("settings", "alpha", false, conflictConfig("settings",
		map[string]any{"colour": "blue", "size": "large"}, map[string]any{"team": "b"})); err != nil {
		return fmt.Errorf("%w\n\nalpha changed metadata.labels.team, which only alpha owns: a manager never conflicts with itself", err)
	}

	// beta wants a different colour.
	if err := refused("settings", "beta", conflictConfig("settings", map[string]any{"colour": "red"}, nil), "alpha", ".data.colour"); err != nil {
		return err
	}
	if got, err = srv.getJSON(ctx, path+"/settings"); err != nil {
		return err
	}
	if fields, there := conflictOwned(got, "beta", "Apply"); there {
		return fmt.Errorf("after beta's apply was refused, beta has a managedFields entry owning %v: a refused apply records nothing", fields)
	}

	// beta agrees with alpha's colour: that is not a conflict, and the field
	// now has two owners.
	if got, err = applyOK("settings", "beta", false, conflictConfig("settings", map[string]any{"colour": "blue"}, nil)); err != nil {
		return fmt.Errorf("%w\n\nbeta asked for the value alpha already set — two managers that agree are not in conflict, and both of them own the field afterwards", err)
	}
	if err := conflictWantOwns(got, "beta", "Apply", ".data.colour"); err != nil {
		return err
	}
	if err := conflictWantOwns(got, "alpha", "Apply", ".data.colour"); err != nil {
		return fmt.Errorf("%w\n\nbeta applying the same value shares the field; it does not take it from alpha", err)
	}

	// alpha stops setting colour. beta still wants it, so it stays.
	if got, err = applyOK("settings", "alpha", false, conflictConfig("settings",
		map[string]any{"size": "large"}, map[string]any{"team": "a"})); err != nil {
		return err
	}
	if data, _ := got["data"].(map[string]any); data["colour"] != "blue" {
		return fmt.Errorf("alpha applied a config without data.colour, which beta also owns, and data.colour is now %v rather than blue: dropping a field from your config gives up your claim on it, and the field is only removed when nobody is left claiming it", data["colour"])
	}
	if fields, _ := conflictOwned(got, "alpha", "Apply"); fields[".data.colour"] {
		return fmt.Errorf("alpha's config no longer sets data.colour, but alpha's managedFields entry still owns it: an apply entry is replaced by exactly the fields of the latest config")
	}

	// beta wants size smaller. Without force it is alpha's; with force it is
	// beta's, and alpha loses it while keeping the rest of what it owns.
	smaller := conflictConfig("settings", map[string]any{"colour": "blue", "size": "small"}, nil)
	if err := refused("settings", "beta", smaller, "alpha", ".data.size"); err != nil {
		return err
	}
	if got, err = applyOK("settings", "beta", true, smaller); err != nil {
		return fmt.Errorf("%w\n\nforce=true is the applier saying it knows the field is someone else's and means to take it", err)
	}
	if data, _ := got["data"].(map[string]any); data["size"] != "small" {
		return fmt.Errorf("after beta's forced apply of data.size: small, data.size is %v", data["size"])
	}
	if err := conflictWantOwns(got, "beta", "Apply", ".data.size"); err != nil {
		return err
	}
	fields, there := conflictOwned(got, "alpha", "Apply")
	if fields[".data.size"] {
		return fmt.Errorf("after beta forced data.size, alpha's entry still owns it: force moves the field — a field owned by two managers who disagree about its value is the state conflicts exist to prevent\nmanagedFields: %s", conflictDescribe(got))
	}
	if !there || !fields[".metadata.labels.team"] {
		return fmt.Errorf("after beta forced data.size, alpha no longer owns metadata.labels.team: force takes the fields that conflicted and nothing else\nmanagedFields: %s", conflictDescribe(got))
	}

	// A hand edit is a write like any other, and it owns what it changed. The
	// applier finds out on its next apply rather than reverting it unseen.
	if _, err := applyOK("edited", "alpha", false, conflictConfig("edited", map[string]any{"colour": "blue"}, nil)); err != nil {
		return err
	}
	if got, err = conflictPut(ctx, srv, path, "edited", "?fieldManager=editor", "colour", "red"); err != nil {
		return fmt.Errorf("%w\n\nalpha owns data.colour by apply, and this PUT changes it: a PUT is not an apply and never conflicts — it takes the field instead", err)
	}
	if err := conflictWantOwns(got, "editor", "Update", ".data.colour"); err != nil {
		return fmt.Errorf("%w\n\na PUT or patch is recorded too, as operation Update under the manager named by ?fieldManager=, owning every field whose value it changed", err)
	}
	if fields, there := conflictOwned(got, "alpha", "Apply"); there {
		return fmt.Errorf("editor's PUT changed data.colour, the only field alpha owned, yet alpha still has an entry owning %v: the field moved to editor, and an entry left owning nothing is dropped", conflictSorted(fields))
	}
	if err := refused("edited", "alpha", conflictConfig("edited", map[string]any{"colour": "blue"}, nil), "editor", ".data.colour"); err != nil {
		return fmt.Errorf("%w\n\nthis is what ownership is for: without it, alpha's next apply would silently put back the value someone just changed by hand", err)
	}
	if got, err = applyOK("edited", "alpha", true, conflictConfig("edited", map[string]any{"colour": "blue"}, nil)); err != nil {
		return err
	}
	if data, _ := got["data"].(map[string]any); data["colour"] != "blue" {
		return fmt.Errorf("after alpha's forced apply of data.colour: blue, data.colour is %v", data["colour"])
	}
	if fields, there := conflictOwned(got, "editor", "Update"); there {
		return fmt.Errorf("alpha forced data.colour, the only field editor owned, yet editor still has an entry owning %v: an entry left owning nothing is dropped", conflictSorted(fields))
	}

	// A write that names no manager still has one.
	if got, err = conflictPut(ctx, srv, path, "edited", "", "shape", "round"); err != nil {
		return err
	}
	if err := conflictWantOwns(got, "unknown", "Update", ".data.shape"); err != nil {
		return fmt.Errorf("%w\n\na write without ?fieldManager= is recorded under the manager \"unknown\" — every field still has an owner", err)
	}
	if fields, _ := conflictOwned(got, "unknown", "Update"); fields[".data.colour"] {
		return fmt.Errorf("a PUT that left data.colour as it was took ownership of it: a write owns what it changed, compared old against new, not every field it happened to send — a PUT sends all of them")
	}
	if err := conflictWantOwns(got, "alpha", "Apply", ".data.colour"); err != nil {
		return fmt.Errorf("%w\n\nthe PUT's body had no managedFields, and a client that never heard of them cannot erase them", err)
	}
	res, raw, err := srv.patch(ctx, path+"/edited", "application/merge-patch+json", map[string]any{"data": map[string]any{"mood": "calm"}})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PATCH %s/edited with a merge patch answered %d rather than 200\nthe body was:\n%s", path, res.StatusCode, tail(string(raw)))
	}
	if got, err = srv.getJSON(ctx, path+"/edited"); err != nil {
		return err
	}
	if err := conflictWantOwns(got, "unknown", "Update", ".data.shape", ".data.mood"); err != nil {
		return fmt.Errorf("%w\n\nthere is one entry per manager and operation: the patch's field joins the PUT's in the same \"unknown\" Update entry", err)
	}
	if n := conflictEntries(got, "unknown", "Update"); n != 1 {
		return fmt.Errorf("managedFields has %d entries for manager \"unknown\" with operation Update, and there is one per manager and operation\nmanagedFields: %s", n, conflictDescribe(got))
	}

	// Many managers at once, each wanting its own value in a field nobody
	// owns yet. The first to reach the store owns it, and every other one now
	// disagrees with an owner. A check made before taking the lock lets several
	// of them see the field free and all succeed.
	// A large object makes the copy a pre-lock read takes slow enough for the
	// other writers to land between that read and the lock.
	ballast := map[string]any{}
	for i := range 2000 {
		ballast["base-"+strconv.Itoa(i)] = "x"
	}
	if _, err := applyOK("race", "seed", false, conflictConfig("race", ballast, nil)); err != nil {
		return err
	}
	const writers = 20
	type outcome struct {
		manager string
		res     *http.Response
		raw     []byte
		err     error
	}
	outcomes := make(chan outcome, writers)
	for i := range writers {
		go func() {
			manager := "writer-" + strconv.Itoa(i)
			res, raw, err := conflictApply(ctx, srv, path, "race", manager, false,
				conflictConfig("race", map[string]any{"winner": manager}, nil))
			outcomes <- outcome{manager, res, raw, err}
		}()
	}
	var winners []string
	for range writers {
		o := <-outcomes
		if o.err != nil {
			return o.err
		}
		switch o.res.StatusCode {
		case http.StatusOK:
			winners = append(winners, o.manager)
		case http.StatusConflict:
			if err := wantStatus("an apply racing "+strconv.Itoa(writers-1)+" others for data.winner", o.res, o.raw, http.StatusConflict, "Conflict"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("one of %d concurrent applies, each by its own manager setting data.winner to its own name, answered %d: the first to reach the store wins with 200, and each of the rest is a 409 Conflict with whoever got there first\nthe body was:\n%s",
				writers, o.res.StatusCode, tail(string(o.raw)))
		}
	}
	if len(winners) != 1 {
		sort.Strings(winners)
		return fmt.Errorf("%d concurrent applies set data.winner to %d different values, and %d of them succeeded (%s): only the first can find the field unowned — a conflict check made against a read taken before the lock lets several writers each believe they are first, and the last write silently wins",
			writers, writers, len(winners), strings.Join(winners, ", "))
	}
	if got, err = srv.getJSON(ctx, path+"/race"); err != nil {
		return err
	}
	if data, _ := got["data"].(map[string]any); data["winner"] != winners[0] {
		return fmt.Errorf("only %s's apply succeeded, but data.winner is %v: the refused applies wrote something anyway", winners[0], data["winner"])
	}
	for _, entry := range conflictManaged(got) {
		manager, _ := entry["manager"].(string)
		if manager != winners[0] && conflictLeaves(entry["fieldsV1"], "")[".data.winner"] {
			return fmt.Errorf("%s won data.winner, yet %s also owns it: a refused apply records no ownership\nmanagedFields: %s", winners[0], manager, conflictDescribe(got))
		}
	}
	return nil
}

// conflictConfig is an apply config for a configmap: identity, data, and
// labels when there are any.
func conflictConfig(name string, data, labels map[string]any) map[string]any {
	meta := map[string]any{"name": name}
	if labels != nil {
		meta["labels"] = labels
	}
	return map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta, "data": data}
}

// conflictApply sends one server-side apply as manager.
func conflictApply(ctx context.Context, srv *server, path, name, manager string, force bool, config map[string]any) (*http.Response, []byte, error) {
	query := "?fieldManager=" + manager
	if force {
		query += "&force=true"
	}
	return srv.patch(ctx, path+"/"+name+query, "application/apply-patch+yaml", config)
}

// conflictPut reads an object, sets one data key, and PUTs it back with the
// given query. managedFields is left out of the body, as an older client
// would, so the server's own bookkeeping is all that decides ownership.
func conflictPut(ctx context.Context, srv *server, path, name, query, key, value string) (map[string]any, error) {
	obj, err := srv.getJSON(ctx, path+"/"+name)
	if err != nil {
		return nil, err
	}
	data, _ := obj["data"].(map[string]any)
	if data == nil {
		data = map[string]any{}
		obj["data"] = data
	}
	data[key] = value
	if meta, ok := obj["metadata"].(map[string]any); ok {
		delete(meta, "managedFields")
	}
	res, raw, err := srv.send(ctx, http.MethodPut, path+"/"+name+query, obj)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("PUT %s/%s%s setting data.%s answered %d rather than 200\nthe body was:\n%s",
			path, name, query, key, res.StatusCode, tail(string(raw)))
	}
	return srv.getJSON(ctx, path+"/"+name)
}

// conflictWantNamed insists a conflict's Status says who holds the field and
// which field it is, in the message and in details.causes.
func conflictWantNamed(what string, raw []byte, manager, field string) error {
	status, _ := decode(raw)
	key := field[strings.LastIndex(field, ".")+1:]
	message, _ := status["message"].(string)
	if !strings.Contains(message, manager) || !strings.Contains(message, key) {
		return fmt.Errorf("the 409 for %s has message %q, which does not name both the manager %q and the field %s: the message is all kubectl shows, and the person reading it needs to know whose field it is and which one to decide whether to force",
			what, message, manager, field)
	}
	details, _ := status["details"].(map[string]any)
	causes, _ := details["causes"].([]any)
	for _, c := range causes {
		cause, _ := c.(map[string]any)
		if f, _ := cause["field"].(string); strings.Contains(f, key) {
			return nil
		}
	}
	return fmt.Errorf("the 409 for %s has details.causes %v, with no cause whose field is %s: each conflict is one cause, {\"type\": \"FieldManagerConflict\", \"message\": ..., \"field\": ...}, which is what a client reads to resolve them one by one",
		what, details["causes"], field)
}

// conflictManaged is an object's managedFields entries.
func conflictManaged(obj map[string]any) []map[string]any {
	meta, _ := obj["metadata"].(map[string]any)
	list, _ := meta["managedFields"].([]any)
	var entries []map[string]any
	for _, e := range list {
		if entry, ok := e.(map[string]any); ok {
			entries = append(entries, entry)
		}
	}
	return entries
}

// conflictOwned is the set of field paths one manager owns by one operation,
// and whether it has an entry at all.
func conflictOwned(obj map[string]any, manager, operation string) (map[string]bool, bool) {
	for _, entry := range conflictManaged(obj) {
		if entry["manager"] == manager && entry["operation"] == operation {
			return conflictLeaves(entry["fieldsV1"], ""), true
		}
	}
	return map[string]bool{}, false
}

func conflictEntries(obj map[string]any, manager, operation string) int {
	n := 0
	for _, entry := range conflictManaged(obj) {
		if entry["manager"] == manager && entry["operation"] == operation {
			n++
		}
	}
	return n
}

// conflictLeaves flattens fieldsV1 into dotted paths: {"f:data":{"f:a":{}}}
// is .data.a.
func conflictLeaves(node any, at string) map[string]bool {
	out := map[string]bool{}
	m, _ := node.(map[string]any)
	children := 0
	for key, child := range m {
		if name, ok := strings.CutPrefix(key, "f:"); ok {
			children++
			for leaf := range conflictLeaves(child, at+"."+name) {
				out[leaf] = true
			}
		}
	}
	if children == 0 && at != "" {
		out[at] = true
	}
	return out
}

func conflictWantOwns(obj map[string]any, manager, operation string, want ...string) error {
	fields, there := conflictOwned(obj, manager, operation)
	if !there {
		return fmt.Errorf("metadata.managedFields has no entry for manager %q with operation %s, and it should own %s\nmanagedFields: %s",
			manager, operation, strings.Join(want, ", "), conflictDescribe(obj))
	}
	for _, field := range want {
		if !fields[field] {
			return fmt.Errorf("%s's %s entry in metadata.managedFields does not own %s\nmanagedFields: %s", manager, operation, field, conflictDescribe(obj))
		}
	}
	return nil
}

// conflictDescribe is managedFields as one line per entry, for error messages.
func conflictDescribe(obj map[string]any) string {
	entries := conflictManaged(obj)
	if len(entries) == 0 {
		return "(none)"
	}
	var lines []string
	for _, entry := range entries {
		lines = append(lines, fmt.Sprintf("%v (%v): %s", entry["manager"], entry["operation"],
			strings.Join(conflictSorted(conflictLeaves(entry["fieldsV1"], "")), ", ")))
	}
	return "\n  " + strings.Join(lines, "\n  ")
}

func conflictSorted(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// stageSubresourceStatus checks an object is split in two at the API: spec
// written through the object, status through its /status subresource, and
// neither write able to touch the other half.
//
// The split is about ownership. A user owns what they asked for; a controller
// owns what it observed. Two endpoints let RBAC grant one without the other,
// and keep a user's stale read-modify-write from erasing a status a controller
// wrote in between.
func stageSubresourceStatus(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := statusDiscovery(ctx, srv); err != nil {
		return err
	}

	const (
		name = "status-demo"
		path = "/api/v1/namespaces/" + name
	)
	created, err := srv.create(ctx, "/api/v1/namespaces", map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": name, "labels": map[string]any{"team": "a"}},
	})
	if err != nil {
		return err
	}
	first := metaField(created, "resourceVersion")

	// The main endpoint, carrying a status the client read and then edited.
	// The labels are the client's to change; the phase is not.
	res, body, err := srv.send(ctx, http.MethodPut, path, statusNamespace(name, first, "b", "Terminating"))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s answered %d, and a namespace is updated like a configmap: 200 with the object as stored — the verb is new for this resource, not new to the server\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	updated, err := decode(body)
	if err != nil {
		return fmt.Errorf("the reply to PUT %s is not JSON (%w)\nthe body was:\n%s", path, err, tail(string(body)))
	}
	if err := statusWant(updated, "the reply to PUT "+path, "b", "Active"); err != nil {
		return fmt.Errorf("%w\n\nthe main endpoint writes everything but status: a status in the body is ignored and the stored one kept, because a client that read the object, changed a label and sent it back is sending a status it never meant to write", err)
	}
	second := metaField(updated, "resourceVersion")
	if second == first {
		return fmt.Errorf("the namespace still has resourceVersion %q after PUT %s changed its labels: a write moves the store forward whichever endpoint it came through", first, path)
	}
	stored, err := srv.getJSON(ctx, path)
	if err != nil {
		return err
	}
	if err := statusWant(stored, "GET "+path+" after the PUT", "b", "Active"); err != nil {
		return fmt.Errorf("%w\n\nthe reply to a write has to be what was stored, and the store is where status is protected", err)
	}

	// Opened from the version just written, so the next event is the status
	// write and nothing earlier.
	w, err := srv.watch(ctx, "/api/v1/namespaces?watch=true&resourceVersion="+url.QueryEscape(second))
	if err != nil {
		return err
	}
	defer w.stop()

	// The subresource, the other way round: the phase is taken, the labels in
	// the body are not.
	res, body, err = srv.send(ctx, http.MethodPut, path+"/status", statusNamespace(name, second, "c", "Terminating"))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s/status answered %d: the status subresource is how a controller writes what it observed, and it answers 200 with the whole object as stored\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}
	written, err := decode(body)
	if err != nil {
		return fmt.Errorf("the reply to PUT %s/status is not JSON (%w)\nthe body was:\n%s", path, err, tail(string(body)))
	}
	if err := statusWant(written, "the reply to PUT "+path+"/status", "b", "Terminating"); err != nil {
		return fmt.Errorf("%w\n\n/status takes status from the body and nothing else: the labels are the user's, and a controller holding an old copy of them must not be able to put that copy back", err)
	}
	third := metaField(written, "resourceVersion")
	if third == second {
		return fmt.Errorf("the namespace still has resourceVersion %q after PUT %s/status changed its phase: a status write is a write, and a client holding the old version has to be able to tell", second, path)
	}
	kind, obj, err := w.next(ctx, "MODIFIED "+name+" for the status write")
	if err != nil {
		return fmt.Errorf("%w\n\na status write goes through the same store as any other, so it reaches watchers the same way — it is what every controller watching the object is waiting for", err)
	}
	if kind != "MODIFIED" || metaField(obj, "name") != name {
		return fmt.Errorf("the watch on /api/v1/namespaces from resourceVersion %s sent %s %s where MODIFIED %s was expected: the status write is the only write since that version", second, kind, metaField(obj, "name"), name)
	}
	if phase := statusPhase(obj); phase != "Terminating" {
		return fmt.Errorf("the MODIFIED event for the status write carries status.phase %q rather than Terminating: an event holds the object as it is after the write", phase)
	}

	// The subresource reads as the object it is part of.
	viaStatus, err := srv.getJSON(ctx, path+"/status")
	if err != nil {
		return fmt.Errorf("%w\n\nGET on the status subresource answers the whole object, the same as GET on the object: a controller reads through the endpoint it writes through", err)
	}
	if err := statusWant(viaStatus, "GET "+path+"/status", "b", "Terminating"); err != nil {
		return err
	}
	if rv := metaField(viaStatus, "resourceVersion"); rv != third {
		return fmt.Errorf("GET %s/status answered resourceVersion %q, and the namespace is at %q: it is the same object, read through another URL", path, rv, third)
	}

	// Stale on either endpoint is the conflict it always was. This is the case
	// the split exists for: two writers working from the same old read.
	for _, target := range []string{path, path + "/status"} {
		res, body, err := srv.send(ctx, http.MethodPut, target, statusNamespace(name, first, "d", "Active"))
		if err != nil {
			return err
		}
		if err := statusConflict(target, res, body); err != nil {
			return err
		}
	}
	stored, err = srv.getJSON(ctx, path)
	if err != nil {
		return err
	}
	if rv := metaField(stored, "resourceVersion"); rv != third {
		return fmt.Errorf("after two refused PUTs the namespace is at resourceVersion %q rather than %q: a write that answered 409 has to have left the store alone", rv, third)
	}

	for _, target := range []string{"/api/v1/namespaces/absent", "/api/v1/namespaces/absent/status"} {
		res, body, err := srv.send(ctx, http.MethodPut, target, statusNamespace("absent", "", "a", "Active"))
		if err != nil {
			return err
		}
		if err := wantStatus("PUT "+target+", where absent has never been created", res, body, http.StatusNotFound, "NotFound"); err != nil {
			return fmt.Errorf("%w\n\nan update needs something to update: a controller reporting on an object that has gone needs to hear it has gone, not to bring it back", err)
		}
	}

	res, body, err = srv.send(ctx, http.MethodPut, path, statusNamespace("something-else", "", "a", "Active"))
	if err != nil {
		return err
	}
	if err := wantStatus("PUT "+path+" carrying metadata.name \"something-else\"", res, body, http.StatusBadRequest, "BadRequest"); err != nil {
		return fmt.Errorf("%w\n\nthe name in the body has to agree with the one in the URL, for a namespace as for a configmap: an update is not a rename", err)
	}
	return nil
}

// statusDiscovery insists /api/v1 offers update on namespaces and lists the
// status subresource: kubectl and client-go's UpdateStatus are written against
// what discovery says exists.
func statusDiscovery(ctx context.Context, srv *server) error {
	list, err := srv.getJSON(ctx, "/api/v1")
	if err != nil {
		return err
	}
	entries := map[string]map[string]any{}
	resources, _ := list["resources"].([]any)
	for _, r := range resources {
		if m, ok := r.(map[string]any); ok {
			name, _ := m["name"].(string)
			entries[name] = m
		}
	}
	if verbs, _ := entries["namespaces"]["verbs"].([]any); !contains(verbs, "update") {
		return fmt.Errorf("the namespaces entry in GET /api/v1 does not offer the verb \"update\" (it offers %v): a namespace can now be written back, and a client is not told so any other way", entries["namespaces"]["verbs"])
	}
	sub := entries["namespaces/status"]
	if sub == nil {
		return fmt.Errorf("GET /api/v1 lists no resource named namespaces/status: a subresource is discovered as its own entry, <resource>/<subresource>, and that name is what an RBAC rule grants to let a controller write status without writing spec")
	}
	if namespaced, _ := sub["namespaced"].(bool); namespaced {
		return fmt.Errorf("the namespaces/status entry says namespaced: true, and a subresource is scoped like the resource it belongs to — namespaces are cluster-scoped")
	}
	if sub["kind"] != "Namespace" {
		return fmt.Errorf("the namespaces/status entry has kind %v rather than Namespace: what goes in and comes out of /status is the whole object", sub["kind"])
	}
	verbs, _ := sub["verbs"].([]any)
	for _, want := range []string{"get", "update"} {
		if !contains(verbs, want) {
			return fmt.Errorf("the namespaces/status entry does not offer the verb %q (it offers %v): status is read and replaced through this endpoint", want, sub["verbs"])
		}
	}
	return nil
}

// statusNamespace is a whole Namespace as a client would send it back, with
// both halves edited, so each endpoint can be seen to take only its own.
func statusNamespace(name, resourceVersion, team, phase string) map[string]any {
	meta := map[string]any{"name": name, "labels": map[string]any{"team": team}}
	if resourceVersion != "" {
		meta["resourceVersion"] = resourceVersion
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   meta,
		"status":     map[string]any{"phase": phase},
	}
}

func statusPhase(obj map[string]any) string {
	status, _ := obj["status"].(map[string]any)
	phase, _ := status["phase"].(string)
	return phase
}

// statusWant checks both halves of a namespace: its team label and its phase.
func statusWant(obj map[string]any, what, team, phase string) error {
	meta, _ := obj["metadata"].(map[string]any)
	labels, _ := meta["labels"].(map[string]any)
	if labels["team"] != team {
		return fmt.Errorf("%s has labels %v, where team should be %q", what, meta["labels"], team)
	}
	if got := statusPhase(obj); got != phase {
		return fmt.Errorf("%s has status.phase %q, where it should be %q", what, got, phase)
	}
	return nil
}

func statusConflict(target string, res *http.Response, body []byte) error {
	if err := wantStatus("PUT "+target+" with the resourceVersion the namespace had when it was created", res, body, http.StatusConflict, "Conflict"); err != nil {
		return fmt.Errorf("%w\n\nsplitting the object does not split its version: there is one resourceVersion for the whole namespace, and a write through either endpoint from an old read is refused rather than allowed to undo what happened since", err)
	}
	return nil
}

// stageSubresourceScale checks a second resource, the ReplicationController,
// and the /scale subresource that reads and writes one number of it.
//
// Scale is a shape shared by every resource that has a replica count. The
// HorizontalPodAutoscaler and kubectl scale work through it on Deployments,
// StatefulSets and custom resources alike without knowing any of their
// schemas, and RBAC can grant resizing without granting the rest of the spec.
func stageSubresourceScale(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := scaleDiscovery(ctx, srv); err != nil {
		return err
	}

	const (
		ns    = "scale-demo"
		rcs   = "/api/v1/namespaces/" + ns + "/replicationcontrollers"
		rc    = rcs + "/web"
		scale = rc + "/scale"
	)
	if _, err := srv.create(ctx, "/api/v1/namespaces", map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": ns},
	}); err != nil {
		return err
	}

	// No replicas, no selector, and a status the server did not observe: all
	// three are the server's to fill in.
	created, err := srv.create(ctx, rcs, map[string]any{
		"apiVersion": "v1",
		"kind":       "ReplicationController",
		"metadata":   map[string]any{"name": "web", "labels": map[string]any{"app": "web"}},
		"spec":       map[string]any{"template": scaleTemplate()},
		"status":     map[string]any{"replicas": 7},
	})
	if err != nil {
		return fmt.Errorf("%w\n\nreplicationcontrollers is a second resource beside configmaps, served at the same shape of URL: what a configmap can do, it can do", err)
	}
	if created["kind"] != "ReplicationController" {
		return fmt.Errorf("POST %s answered kind %v rather than ReplicationController: each resource stores and answers its own kind", rcs, created["kind"])
	}
	if n, ok := scaleReplicas(created, "spec"); !ok || n != 1 {
		return fmt.Errorf("the created controller has spec.replicas %v, and the body named none: an absent replica count defaults to 1, so a controller nobody sized still runs something", scaleHalf(created, "spec")["replicas"])
	}
	if selector := scaleHalf(created, "spec")["selector"]; !reflect.DeepEqual(selector, scaleTemplate()["metadata"].(map[string]any)["labels"]) {
		return fmt.Errorf("the created controller has spec.selector %v, and the body named none: an absent selector is copied from spec.template.metadata.labels, so the controller selects exactly the pods it makes", selector)
	}
	if n, ok := scaleReplicas(created, "status"); !ok || n != 0 {
		return fmt.Errorf("the created controller has status %v, where the body said replicas 7: status is what a controller observed, and nothing has been observed yet — the server sets it to {\"replicas\": 0} whatever a create sends", created["status"])
	}

	// The basics, briefly: everything else a configmap does has been graded on
	// configmaps, and this is the same code over another resource.
	got, err := srv.getJSON(ctx, rc)
	if err != nil {
		return err
	}
	if metaField(got, "uid") != metaField(created, "uid") {
		return fmt.Errorf("GET %s answered uid %q, and the controller was created with %q", rc, metaField(got, "uid"), metaField(created, "uid"))
	}
	for _, path := range []string{rcs, "/api/v1/replicationcontrollers"} {
		names, list, err := srv.names(ctx, path)
		if err != nil {
			return err
		}
		if list["kind"] != "ReplicationControllerList" || len(names) != 1 || names[0] != "web" {
			return fmt.Errorf("GET %s answered kind %v with items %v, where it should be a ReplicationControllerList holding web: a list is named after the kind it holds", path, list["kind"], names)
		}
	}

	// The main endpoint keeps the stored status, as a namespace's did.
	res, body, err := srv.send(ctx, http.MethodPut, rc, map[string]any{
		"apiVersion": "v1",
		"kind":       "ReplicationController",
		"metadata":   map[string]any{"name": "web", "resourceVersion": metaField(created, "resourceVersion"), "labels": map[string]any{"app": "web", "tier": "front"}},
		"spec":       map[string]any{"replicas": 2, "selector": scaleHalf(created, "spec")["selector"], "template": scaleTemplate()},
		"status":     map[string]any{"replicas": 9},
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s answered %d rather than 200\nthe body was:\n%s", rc, res.StatusCode, tail(string(body)))
	}
	updated, err := decode(body)
	if err != nil {
		return fmt.Errorf("the reply to PUT %s is not JSON (%w)\nthe body was:\n%s", rc, err, tail(string(body)))
	}
	if n, _ := scaleReplicas(updated, "status"); n != 0 {
		return fmt.Errorf("after a PUT %s whose body carried status.replicas 9, the controller has status %v: the main endpoint keeps the stored status, as it does for a namespace", rc, updated["status"])
	}
	res, body, err = srv.patch(ctx, rc, "application/merge-patch+json", map[string]any{"status": map[string]any{"replicas": 9}})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PATCH %s answered %d rather than 200\nthe body was:\n%s", rc, res.StatusCode, tail(string(body)))
	}
	kept, err := decode(body)
	if err != nil {
		return fmt.Errorf("the reply to PATCH %s is not JSON (%w)\nthe body was:\n%s", rc, err, tail(string(body)))
	}
	if n, _ := scaleReplicas(kept, "status"); n != 0 {
		return fmt.Errorf("after a merge patch of %s setting status.replicas 9, the controller has status %v: a patch through the main endpoint keeps the stored status too — it is the same endpoint, in fewer words", rc, kept["status"])
	}

	// A status written through /status, so the Scale has a count of its own to
	// report rather than a zero that could be a constant.
	res, body, err = srv.send(ctx, http.MethodPut, rc+"/status", map[string]any{
		"apiVersion": "v1",
		"kind":       "ReplicationController",
		"metadata":   map[string]any{"name": "web"},
		"status":     map[string]any{"replicas": 2},
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s/status answered %d rather than 200: a controller writes what it observed through the status subresource, as for a namespace\nthe body was:\n%s", rc, res.StatusCode, tail(string(body)))
	}
	before, err := srv.getJSON(ctx, rc)
	if err != nil {
		return err
	}
	if n, _ := scaleReplicas(before, "status"); n != 2 {
		return fmt.Errorf("after PUT %s/status with replicas 2 the controller has status %v", rc, before["status"])
	}

	s, err := srv.getJSON(ctx, scale)
	if err != nil {
		return fmt.Errorf("%w\n\nthe scale subresource is how anything that resizes workloads reads one: the HPA and kubectl scale know the Scale shape, and nothing about a ReplicationController", err)
	}
	if err := scaleWant(s, "GET "+scale, before, 2, 2); err != nil {
		return err
	}
	// Read many times: a selector built by ranging over a map comes out in
	// whatever order the map gives, which one read can happen to get right.
	for i := 0; i < 20; i++ {
		if i > 0 {
			if s, err = srv.getJSON(ctx, scale); err != nil {
				return err
			}
		}
		if sel, _ := scaleHalf(s, "status")["selector"].(string); sel != "app=web,env=prod,tier=front" {
			return fmt.Errorf("GET %s answered status.selector %q on read %d, where the controller selects %v: a Scale carries the selector as one label-selector string, k=v pairs sorted by key and joined with commas, so the HPA can list the pods it is scaling with ?labelSelector= — and sorted, so the same selector is always the same string", scale, sel, i+1, scaleHalf(before, "spec")["selector"])
		}
	}

	// Opened from the controller's current version, so the next event is the
	// scale write and nothing earlier.
	w, err := srv.watch(ctx, rcs+"?watch=true&resourceVersion="+url.QueryEscape(metaField(before, "resourceVersion")))
	if err != nil {
		return err
	}
	defer w.stop()

	// The Scale carries a status and a selector of its own; only the count is
	// taken.
	res, body, err = srv.send(ctx, http.MethodPut, scale, map[string]any{
		"apiVersion": "autoscaling/v1",
		"kind":       "Scale",
		"metadata":   map[string]any{"name": "web", "namespace": ns, "resourceVersion": metaField(before, "resourceVersion")},
		"spec":       map[string]any{"replicas": 3},
		"status":     map[string]any{"replicas": 8, "selector": "app=other"},
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s answered %d rather than 200: writing a Scale is how a controller is resized, and the reply is the new Scale\nthe body was:\n%s", scale, res.StatusCode, tail(string(body)))
	}
	reply, err := decode(body)
	if err != nil {
		return fmt.Errorf("the reply to PUT %s is not JSON (%w)\nthe body was:\n%s", scale, err, tail(string(body)))
	}
	after, err := srv.getJSON(ctx, rc)
	if err != nil {
		return err
	}
	if err := scaleWant(reply, "the reply to PUT "+scale, after, 3, 2); err != nil {
		return fmt.Errorf("%w\n\nthe reply is the Scale of the controller as it now is, resourceVersion included, so the client can send its next write from it", err)
	}
	if metaField(after, "resourceVersion") == metaField(before, "resourceVersion") {
		return fmt.Errorf("the controller is still at resourceVersion %q after PUT %s changed its replica count: a write through a subresource is a write to the object", metaField(before, "resourceVersion"), scale)
	}
	if n, _ := scaleReplicas(after, "spec"); n != 3 {
		return fmt.Errorf("after PUT %s with spec.replicas 3 the controller has spec.replicas %v: the Scale is a view of the controller, and writing it writes the controller", scale, scaleHalf(after, "spec")["replicas"])
	}
	if err := scaleUntouched(before, after, "PUT "+scale); err != nil {
		return err
	}
	kind, obj, err := w.next(ctx, "MODIFIED web for the scale write")
	if err != nil {
		return fmt.Errorf("%w\n\na scale write goes through the same store as any other write, and the controller that makes the pods learns its new count from this event", err)
	}
	if n, _ := scaleReplicas(obj, "spec"); kind != "MODIFIED" || metaField(obj, "name") != "web" || n != 3 {
		return fmt.Errorf("the watch on %s sent %s %s with spec.replicas %v, where MODIFIED web with spec.replicas 3 was expected: the event holds the controller, not the Scale", rcs, kind, metaField(obj, "name"), scaleHalf(obj, "spec")["replicas"])
	}

	// The refusals. None of them may move the controller.
	current := metaField(after, "resourceVersion")
	refusals := []struct {
		what   string
		body   map[string]any
		code   int
		reason string
		why    string
	}{
		{"carrying the resourceVersion from before the last scale", scaleBody("web", metaField(before, "resourceVersion"), 4), http.StatusConflict, "Conflict",
			"a resourceVersion in a Scale is a precondition on the controller, as in any write: two autoscalers acting on the same old read must not both win"},
		{"carrying metadata.name \"other\"", scaleBody("other", "", 4), http.StatusBadRequest, "BadRequest",
			"the name in the body has to agree with the URL: a Scale for one controller sent to another is a client bug, not a resize"},
		{"with spec.replicas -1", scaleBody("web", "", -1), http.StatusUnprocessableEntity, "Invalid",
			"a replica count below zero means nothing, and storing it hands the controller a number it cannot act on"},
		{"with no spec.replicas", scaleBody("web", "", nil), http.StatusUnprocessableEntity, "Invalid",
			"the count is the only thing a Scale writes; a Scale without one is not a request to scale to zero"},
		{"with spec.replicas 2.5", scaleBody("web", "", 2.5), http.StatusUnprocessableEntity, "Invalid",
			"a replica count is a whole number of pods; 2.5 is refused, not rounded"},
		{"with spec.replicas \"three\"", scaleBody("web", "", "three"), http.StatusUnprocessableEntity, "Invalid",
			"a replica count is an integer, and a body that says otherwise is refused field by field as 422 rather than stored"},
	}
	for _, r := range refusals {
		res, body, err := srv.send(ctx, http.MethodPut, scale, r.body)
		if err != nil {
			return err
		}
		if err := wantStatus("PUT "+scale+" "+r.what, res, body, r.code, r.reason); err != nil {
			return fmt.Errorf("%w\n\n%s", err, r.why)
		}
	}
	got, err = srv.getJSON(ctx, rc)
	if err != nil {
		return err
	}
	if rv := metaField(got, "resourceVersion"); rv != current {
		return fmt.Errorf("after five refused PUTs to %s the controller is at resourceVersion %q rather than %q: a write that was refused has to have left the store alone", scale, rv, current)
	}

	for _, r := range []struct {
		method string
		body   any
	}{{http.MethodGet, nil}, {http.MethodPut, scaleBody("absent", "", 1)}} {
		res, body, err := srv.send(ctx, r.method, rcs+"/absent/scale", r.body)
		if err != nil {
			return err
		}
		if err := wantStatus(r.method+" "+rcs+"/absent/scale, where absent has never been created", res, body, http.StatusNotFound, "NotFound"); err != nil {
			return fmt.Errorf("%w\n\na Scale is a view of a controller, and there is no controller here to view or resize", err)
		}
	}

	// What kubectl scale sends: a merge patch of the Scale, applied to the
	// current Scale and written like a PUT.
	res, body, err = srv.patch(ctx, scale, "application/merge-patch+json", map[string]any{"spec": map[string]any{"replicas": 5}})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PATCH %s with the merge patch {\"spec\":{\"replicas\":5}} answered %d rather than 200: this is exactly what kubectl scale --replicas=5 sends\nthe body was:\n%s", scale, res.StatusCode, tail(string(body)))
	}
	if reply, err = decode(body); err != nil {
		return fmt.Errorf("the reply to PATCH %s is not JSON (%w)\nthe body was:\n%s", scale, err, tail(string(body)))
	}
	patched, err := srv.getJSON(ctx, rc)
	if err != nil {
		return err
	}
	if err := scaleWant(reply, "the reply to PATCH "+scale, patched, 5, 2); err != nil {
		return fmt.Errorf("%w\n\na patch of the Scale is applied to the Scale as it is now, and the result written to the controller like a PUT", err)
	}
	if err := scaleUntouched(before, patched, "PATCH "+scale); err != nil {
		return err
	}
	if err := w.want(ctx, "MODIFIED", "web"); err != nil {
		return err
	}

	// Asking for the count the controller already has changes nothing, so it
	// writes nothing: an autoscaler re-asserting its answer every few seconds
	// must not wake every watcher each time.
	res, body, err = srv.send(ctx, http.MethodPut, scale, scaleBody("web", "", 5))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s with the replica count the controller already has answered %d rather than 200\nthe body was:\n%s", scale, res.StatusCode, tail(string(body)))
	}
	if reply, err = decode(body); err != nil {
		return fmt.Errorf("the reply to PUT %s is not JSON (%w)\nthe body was:\n%s", scale, err, tail(string(body)))
	}
	if rv := metaField(reply, "resourceVersion"); rv != metaField(patched, "resourceVersion") {
		return fmt.Errorf("PUT %s with spec.replicas 5, which the controller already had, moved its resourceVersion from %q to %q: a write that changes nothing is not a write, and is not an event", scale, metaField(patched, "resourceVersion"), rv)
	}

	// The controller is in the namespace, and goes with it.
	res, body, err = srv.send(ctx, http.MethodDelete, "/api/v1/namespaces/"+ns, nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE /api/v1/namespaces/%s answered %d\nthe body was:\n%s", ns, res.StatusCode, tail(string(body)))
	}
	if err := w.want(ctx, "DELETED", "web"); err != nil {
		return fmt.Errorf("%w\n\ndeleting a namespace deletes every resource in it, not only configmaps, and each one is an event to whoever is watching", err)
	}
	res, body, err = srv.send(ctx, http.MethodGet, rc, nil)
	if err != nil {
		return err
	}
	if err := wantStatus("GET "+rc+" after its namespace was deleted", res, body, http.StatusNotFound, "NotFound"); err != nil {
		return fmt.Errorf("%w\n\nwhat is in a namespace goes with it, whatever resource it is: a namespace delete that knows only about configmaps leaves the second resource behind", err)
	}
	return nil
}

// scaleDiscovery insists /api/v1 lists the controller and both its
// subresources. kubectl scale finds the Scale endpoint through discovery, and
// refuses a resource that lists none.
func scaleDiscovery(ctx context.Context, srv *server) error {
	list, err := srv.getJSON(ctx, "/api/v1")
	if err != nil {
		return err
	}
	entries := map[string]map[string]any{}
	resources, _ := list["resources"].([]any)
	for _, r := range resources {
		if m, ok := r.(map[string]any); ok {
			name, _ := m["name"].(string)
			entries[name] = m
		}
	}
	for _, want := range []struct {
		name, kind, group, version string
		verbs                      []string
	}{
		{"replicationcontrollers", "ReplicationController", "", "", []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
		{"replicationcontrollers/scale", "Scale", "autoscaling", "v1", []string{"get", "patch", "update"}},
		{"replicationcontrollers/status", "ReplicationController", "", "", []string{"get", "update"}},
	} {
		e := entries[want.name]
		if e == nil {
			return fmt.Errorf("GET /api/v1 lists no resource named %s: a client is told a resource and each of its subresources exist through discovery, and nothing else", want.name)
		}
		if namespaced, _ := e["namespaced"].(bool); !namespaced {
			return fmt.Errorf("the %s entry says namespaced: false, and a replication controller lives in a namespace — a subresource is scoped like the resource it belongs to", want.name)
		}
		if e["kind"] != want.kind {
			return fmt.Errorf("the %s entry has kind %v rather than %s: kind is what goes in and comes out of the endpoint", want.name, e["kind"], want.kind)
		}
		if want.group != "" && (e["group"] != want.group || e["version"] != want.version) {
			return fmt.Errorf("the %s entry has group %v and version %v, where the Scale is autoscaling/v1: a subresource can answer a kind from another group, and the entry says which so a client decodes it right", want.name, e["group"], e["version"])
		}
		verbs, _ := e["verbs"].([]any)
		for _, v := range want.verbs {
			if !contains(verbs, v) {
				return fmt.Errorf("the %s entry does not offer the verb %q (it offers %v)", want.name, v, e["verbs"])
			}
		}
	}
	if short, _ := entries["replicationcontrollers"]["shortNames"].([]any); !contains(short, "rc") {
		return fmt.Errorf("the replicationcontrollers entry has shortNames %v, without rc: kubectl get rc works because discovery says rc means this resource", entries["replicationcontrollers"]["shortNames"])
	}
	return nil
}

// scaleTemplate is the pod template every write in the stage carries. Its
// labels have three keys so that an unsorted selector string shows up.
func scaleTemplate() map[string]any {
	return map[string]any{
		"metadata": map[string]any{"labels": map[string]any{"tier": "front", "app": "web", "env": "prod"}},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "web", "image": "nginx"}}},
	}
}

// scaleBody is a Scale as a client sends it. A nil replicas leaves the field
// out; resourceVersion "" sends no precondition.
func scaleBody(name, resourceVersion string, replicas any) map[string]any {
	meta := map[string]any{"name": name}
	if resourceVersion != "" {
		meta["resourceVersion"] = resourceVersion
	}
	spec := map[string]any{}
	if replicas != nil {
		spec["replicas"] = replicas
	}
	return map[string]any{"apiVersion": "autoscaling/v1", "kind": "Scale", "metadata": meta, "spec": spec}
}

func scaleHalf(obj map[string]any, half string) map[string]any {
	m, _ := obj[half].(map[string]any)
	return m
}

// scaleReplicas reads spec.replicas or status.replicas, a JSON number.
func scaleReplicas(obj map[string]any, half string) (float64, bool) {
	n, ok := scaleHalf(obj, half)["replicas"].(float64)
	return n, ok
}

// scaleWant checks a Scale against the controller it is a view of.
func scaleWant(s map[string]any, what string, rc map[string]any, spec, status float64) error {
	if s["kind"] != "Scale" || s["apiVersion"] != "autoscaling/v1" {
		return fmt.Errorf("%s has kind %v and apiVersion %v, where it is a Scale in autoscaling/v1: one shape for every scalable resource is the point of the subresource", what, s["kind"], s["apiVersion"])
	}
	for _, field := range []string{"name", "namespace", "uid", "resourceVersion", "creationTimestamp"} {
		if got, want := metaField(s, field), metaField(rc, field); got != want {
			return fmt.Errorf("%s has metadata.%s %q, and the controller has %q: a Scale's metadata is the controller's own, so a client can send its resourceVersion back as a precondition", what, field, got, want)
		}
	}
	if n, ok := scaleReplicas(s, "spec"); !ok || n != spec {
		return fmt.Errorf("%s has spec %v, where spec.replicas should be %v: the Scale's spec.replicas is the controller's spec.replicas", what, s["spec"], spec)
	}
	if n, ok := scaleReplicas(s, "status"); !ok || n != status {
		return fmt.Errorf("%s has status %v, where status.replicas should be %v: the Scale's status.replicas is the controller's status.replicas, what it observed rather than what was asked for", what, s["status"], status)
	}
	return nil
}

// scaleUntouched insists a scale write changed the replica count and nothing
// else a user owns or a controller observed.
func scaleUntouched(before, after map[string]any, what string) error {
	for _, part := range []struct {
		name          string
		before, after any
	}{
		{"metadata.labels", scaleHalf(before, "metadata")["labels"], scaleHalf(after, "metadata")["labels"]},
		{"spec.template", scaleHalf(before, "spec")["template"], scaleHalf(after, "spec")["template"]},
		{"spec.selector", scaleHalf(before, "spec")["selector"], scaleHalf(after, "spec")["selector"]},
		{"status", before["status"], after["status"]},
	} {
		if !reflect.DeepEqual(part.before, part.after) {
			return fmt.Errorf("after %s the controller's %s changed from %v to %v: a Scale writes the replica count and nothing else, which is what lets RBAC grant resizing without granting the template", what, part.name, part.before, part.after)
		}
	}
	return nil
}

// stageOpenAPI checks the OpenAPI v3 document: the schema of every type the
// server serves, which is what kubectl explain prints, what client-side
// validation checks a manifest against, and where a client learns how each
// list in an object merges.
//
// It is read before almost anything else, so it is cached hard. The index
// names one URL per group-version with a hash of the document in it; a URL
// that changes whenever the content does can be cached for ever, and a client
// re-reads only the group-versions whose hash moved.
func stageOpenAPI(ctx context.Context, _ *kube.Env, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	first, err := openapiIndex(ctx, srv)
	if err != nil {
		return err
	}

	res, raw, err := openapiGet(ctx, srv.url+first, "")
	if err != nil {
		return fmt.Errorf("GET %s: %w\nthe program said:\n%s", first, err, tail(srv.p.Stdout()))
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s answered %d: this is the URL GET /openapi/v3 named for api/v1, and a client follows it exactly as given, query and all\nthe body was:\n%s",
			first, res.StatusCode, tail(string(raw)))
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return fmt.Errorf("GET %s answered with Content-Type %q, and the document is JSON", first, ct)
	}
	doc, err := decode(raw)
	if err != nil {
		return fmt.Errorf("GET %s did not answer JSON (%w)\nthe body was:\n%s", first, err, tail(string(raw)))
	}
	if version, _ := doc["openapi"].(string); !strings.HasPrefix(version, "3.") {
		return fmt.Errorf("the document at %s has openapi %v, and this is the v3 endpoint: the field is the OpenAPI version the document is written in, and a client picks its parser by it", first, doc["openapi"])
	}
	if info, _ := doc["info"].(map[string]any); info["title"] == nil || info["version"] == nil {
		return fmt.Errorf("the document at %s has info %v, and an OpenAPI document's info carries a title and a version", first, doc["info"])
	}

	// The hash in the URL is a promise that what it names never changes.
	if cc := res.Header.Get("Cache-Control"); !strings.Contains(cc, "immutable") {
		return fmt.Errorf("GET %s answered Cache-Control %q, and a URL carrying the current hash serves content that cannot change under it: say immutable, and a client keeps its copy until the index names a different hash", first, cc)
	}
	etag := res.Header.Get("ETag")
	if len(etag) < 3 || !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		return fmt.Errorf("GET %s answered ETag %q, and the document needs one, quoted as HTTP writes entity tags: it is what a client sends back to ask whether its copy is still good", first, etag)
	}

	// Without the hash, the same document: the hash is for caching, not for
	// finding it.
	const plain = "/openapi/v3/api/v1"
	res, again, err := openapiGet(ctx, srv.url+plain, "")
	if err != nil {
		return fmt.Errorf("GET %s: %w", plain, err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s answered %d: the document is served with or without ?hash=, and the hash only says which version the client expects\nthe body was:\n%s",
			plain, res.StatusCode, tail(string(again)))
	}
	if got := res.Header.Get("ETag"); got != etag {
		return fmt.Errorf("GET %s answered ETag %s and GET %s answered %s: it is the same document, and its tag is a function of its bytes", plain, got, first, etag)
	}
	if !bytes.Equal(raw, again) {
		return fmt.Errorf("GET %s and GET %s answered different bytes: it is one document, and a hash taken of it has to describe whichever copy a client got", first, plain)
	}

	res, body, err := openapiGet(ctx, srv.url+plain, etag)
	if err != nil {
		return fmt.Errorf("GET %s with If-None-Match: %w", plain, err)
	}
	if res.StatusCode != http.StatusNotModified {
		return fmt.Errorf("GET %s with If-None-Match: %s answered %d, and that is the ETag this server just gave for it: a match is 304 Not Modified, so a client that already has the document is not sent it again", plain, etag, res.StatusCode)
	}
	if len(body) != 0 {
		return fmt.Errorf("the 304 for GET %s carried a %d-byte body, and a 304 has none: the point of it is that the client already holds the bytes", plain, len(body))
	}

	// A hash that is not the document's: what a client still holding an old
	// index would ask for.
	const stale = "/openapi/v3/api/v1?hash=0000"
	if res, _, err = openapiGet(ctx, srv.url+stale, ""); err != nil {
		return fmt.Errorf("GET %s: %w", stale, err)
	}
	if strings.Contains(res.Header.Get("Cache-Control"), "immutable") {
		return fmt.Errorf("GET %s answered Cache-Control %q, and that hash is not the document's: immutable promises the content behind a URL never changes, so it belongs only on the URL carrying the current hash", stale, res.Header.Get("Cache-Control"))
	}

	if err := openapiSchemas(doc); err != nil {
		return err
	}

	paths, _ := doc["paths"].(map[string]any)
	const item = "/api/v1/namespaces/{namespace}/configmaps/{name}"
	ops, _ := paths[item].(map[string]any)
	if ops == nil {
		return fmt.Errorf("the document's paths have no entry %s: paths list every URL the group-version serves, with the namespace and name as {placeholders}", item)
	}
	for _, verb := range []string{"get", "put", "patch", "delete"} {
		if ops[verb] == nil {
			return fmt.Errorf("the paths entry %s has no %s operation: one configmap can be read, replaced, patched and deleted at that URL, and the document says so with one operation per method", item, verb)
		}
	}

	const unknown = "/openapi/v3/apis/nope/v1"
	res, body, err = openapiGet(ctx, srv.url+unknown, "")
	if err != nil {
		return fmt.Errorf("GET %s: %w", unknown, err)
	}
	if res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GET %s answered %d: no group nope is served here, so it has no document, and a client that guessed the URL needs to hear 404 rather than read some other group's schema as this one's\nthe body was:\n%s",
			unknown, res.StatusCode, tail(string(body)))
	}
	if status, err := decode(body); err != nil || status["kind"] != "Status" {
		return fmt.Errorf("the 404 for GET %s is not a Status object: every failure this server reports is one\nthe body was:\n%s", unknown, tail(string(body)))
	}

	second, err := openapiIndex(ctx, srv)
	if err != nil {
		return err
	}
	if second != first {
		return fmt.Errorf("GET /openapi/v3 named %s and then %s with nothing changed in between: the hash is of the document's bytes, so a document that is not built the same way every time — map order, a timestamp — moves the hash and empties every client's cache for nothing", first, second)
	}
	return nil
}

// openapiIndex reads GET /openapi/v3 and returns the URL it names for api/v1.
func openapiIndex(ctx context.Context, srv *server) (string, error) {
	index, err := srv.getJSON(ctx, "/openapi/v3")
	if err != nil {
		return "", fmt.Errorf("%w\n\nthe index is where a client starts: one entry per group-version, each naming the URL of that group-version's document", err)
	}
	paths, _ := index["paths"].(map[string]any)
	entry, _ := paths["api/v1"].(map[string]any)
	link, _ := entry["serverRelativeURL"].(string)
	if !strings.HasPrefix(link, "/") {
		return "", fmt.Errorf("GET /openapi/v3 answered paths %v, with no serverRelativeURL for api/v1: the key is the group-version's path without a leading slash, and its serverRelativeURL is where the document is, with ?hash= on the end", index["paths"])
	}
	return link, nil
}

// openapiGet is a GET that can carry If-None-Match, which request cannot.
func openapiGet(ctx context.Context, url, ifNoneMatch string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("read the response body: %w", err)
	}
	return res, body, nil
}

const openapiObjectMeta = "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"

// openapiSchemas checks the schemas kubectl needs for what this server serves.
func openapiSchemas(doc map[string]any) error {
	components, _ := doc["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	get := func(name string) (map[string]any, error) {
		s, _ := schemas[name].(map[string]any)
		if s == nil {
			return nil, fmt.Errorf("components.schemas has no %s: schemas are named by the Go package and type the real server generates them from, and a client looks them up by that name", name)
		}
		return s, nil
	}

	configMap, err := get("io.k8s.api.core.v1.ConfigMap")
	if err != nil {
		return err
	}
	if err := openapiKind(configMap, "ConfigMap"); err != nil {
		return err
	}
	props, _ := configMap["properties"].(map[string]any)
	rawMeta, _ := props["metadata"].(map[string]any)
	if ref := openapiRef(rawMeta); ref != "#/components/schemas/"+openapiObjectMeta {
		return fmt.Errorf("ConfigMap's metadata property refers to %q, and every object's metadata is the one shared ObjectMeta schema, by $ref to #/components/schemas/%s", ref, openapiObjectMeta)
	}
	for _, check := range []struct{ field, typ, format string }{
		{"apiVersion", "string", ""},
		{"kind", "string", ""},
		{"data", "map", ""},
		{"binaryData", "map", "byte"},
	} {
		if err := openapiType(schemas, configMap, "ConfigMap", check.typ, check.format, check.field); err != nil {
			return err
		}
	}

	namespace, err := get("io.k8s.api.core.v1.Namespace")
	if err != nil {
		return err
	}
	if err := openapiKind(namespace, "Namespace"); err != nil {
		return err
	}
	if openapiProp(schemas, namespace, "spec") == nil {
		return fmt.Errorf("Namespace has no spec property: kubectl explain namespace.spec reads it, and a field the schema does not list is one client-side validation refuses")
	}
	if err := openapiType(schemas, namespace, "Namespace", "string", "", "status", "phase"); err != nil {
		return err
	}

	rc, err := get("io.k8s.api.core.v1.ReplicationController")
	if err != nil {
		return err
	}
	if err := openapiKind(rc, "ReplicationController"); err != nil {
		return err
	}
	for _, check := range []struct {
		typ, format string
		path        []string
	}{
		{"integer", "int32", []string{"spec", "replicas"}},
		{"map", "", []string{"spec", "selector"}},
		{"object", "", []string{"spec", "template"}},
		{"integer", "", []string{"status", "replicas"}},
	} {
		if err := openapiType(schemas, rc, "ReplicationController", check.typ, check.format, check.path...); err != nil {
			return err
		}
	}

	meta, err := get(openapiObjectMeta)
	if err != nil {
		return err
	}
	for _, field := range []string{"name", "namespace", "uid", "resourceVersion", "creationTimestamp"} {
		if err := openapiType(schemas, meta, "ObjectMeta", "string", "", field); err != nil {
			return err
		}
	}
	for _, field := range []string{"labels", "annotations"} {
		if err := openapiType(schemas, meta, "ObjectMeta", "map", "", field); err != nil {
			return err
		}
	}
	for _, field := range []string{"finalizers", "ownerReferences", "managedFields"} {
		if err := openapiType(schemas, meta, "ObjectMeta", "array", "", field); err != nil {
			return err
		}
	}
	finalizers := openapiProp(schemas, meta, "finalizers")
	if items, _ := finalizers["items"].(map[string]any); items["type"] != "string" {
		return fmt.Errorf("ObjectMeta.finalizers has items %v, and a finalizer is a string", finalizers["items"])
	}

	// These two are the strategies stage 19 merges by. kubectl computes the
	// strategic patch it sends from them, so a schema that disagrees with the
	// server has kubectl building patches the server then reads another way.
	if finalizers["x-kubernetes-patch-strategy"] != "merge" {
		return fmt.Errorf("ObjectMeta.finalizers has x-kubernetes-patch-strategy %v, and this server merges finalizers as a set (stage 19): the schema has to say \"merge\", because it is where kubectl learns that, and it builds its patches from what it learns", finalizers["x-kubernetes-patch-strategy"])
	}
	owners := openapiProp(schemas, meta, "ownerReferences")
	if owners["x-kubernetes-patch-strategy"] != "merge" || owners["x-kubernetes-patch-merge-key"] != "uid" {
		return fmt.Errorf("ObjectMeta.ownerReferences has x-kubernetes-patch-strategy %v and x-kubernetes-patch-merge-key %v, and this server merges ownerReferences by uid (stage 19): the schema has to say strategy \"merge\" with merge key \"uid\", or kubectl's patches replace the list it should be merging into",
			owners["x-kubernetes-patch-strategy"], owners["x-kubernetes-patch-merge-key"])
	}
	return nil
}

// openapiKind checks a top-level schema is an object and names the kind it
// is, which is how kubectl explain maps "configmap" to a schema.
func openapiKind(schema map[string]any, kind string) error {
	if schema["type"] != "object" {
		return fmt.Errorf("the %s schema has type %v, and a kind's schema is type object", kind, schema["type"])
	}
	gvks, _ := schema["x-kubernetes-group-version-kind"].([]any)
	for _, g := range gvks {
		if m, _ := g.(map[string]any); m["group"] == "" && m["version"] == "v1" && m["kind"] == kind {
			return nil
		}
	}
	return fmt.Errorf("the %s schema has x-kubernetes-group-version-kind %v, and it needs [{group: \"\", version: v1, kind: %s}]: a schema is just a name until this ties it to the kind a client is holding — it is how kubectl explain %s finds it",
		kind, schema["x-kubernetes-group-version-kind"], kind, strings.ToLower(kind))
}

// openapiType checks the property at path has the type, and the format when
// one is named. "map" is an object whose additionalProperties are strings,
// with the format on the inner schema.
func openapiType(schemas, root map[string]any, kind, typ, format string, path ...string) error {
	where := kind + "." + strings.Join(path, ".")
	node := openapiProp(schemas, root, path...)
	if node == nil {
		return fmt.Errorf("%s is not in the schema: kubectl explain prints nothing for a field the schema leaves out, and client-side validation refuses a manifest that sets it", where)
	}
	if typ == "map" {
		inner, _ := node["additionalProperties"].(map[string]any)
		if node["type"] != "object" || inner["type"] != "string" {
			return fmt.Errorf("%s has type %v and additionalProperties %v, and it is a map of strings: type object, additionalProperties {type: string} — the keys are the user's, so they cannot be listed as properties", where, node["type"], node["additionalProperties"])
		}
		if format != "" && inner["format"] != format {
			return fmt.Errorf("%s has additionalProperties %v, and its values carry format %q: the format is how a client knows to base64-encode them", where, node["additionalProperties"], format)
		}
		return nil
	}
	if node["type"] != typ {
		return fmt.Errorf("%s has type %v, and it is %s: client-side validation refuses a manifest whose field has the wrong type, by this schema, before it is ever sent", where, node["type"], typ)
	}
	if format != "" && node["format"] != format {
		return fmt.Errorf("%s has format %v, and it is %s %s: the format says how wide a number is, and a client generated from this schema picks its type by it", where, node["format"], typ, format)
	}
	return nil
}

// openapiProp walks properties along path, resolving a $ref at each step.
func openapiProp(schemas, schema map[string]any, path ...string) map[string]any {
	node := schema
	for _, name := range path {
		props, _ := node["properties"].(map[string]any)
		node = openapiResolve(schemas, props[name])
		if node == nil {
			return nil
		}
	}
	return node
}

// openapiRef is the $ref a property points at, written directly or, as the
// real server writes it beside a default, as the single entry of an allOf.
func openapiRef(node map[string]any) string {
	if ref, ok := node["$ref"].(string); ok {
		return ref
	}
	if all, _ := node["allOf"].([]any); len(all) == 1 {
		if m, _ := all[0].(map[string]any); m != nil {
			ref, _ := m["$ref"].(string)
			return ref
		}
	}
	return ""
}

// openapiResolve follows one $ref to #/components/schemas/X.
func openapiResolve(schemas map[string]any, node any) map[string]any {
	m, _ := node.(map[string]any)
	if name, ok := strings.CutPrefix(openapiRef(m), "#/components/schemas/"); ok {
		target, _ := schemas[name].(map[string]any)
		return target
	}
	return m
}

// The token file this stage hands the program. alice's groups are the quoted
// fourth column, bob has none, and the blank line is there to be skipped.
const authnTokens = `alice-token-0001,alice,1001,"developers,oncall"

bob-token-0002,bob,1002
`

const authnWhoamiPath = "/apis/authentication.k8s.io/v1/selfsubjectreviews"

// stageAuthnToken checks the server can tell who is asking, and refuses to
// answer anybody it cannot name.
//
// Authentication only produces a name and a list of groups. What that name is
// allowed to do is the next question, and a separate one; this stage is about
// the answer being right, and about a request with no good answer never
// reaching a handler at all.
func stageAuthnToken(ctx context.Context, _ *kube.Env, bin string) error {
	if err := authnAnonymous(ctx, bin); err != nil {
		return err
	}
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
		return fmt.Errorf("%w\n\nthis stage passes -token-auth-file <path>, a CSV of token,user,uid[,\"groups\"]: the harness waits on GET /healthz with no token, and the three health endpoints answer whoever asks — a kubelet probing the server has no credentials to send", err)
	}
	defer cleanup()

	for _, path := range []string{"/healthz", "/livez", "/readyz"} {
		res, body, err := authnSend(ctx, srv, http.MethodGet, path, "", nil)
		if err != nil {
			return err
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s with no token answered %d: health is answered without authentication, because what probes it — kubelet, a load balancer — is not a user and has nothing to present\nthe body was:\n%s",
				path, res.StatusCode, tail(string(body)))
		}
	}

	const alice, bob = "Bearer alice-token-0001", "Bearer bob-token-0002"
	const path = "/api/v1/namespaces/default/configmaps"
	res, body, err := authnSend(ctx, srv, http.MethodGet, path, alice, nil)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s with alice's token answered %d rather than 200: a token in the file is a user, and once the server knows who is asking the request goes on exactly as it did before there was a file\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}

	user, err := authnWhoami(ctx, srv, alice)
	if err != nil {
		return err
	}
	if user["username"] != "alice" || user["uid"] != "1001" {
		return fmt.Errorf("alice's token reviewed as username %v, uid %v, and the file says alice and 1001: the second and third columns are the user, as written", user["username"], user["uid"])
	}
	groups, _ := user["groups"].([]any)
	for _, want := range []string{"developers", "oncall", "system:authenticated"} {
		if !contains(groups, want) {
			return fmt.Errorf("alice's groups are %v, missing %q: the fourth column is one quoted field holding a comma-separated list — split the line with encoding/csv, not strings.Split, or the quotes and the comma inside them break it — and every user who got this far is also in system:authenticated",
				user["groups"], want)
		}
	}

	user, err = authnWhoami(ctx, srv, bob)
	if err != nil {
		return err
	}
	groups, _ = user["groups"].([]any)
	if user["username"] != "bob" || !contains(groups, "system:authenticated") {
		return fmt.Errorf("bob's token reviewed as username %v with groups %v: a line with three columns is a user with no groups of their own, who is still in system:authenticated", user["username"], user["groups"])
	}

	// Every way of not being somebody. A token that is the start of a real
	// one is here because a comparison that stops early accepts it.
	for _, auth := range []string{"", "Bearer nobody-knows-this", "Bearer alice-token", "Basic YWxpY2U6c2VjcmV0", "Token alice-token-0001"} {
		what := fmt.Sprintf("GET %s with Authorization %q", path, auth)
		if auth == "" {
			what = "GET " + path + " with no Authorization header"
		}
		res, body, err := authnSend(ctx, srv, http.MethodGet, path, auth, nil)
		if err != nil {
			return err
		}
		if err := wantStatus(what, res, body, http.StatusUnauthorized, "Unauthorized"); err != nil {
			return fmt.Errorf("%w\n\n401 means the server could not tell who is asking, and it is answered before the request reaches anything else: a missing header, an unknown token and a scheme this server does not speak are all the same failure", err)
		}
	}

	// Refused means refused: a write without a token must not have happened
	// with a 401 reported afterwards.
	intruder := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "intruder"}}
	if res, body, err = authnSend(ctx, srv, http.MethodPost, path, "", intruder); err != nil {
		return err
	}
	if err := wantStatus("POST "+path+" with no token", res, body, http.StatusUnauthorized, "Unauthorized"); err != nil {
		return err
	}
	if res, body, err = authnSend(ctx, srv, http.MethodGet, path+"/intruder", alice, nil); err != nil {
		return err
	}
	if res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GET %s/intruder answered %d after a POST of it without a token was refused: authentication runs before the handler, so a refused write stores nothing\nthe body was:\n%s",
			path, res.StatusCode, tail(string(body)))
	}

	if err := authnWatch(ctx, srv, path+"?watch=true", "Bearer nobody-knows-this"); err != nil {
		return err
	}
	// Checked last: a program that does not take the flag at all also exits
	// non-zero here, and the serve above names that failure better.
	return authnBadFile(ctx, bin)
}

// authnAnonymous checks a program started without the flag: nothing is
// refused, and whoever asks is told they are nobody in particular.
func authnAnonymous(ctx context.Context, bin string) error {
	srv, cleanup, err := serve(ctx, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if _, _, err := srv.names(ctx, "/api/v1/namespaces/default/configmaps"); err != nil {
		return fmt.Errorf("%w\n\nthis program was started without -token-auth-file: with no authenticator configured every earlier stage still has to pass, unchanged", err)
	}

	groups, err := srv.getJSON(ctx, "/apis")
	if err != nil {
		return err
	}
	var group map[string]any
	list, _ := groups["groups"].([]any)
	for _, g := range list {
		if m, ok := g.(map[string]any); ok && m["name"] == "authentication.k8s.io" {
			group = m
		}
	}
	if group == nil {
		return fmt.Errorf("GET /apis lists no group named authentication.k8s.io: kubectl auth whoami looks for it there before it builds a request, and a group that is not listed is one a client concludes the server does not have")
	}
	preferred, _ := group["preferredVersion"].(map[string]any)
	if preferred["groupVersion"] != "authentication.k8s.io/v1" {
		return fmt.Errorf("the authentication.k8s.io group prefers %v, and the version it offers is authentication.k8s.io/v1", group["preferredVersion"])
	}

	resources, err := srv.getJSON(ctx, "/apis/authentication.k8s.io/v1")
	if err != nil {
		return err
	}
	var reviews map[string]any
	items, _ := resources["resources"].([]any)
	for _, r := range items {
		if m, ok := r.(map[string]any); ok && m["name"] == "selfsubjectreviews" {
			reviews = m
		}
	}
	if resources["kind"] != "APIResourceList" || reviews == nil {
		return fmt.Errorf("GET /apis/authentication.k8s.io/v1 answered kind %v without a selfsubjectreviews resource: it is an APIResourceList, like /api/v1, listing what the version offers", resources["kind"])
	}
	verbs, _ := reviews["verbs"].([]any)
	if reviews["kind"] != "SelfSubjectReview" || reviews["namespaced"] != false || !contains(verbs, "create") {
		return fmt.Errorf("the selfsubjectreviews entry is %v, and it is kind SelfSubjectReview, not namespaced, with the one verb create: a review is asked for and answered, never stored", reviews)
	}

	user, err := authnWhoami(ctx, srv, "")
	if err != nil {
		return err
	}
	groupsOf, _ := user["groups"].([]any)
	if user["username"] != "system:anonymous" || !contains(groupsOf, "system:unauthenticated") {
		return fmt.Errorf("with no authenticator configured the review says username %v, groups %v, and it is system:anonymous in system:unauthenticated: not knowing who is asking is itself an identity, and a policy can be written against it",
			user["username"], user["groups"])
	}
	return nil
}

// authnBadFile checks a token file the program cannot read stops it before
// it serves.
func authnBadFile(ctx context.Context, bin string) error {
	dir, err := os.MkdirTemp("", "byok8s-apiserver-*")
	if err != nil {
		return fmt.Errorf("make a directory for the token file: %w", err)
	}
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "tokens.csv")
	if err := os.WriteFile(file, []byte("alice-token-0001,alice,1001\ncarol-token-0003,carol\n"), 0o600); err != nil {
		return fmt.Errorf("write the token file: %w", err)
	}

	addr, err := freeAddr()
	if err != nil {
		return err
	}
	p, err := runner.Start(ctx, bin, nil, "-addr", addr, "-token-auth-file", file)
	if err != nil {
		return err
	}
	defer p.Stop(5 * time.Second)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if done, result := p.Exited(); done {
			if result.ExitCode == 0 {
				return fmt.Errorf("given a token file with a two-column line the program exited 0: a file it cannot read is a failure, and the exit code is how whatever started it finds out")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("given a token file whose second line has two columns (carol-token-0003,carol), the program was still running after 15s: a server that skips the lines it cannot parse starts up missing users nobody is told about, so it refuses to start instead\nthe program said:\n%s",
		tail(p.Stdout()))
}

// authnWhoami asks the server who the caller is, as kubectl auth whoami does,
// and returns the userInfo it answers with.
func authnWhoami(ctx context.Context, srv *server, auth string) (map[string]any, error) {
	review := map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"}
	res, body, err := authnSend(ctx, srv, http.MethodPost, authnWhoamiPath, auth, review)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("POST %s answered %d rather than 201: a review is a create, answered with the object filled in\nthe body was:\n%s",
			authnWhoamiPath, res.StatusCode, tail(string(body)))
	}
	obj, err := decode(body)
	if err != nil {
		return nil, fmt.Errorf("the reply to POST %s is not JSON (%w)\nthe body was:\n%s", authnWhoamiPath, err, tail(string(body)))
	}
	if obj["kind"] != "SelfSubjectReview" || obj["apiVersion"] != "authentication.k8s.io/v1" {
		return nil, fmt.Errorf("the reply to POST %s has kind %v, apiVersion %v, and it is the SelfSubjectReview that was sent, from authentication.k8s.io/v1", authnWhoamiPath, obj["kind"], obj["apiVersion"])
	}
	status, _ := obj["status"].(map[string]any)
	user, ok := status["userInfo"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the SelfSubjectReview has no status.userInfo: that is the answer, the username, uid and groups the server decided this request came from\nthe body was:\n%s", tail(string(body)))
	}
	return user, nil
}

// authnWatch opens a watch with a token the server does not know, and
// insists it is refused before the stream starts.
func authnWatch(ctx context.Context, srv *server, path, auth string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.url+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auth)
	res, err := (&http.Client{}).Do(req)
	if err != nil {
		return fmt.Errorf("GET %s with an unknown token: %w\n\na watch with a bad token is refused like any other request, straight away", path, err)
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return fmt.Errorf("GET %s with an unknown token answered 200 and started streaming: a watch is authenticated once, when it opens, so the refusal is a 401 in place of the stream rather than an error somewhere inside it", path)
	}
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	return wantStatus("GET "+path+" with an unknown token", res, body, http.StatusUnauthorized, "Unauthorized")
}

// authnSend is server.send with an Authorization header, which is left off
// when auth is empty.
func authnSend(ctx context.Context, srv *server, method, path, auth string, body any) (*http.Response, []byte, error) {
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
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
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

// patch sends one PATCH in the dialect contentType names. The body is the same
// JSON whichever it is; the header is what says how the server reads it.
func (s *server) patch(ctx context.Context, path, contentType string, body any) (*http.Response, []byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, nil, fmt.Errorf("encode the patch: %w", err)
	}
	res, got, err := requestAs(ctx, http.MethodPatch, s.url+path, contentType, raw)
	if err != nil {
		return nil, nil, fmt.Errorf("PATCH %s: %w\nthe program said:\n%s", path, err, tail(s.p.Stdout()))
	}
	return res, got, nil
}

// request makes one HTTP request and reads the whole answer, which is small
// enough here that streaming it would only hide the body from an error message.
func request(ctx context.Context, method, url string, body []byte) (*http.Response, []byte, error) {
	return requestAs(ctx, method, url, "application/json", body)
}

// requestAs is request with the body's Content-Type named.
func requestAs(ctx context.Context, method, url, contentType string, body []byte) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", contentType)
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
