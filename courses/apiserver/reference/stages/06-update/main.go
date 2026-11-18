// Your API server.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// object is one stored resource. The API server keeps what it was given plus
// the fields it sets itself, and this course keeps them as plain JSON: a
// scheme of Go types is the real thing's answer, and it is not what any of
// these stages are about.
type object map[string]any

// store holds every object, and the counter that orders every write to them.
//
// resourceVersion is the whole of what makes a watch resumable, so it is a
// property of the store rather than of an object: one counter, taken under the
// same lock as the write it describes, is what lets a reader say "everything
// after this" and mean it.
type store struct {
	mu      sync.Mutex
	objects map[string]object // "<namespace>/<name>" -> the stored object
	version int64
}

func newStore() *store { return &store{objects: map[string]object{}} }

// create stores an object that must not be there already, stamping it with
// what only the server can know.
func (s *store) create(namespace, name string, obj object) (object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := namespace + "/" + name
	if _, ok := s.objects[key]; ok {
		return nil, errAlreadyExists
	}
	s.version++

	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta["name"] = name
	meta["namespace"] = namespace
	meta["uid"] = newUID()
	meta["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)
	meta["resourceVersion"] = strconv.FormatInt(s.version, 10)
	obj["metadata"] = meta

	s.objects[key] = obj
	return obj, nil
}

// get returns the object stored under a name in a namespace.
//
// The key is both halves: a name is unique inside its namespace and nowhere
// else, so "settings" in default and "settings" in kube-system are two objects
// that never see each other.
func (s *store) get(namespace, name string) (object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[namespace+"/"+name]
	return obj, ok
}

// list returns every object in a namespace, in name order, together with the
// store's version at the moment it was read.
//
// The version belongs to the list rather than to any object in it: it is the
// point in the store's history this answer describes. A client that lists and
// then watches from that number misses nothing and repeats nothing, which is
// the whole of how an informer stays in step.
//
// The order is the server's to decide, and it is by name. The real thing gets
// that order for free — etcd hands back a key range sorted — and a client that
// prints a list relies on it being the same twice in a row.
func (s *store) list(namespace string) ([]object, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Not a nil slice: an empty collection has to marshal to [] rather than
	// null, because a client reads the field before it knows it is empty.
	items := []object{}
	for key, obj := range s.objects {
		if ns, _, ok := strings.Cut(key, "/"); ok && ns == namespace {
			items = append(items, obj)
		}
	}
	sort.Slice(items, func(i, j int) bool { return metaString(items[i], "name") < metaString(items[j], "name") })
	return items, s.version
}

// update replaces an object that has to be there already, keeping the fields
// identity is made of and refusing a write built on a version that has moved on.
func (s *store) update(namespace, name string, obj object) (object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := namespace + "/" + name
	old, ok := s.objects[key]
	if !ok {
		return nil, errNotFound
	}
	// An empty resourceVersion is a caller saying it does not care what is
	// there; one that disagrees is a caller describing an object that has
	// since been written, and letting it through erases whoever wrote it.
	if rv := metaString(obj, "resourceVersion"); rv != "" && rv != metaString(old, "resourceVersion") {
		return nil, errConflict
	}
	s.version++

	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta["name"] = name
	meta["namespace"] = namespace
	// Identity is the server's and is set once. A client is free to send back
	// a uid and a creation time it invented, and the server is not free to
	// believe either.
	meta["uid"] = metaString(old, "uid")
	meta["creationTimestamp"] = metaString(old, "creationTimestamp")
	meta["resourceVersion"] = strconv.FormatInt(s.version, 10)
	obj["metadata"] = meta

	s.objects[key] = obj
	return obj, nil
}

// metaString reads one metadata field, which is a string or is not there.
func metaString(obj object, field string) string {
	meta, _ := obj["metadata"].(map[string]any)
	s, _ := meta[field].(string)
	return s
}

var (
	errAlreadyExists = errors.New("already exists")
	errNotFound      = errors.New("not found")
	errConflict      = errors.New("the object has been modified")
)

// newUID is the identity the server gives an object, and the reason a name
// reused after a delete is a different object to everything that referenced it.
func newUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// A server that cannot tell two objects apart is worse than one that
		// stops, so this is fatal rather than papered over.
		panic("no randomness for a uid: " + err.Error())
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:16]))
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "the address to serve the API on")
	flag.Parse()

	objects := newStore()

	mux := http.NewServeMux()

	// The three endpoints that say the process is alive, reachable and willing.
	// The real server distinguishes them — livez is "restart me", readyz is
	// "send me traffic" — and answers each with a list of named checks.
	for _, path := range []string{"/healthz", "/livez", "/readyz"} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprintln(w, "ok")
		})
	}

	// Discovery: what this server can do, in the shape a client asks for it.
	// Every client starts here, kubectl included, and builds its map of what
	// exists from these three answers before it sends a single request.
	mux.HandleFunc("GET /api", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":     "APIVersions",
			"versions": []string{"v1"},
		})
	})
	// The group-less core API is /api; everything else lives under /apis, in a
	// named group. There are none yet, and the empty list still has to be
	// there: a client that gets a 404 here decides the server is broken, not
	// that it has no groups.
	mux.HandleFunc("GET /apis", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":       "APIGroupList",
			"apiVersion": "v1",
			"groups":     []any{},
		})
	})
	mux.HandleFunc("GET /api/v1", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":         "APIResourceList",
			"apiVersion":   "v1",
			"groupVersion": "v1",
			"resources": []any{map[string]any{
				"name":         "configmaps",
				"singularName": "configmap",
				"namespaced":   true,
				"kind":         "ConfigMap",
				"verbs":        []string{"create", "delete", "get", "list", "update", "watch"},
				"shortNames":   []string{"cm"},
			}},
		})
	})

	mux.HandleFunc("POST /api/v1/namespaces/{namespace}/configmaps", func(w http.ResponseWriter, r *http.Request) {
		namespace := r.PathValue("namespace")

		obj, ok := decodeObject(w, r, namespace)
		if !ok {
			return
		}
		name := metaString(obj, "name")
		if name == "" {
			// 422 rather than 400: the request was understood and the object
			// it carried is the thing that is wrong.
			writeStatus(w, http.StatusUnprocessableEntity, "Invalid", "ConfigMap in version \"v1\" cannot be handled: metadata.name is required")
			return
		}

		stored, err := objects.create(namespace, name, obj)
		if err != nil {
			writeStatus(w, http.StatusConflict, "AlreadyExists",
				fmt.Sprintf("configmaps %q already exists", name))
			return
		}
		// 201, and the body is what was stored rather than what was sent: the
		// client learns its object's uid and resourceVersion from the reply.
		writeJSON(w, http.StatusCreated, stored)
	})

	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/configmaps", func(w http.ResponseWriter, r *http.Request) {
		items, version := objects.list(r.PathValue("namespace"))
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":       "ConfigMapList",
			"apiVersion": "v1",
			// The list's own resourceVersion, which is not any item's: it is
			// where a watch started from this answer would begin.
			"metadata": map[string]any{"resourceVersion": strconv.FormatInt(version, 10)},
			"items":    items,
		})
	})

	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/configmaps/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		obj, ok := objects.get(r.PathValue("namespace"), name)
		if !ok {
			// The name goes in the message because a person reads that, and
			// the reason goes in the object because a program branches on it.
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("configmaps %q not found", name))
			return
		}
		writeJSON(w, http.StatusOK, obj)
	})

	mux.HandleFunc("PUT /api/v1/namespaces/{namespace}/configmaps/{name}", func(w http.ResponseWriter, r *http.Request) {
		namespace, name := r.PathValue("namespace"), r.PathValue("name")

		obj, ok := decodeObject(w, r, namespace)
		if !ok {
			return
		}
		// The name is in the URL and in the object, and an update is not a
		// rename. Two names in one request is a request that cannot be carried
		// out as asked, whichever one the server picked.
		if got := metaString(obj, "name"); got != "" && got != name {
			writeStatus(w, http.StatusBadRequest, "BadRequest",
				fmt.Sprintf("the name of the object (%q) does not match the name on the URL (%q)", got, name))
			return
		}

		stored, err := objects.update(namespace, name, obj)
		switch {
		case errors.Is(err, errNotFound):
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("configmaps %q not found", name))
			return
		case errors.Is(err, errConflict):
			// The conflict every controller retries on: read it again, apply
			// the change to what is there now, write it back.
			writeStatus(w, http.StatusConflict, "Conflict",
				fmt.Sprintf("Operation cannot be fulfilled on configmaps %q: the object has been modified; please apply your changes to the latest version and try again", name))
			return
		}
		// 200, not 201: a client that asked to update an object it had read
		// would otherwise have to wonder which of the two happened.
		writeJSON(w, http.StatusOK, stored)
	})

	// Anything this server does not serve is a 404 carrying a Status, not an
	// empty body: a client reads the reason out of it.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusNotFound, "NotFound", "the server could not find the requested resource: "+r.URL.Path)
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	fmt.Println("serving on", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve on %s: %w", *addr, err)
	}
	return nil
}

// decodeObject reads one object out of a write request, answering the client
// itself if it cannot, and reports whether the handler should carry on.
//
// A namespace in the body has to agree with the one in the path: the URL is
// what a client was authorized against, and a body that names a different
// namespace is asking to write somewhere nobody checked.
func decodeObject(w http.ResponseWriter, r *http.Request, namespace string) (object, bool) {
	var obj object
	if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "the body is not valid JSON: "+err.Error())
		return nil, false
	}
	if got := metaString(obj, "namespace"); got != "" && got != namespace {
		writeStatus(w, http.StatusBadRequest, "BadRequest",
			fmt.Sprintf("the namespace of the provided object does not match the namespace sent on the request: %q != %q", got, namespace))
		return nil, false
	}
	obj["apiVersion"], obj["kind"] = "v1", "ConfigMap"
	return obj, true
}

// writeJSON sends one object. The content type is not decoration: a client
// that asked for JSON and got text decides the server is broken.
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		fmt.Fprintf(os.Stderr, "write response: %v\n", err)
	}
}

// writeStatus sends the object the API server answers every failure with.
//
// An error from this server is a resource like any other: kind Status, with a
// machine-readable reason and the same code as the HTTP status. It is what
// client-go turns back into a typed error, and what makes IsNotFound(err)
// possible on the other side.
func writeStatus(w http.ResponseWriter, code int, reason, message string) {
	writeJSON(w, code, map[string]any{
		"kind":       "Status",
		"apiVersion": "v1",
		"metadata":   map[string]any{},
		"status":     "Failure",
		"message":    message,
		"reason":     reason,
		"code":       code,
	})
}
