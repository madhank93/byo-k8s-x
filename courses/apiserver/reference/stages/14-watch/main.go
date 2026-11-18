// Your API server.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
	mu       sync.Mutex
	objects  map[string]object // the registry key -> the stored object
	version  int64
	dir      string // where the store is kept; empty keeps it in memory only
	watchers map[int]chan watchEvent
	nextID   int
}

func newStore(dir string) *store {
	return &store{objects: map[string]object{}, dir: dir, watchers: map[int]chan watchEvent{}}
}

// watchEvent is one change, in the shape a watcher is told about it. The
// resource and namespace travel with it because one stream of writes feeds
// every open watch, and each of them wants a different slice of it.
type watchEvent struct {
	Type      string // ADDED, MODIFIED or DELETED
	Object    object
	Resource  string
	Namespace string
	Version   int64
}

// watchFrom opens a watch and takes the snapshot it starts from under the same
// lock.
//
// Both halves together or neither: a snapshot taken before the watcher is
// registered misses whatever is written in between, and one taken after it
// shows an object that is also about to arrive as an event. Holding the lock
// across the pair is what makes "everything now, then everything after" true.
func (s *store) watchFrom(resource, namespace string) (int, chan watchEvent, []object, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := s.nextID
	// Buffered, because a write must not wait on a client's socket. A watcher
	// that fills it is disconnected rather than allowed to hold up the server.
	events := make(chan watchEvent, 64)
	s.watchers[id] = events
	return id, events, s.locked(resource, namespace), s.version
}

// unwatch closes a watch, which is what every handler does on its way out.
func (s *store) unwatch(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if events, ok := s.watchers[id]; ok {
		close(events)
		delete(s.watchers, id)
	}
}

// publish hands one change to every open watch. It is called under the store's
// lock, by the write that made the change, so watchers see writes in the order
// the store applied them.
func (s *store) publish(event watchEvent) {
	for id, events := range s.watchers {
		select {
		case events <- event:
		default:
			// A client that cannot keep up is dropped, not waited for: one
			// slow socket must never be able to stall every write in the
			// server. Its stream ends, and the contract is that it lists
			// again and starts a new watch from there.
			close(events)
			delete(s.watchers, id)
		}
	}
}

// registryKey is where an object lives in the key space, in the layout etcd
// holds a real cluster in: one flat key space, and the path is the query. A
// list is a scan of everything under a prefix, which is the only kind of
// question this store has to be able to answer quickly.
//
// A cluster-scoped resource — a Namespace is one — has no namespace segment,
// which is the whole of what "cluster-scoped" means down here.
func registryKey(resource, namespace, name string) string {
	return listPrefix(resource, namespace) + name
}

// listPrefix is everything a list of this resource in this namespace scans,
// and every key in the store belongs to exactly one of them. An empty
// namespace is a list across all of them, which is what kubectl -A asks for.
func listPrefix(resource, namespace string) string {
	if namespace == "" {
		return "/registry/" + resource + "/"
	}
	return "/registry/" + resource + "/" + namespace + "/"
}

// snapshot is the store as it is written down.
type snapshot struct {
	Version int64             `json:"version"`
	Objects map[string]object `json:"objects"`
}

// load reads the store back at startup.
//
// The version comes back with the objects, and that is the part worth being
// careful about: a counter that restarts at 1 hands out numbers a client has
// already seen, and every watcher holding one is now pointing at a future that
// already happened.
func (s *store) load() error {
	if s.dir == "" {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(s.dir, "store.json"))
	if errors.Is(err, fs.ErrNotExist) {
		// The first run of a server has nothing to read, which is not a
		// failure — it is an empty cluster.
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the store: %w", err)
	}
	var snap snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return fmt.Errorf("the store at %s is not readable: %w", s.dir, err)
	}
	if snap.Objects != nil {
		s.objects = snap.Objects
	}
	s.version = snap.Version
	return nil
}

// write makes a change to the store, on disk first and in memory second.
//
// That order is the whole point of this stage. A create that answered 201 and
// did not survive a restart is a lie, so nothing becomes true in here until it
// is durable — which is the order the real server keeps with etcd as well: it
// writes there, and its own caches follow.
//
// The copy per write is honest at this size and nowhere near how the real one
// works; it is what makes "the disk decides" a single readable step.
func (s *store) write(next map[string]object, version int64) error {
	if s.dir != "" {
		raw, err := json.Marshal(snapshot{Version: version, Objects: next})
		if err != nil {
			return err
		}
		// Written beside the real file and renamed onto it: a rename is
		// atomic, and a half-written store is one nothing can read back at
		// all — worse than the write never having happened.
		tmp := filepath.Join(s.dir, "store.json.tmp")
		if err := os.WriteFile(tmp, raw, 0o600); err != nil {
			return err
		}
		if err := os.Rename(tmp, filepath.Join(s.dir, "store.json")); err != nil {
			return err
		}
	}
	s.objects, s.version = next, version
	return nil
}

// create stores an object that must not be there already, stamping it with
// what only the server can know.
func (s *store) create(resource, namespace, name string, obj object) (object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := registryKey(resource, namespace, name)
	if _, ok := s.objects[key]; ok {
		return nil, errAlreadyExists
	}
	version := s.version + 1

	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta["name"] = name
	if namespace != "" {
		meta["namespace"] = namespace
	}
	meta["uid"] = newUID()
	meta["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)
	meta["resourceVersion"] = strconv.FormatInt(version, 10)
	obj["metadata"] = meta

	next := maps.Clone(s.objects)
	next[key] = obj
	if err := s.write(next, version); err != nil {
		return nil, err
	}
	s.publish(watchEvent{Type: "ADDED", Object: obj, Resource: resource, Namespace: namespace, Version: version})
	return obj, nil
}

// get returns the object stored under a name in a namespace.
//
// The key is both halves: a name is unique inside its namespace and nowhere
// else, so "settings" in default and "settings" in kube-system are two objects
// that never see each other.
func (s *store) get(resource, namespace, name string) (object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[registryKey(resource, namespace, name)]
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
func (s *store) list(resource, namespace string) ([]object, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.locked(resource, namespace), s.version
}

// locked is the scan itself, for the callers that already hold the lock.
func (s *store) locked(resource, namespace string) []object {
	prefix := listPrefix(resource, namespace)
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	// Sorted by key, which is namespace then name: the order etcd hands a
	// range back in, and the order kubectl prints.
	sort.Strings(keys)
	// Not a nil slice: an empty collection has to marshal to [] rather than
	// null, because a client reads the field before it knows it is empty.
	items := []object{}
	for _, key := range keys {
		items = append(items, s.objects[key])
	}
	return items
}

// update replaces an object that has to be there already, keeping the fields
// identity is made of and refusing a write built on a version that has moved on.
func (s *store) update(resource, namespace, name string, obj object) (object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := registryKey(resource, namespace, name)
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
	version := s.version + 1

	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta["name"] = name
	if namespace != "" {
		meta["namespace"] = namespace
	}
	// Identity is the server's and is set once. A client is free to send back
	// a uid and a creation time it invented, and the server is not free to
	// believe either.
	meta["uid"] = metaString(old, "uid")
	meta["creationTimestamp"] = metaString(old, "creationTimestamp")
	meta["resourceVersion"] = strconv.FormatInt(version, 10)
	obj["metadata"] = meta

	next := maps.Clone(s.objects)
	next[key] = obj
	if err := s.write(next, version); err != nil {
		return nil, err
	}
	s.publish(watchEvent{Type: "MODIFIED", Object: obj, Resource: resource, Namespace: namespace, Version: version})
	return obj, nil
}

// remove takes an object back out and returns it as it last was.
func (s *store) remove(resource, namespace, name string) (object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := registryKey(resource, namespace, name)
	old, ok := s.objects[key]
	if !ok {
		return nil, errNotFound
	}
	// A delete is a write like any other, so it moves the counter: a watcher
	// has to be able to place the removal after the create it already saw.
	next := maps.Clone(s.objects)
	delete(next, key)
	// A namespace is not a folder, and deleting one is not a folder delete:
	// the real cluster marks it Terminating and a controller removes what is
	// inside it before the namespace itself goes. Here it is one write, but
	// the rule it stands for is the same — nothing outlives the namespace it
	// was in, or a name freed up comes back pointing at somebody's old data.
	cascaded := []object{}
	if resource == "namespaces" {
		for k := range next {
			if strings.HasPrefix(k, "/registry/configmaps/"+name+"/") {
				cascaded = append(cascaded, next[k])
				delete(next, k)
			}
		}
	}
	version := s.version + 1
	if err := s.write(next, version); err != nil {
		return nil, err
	}
	// What the cascade took with it is a delete like any other to whoever was
	// watching those objects: a controller holding a cache of them has to be
	// told they are gone, and "the namespace went" is not something it sees.
	for _, obj := range cascaded {
		s.publish(watchEvent{Type: "DELETED", Object: obj, Resource: "configmaps", Namespace: name, Version: version})
	}
	s.publish(watchEvent{Type: "DELETED", Object: old, Resource: resource, Namespace: namespace, Version: version})
	return old, nil
}

// bootstrap creates the namespaces a cluster is born with, and only when there
// are none: a restart reads them back rather than making them again.
//
// default is the one every client falls back to when a kubeconfig names none,
// so a server without it answers 404 to the simplest request there is.
func (s *store) bootstrap() error {
	if items, _ := s.list("namespaces", ""); len(items) > 0 {
		return nil
	}
	for _, name := range []string{"default", "kube-system", "kube-public", "kube-node-lease"} {
		_, err := s.create("namespaces", "", name, object{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata":   map[string]any{"name": name},
			"status":     map[string]any{"phase": "Active"},
		})
		if err != nil {
			return fmt.Errorf("create the namespace %s: %w", name, err)
		}
	}
	return nil
}

// currentVersion is how far the store has got, which is what a read asking for
// a version has to be measured against.
func (s *store) currentVersion() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
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
	data := flag.String("data", "", "a directory to keep the objects in; empty keeps them in memory only")
	flag.Parse()

	objects := newStore(*data)
	if err := objects.load(); err != nil {
		return err
	}
	if err := objects.bootstrap(); err != nil {
		return err
	}

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
			"resources": []any{
				map[string]any{
					"name":         "configmaps",
					"singularName": "configmap",
					"namespaced":   true,
					"kind":         "ConfigMap",
					"verbs":        []string{"create", "delete", "get", "list", "update", "watch"},
					"shortNames":   []string{"cm"},
				},
				// namespaced: false is the whole difference, and it is what
				// tells a client to build /api/v1/namespaces/<name> rather
				// than putting a namespace in front of it.
				map[string]any{
					"name":         "namespaces",
					"singularName": "namespace",
					"namespaced":   false,
					"kind":         "Namespace",
					"verbs":        []string{"create", "delete", "get", "list", "watch"},
					"shortNames":   []string{"ns"},
				},
			},
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

		// The namespace has to be there first. It is an object like any other,
		// and writing into one that was never created leaves objects nothing
		// will ever clean up — no quota applies to them, no delete reaches
		// them, and nothing lists them but the namespace nobody made.
		if _, ok := objects.get("namespaces", "", namespace); !ok {
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("namespaces %q not found", namespace))
			return
		}

		stored, err := objects.create("configmaps", namespace, name, obj)
		if errors.Is(err, errAlreadyExists) {
			writeStatus(w, http.StatusConflict, "AlreadyExists",
				fmt.Sprintf("configmaps %q already exists", name))
			return
		}
		// A write that could not be stored is a 500, and saying so is the
		// point: the one thing this server must never do is answer 201 for an
		// object it does not have.
		if err != nil {
			writeStatus(w, http.StatusInternalServerError, "InternalError", "the object could not be stored: "+err.Error())
			return
		}
		// 201, and the body is what was stored rather than what was sent: the
		// client learns its object's uid and resourceVersion from the reply.
		writeJSON(w, http.StatusCreated, stored)
	})

	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/configmaps", listHandler(objects, "configmaps", "ConfigMapList"))

	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/configmaps/{name}", func(w http.ResponseWriter, r *http.Request) {
		if !freshEnough(w, r, objects) {
			return
		}
		name := r.PathValue("name")
		obj, ok := objects.get("configmaps", r.PathValue("namespace"), name)
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

		stored, err := objects.update("configmaps", namespace, name, obj)
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
		case err != nil:
			writeStatus(w, http.StatusInternalServerError, "InternalError", "the object could not be stored: "+err.Error())
			return
		}
		// 200, not 201: a client that asked to update an object it had read
		// would otherwise have to wonder which of the two happened.
		writeJSON(w, http.StatusOK, stored)
	})

	mux.HandleFunc("DELETE /api/v1/namespaces/{namespace}/configmaps/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		removed, err := objects.remove("configmaps", r.PathValue("namespace"), name)
		if err != nil && !errors.Is(err, errNotFound) {
			writeStatus(w, http.StatusInternalServerError, "InternalError", "the object could not be removed: "+err.Error())
			return
		}
		if err != nil {
			// Deleting what is not there is a 404, and a cleanup that runs
			// twice depends on it: "I removed it" and "it was already gone"
			// have to be tellable apart without either being fatal.
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("configmaps %q not found", name))
			return
		}
		// The object as it last was, so the caller learns what it deleted
		// rather than inferring it. The real server answers some resources
		// with a Status saying Success instead; both say the delete happened,
		// and an empty body says nothing.
		writeJSON(w, http.StatusOK, removed)
	})

	// Namespaces are cluster-scoped: no namespace in their URLs, because they
	// are what a namespace in a URL refers to.
	mux.HandleFunc("POST /api/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		obj, ok := decodeObject(w, r, "")
		if !ok {
			return
		}
		name := metaString(obj, "name")
		if name == "" {
			writeStatus(w, http.StatusUnprocessableEntity, "Invalid", "Namespace in version \"v1\" cannot be handled: metadata.name is required")
			return
		}
		obj["apiVersion"], obj["kind"] = "v1", "Namespace"
		// Active is the only phase this server has. The real one also has
		// Terminating, which is a namespace that has been deleted and whose
		// contents are still being removed — writes into it are refused for
		// as long as it lasts.
		obj["status"] = map[string]any{"phase": "Active"}

		stored, err := objects.create("namespaces", "", name, obj)
		if errors.Is(err, errAlreadyExists) {
			writeStatus(w, http.StatusConflict, "AlreadyExists", fmt.Sprintf("namespaces %q already exists", name))
			return
		}
		if err != nil {
			writeStatus(w, http.StatusInternalServerError, "InternalError", "the object could not be stored: "+err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, stored)
	})

	mux.HandleFunc("GET /api/v1/namespaces", listHandler(objects, "namespaces", "NamespaceList"))

	mux.HandleFunc("GET /api/v1/namespaces/{name}", func(w http.ResponseWriter, r *http.Request) {
		if !freshEnough(w, r, objects) {
			return
		}
		name := r.PathValue("name")
		obj, ok := objects.get("namespaces", "", name)
		if !ok {
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("namespaces %q not found", name))
			return
		}
		writeJSON(w, http.StatusOK, obj)
	})

	mux.HandleFunc("DELETE /api/v1/namespaces/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		removed, err := objects.remove("namespaces", "", name)
		if errors.Is(err, errNotFound) {
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("namespaces %q not found", name))
			return
		}
		if err != nil {
			writeStatus(w, http.StatusInternalServerError, "InternalError", "the object could not be removed: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, removed)
	})

	// The same resource with no namespace in the path: every configmap in the
	// cluster, which is what kubectl get cm -A asks for. A client tells the
	// two URLs apart from discovery alone.
	mux.HandleFunc("GET /api/v1/configmaps", listHandler(objects, "configmaps", "ConfigMapList"))

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

// listHandler answers one collection URL: every object of a resource, in a
// namespace or across all of them, narrowed by whatever the client selected.
//
// One function serves both URL shapes. The namespaced one has a {namespace} in
// its pattern and the cluster-wide one does not, and an absent path value is
// the empty string — which is already what the store reads as "every
// namespace". The difference between the two URLs is the path, and nothing else.
func listHandler(objects *store, resource, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !freshEnough(w, r, objects) {
			return
		}
		selected, err := parseSelectors(r)
		if err != nil {
			// A selector the server cannot answer is refused rather than
			// ignored: a client that asked for one object and was handed the
			// whole collection would act on every one of them.
			writeStatus(w, http.StatusBadRequest, "BadRequest", err.Error())
			return
		}

		// A watch is the same question asked once and answered for ever, so
		// it is the same URL with a flag on it — same resource, same
		// namespace, same selectors, a different shape of answer.
		if watching(r) {
			streamWatch(w, r, objects, resource, selected)
			return
		}

		limit, err := pageSize(r.URL.Query().Get("limit"))
		if err != nil {
			writeStatus(w, http.StatusBadRequest, "BadRequest", err.Error())
			return
		}
		token, err := decodeContinue(r.URL.Query().Get("continue"))
		if err != nil {
			writeStatus(w, http.StatusBadRequest, "BadRequest", err.Error())
			return
		}

		all, version := objects.list(resource, r.PathValue("namespace"))
		items := []object{}
		for _, obj := range all {
			if selected(obj) {
				items = append(items, obj)
			}
		}

		// Selecting happens before paging, always: limit is how much of the
		// answer to send, not how much of the store to look at. A page of 10
		// out of a collection of 10,000 that then filters down to nothing
		// would be a client paging forever through empty answers.
		listVersion := version
		if token != nil {
			if token.Version > version {
				// A cursor into a history this server does not have. The real
				// one answers the same way once etcd has compacted the
				// revision the first page was read at, and the client's only
				// move is to start the list again.
				writeStatus(w, http.StatusGone, "Expired",
					fmt.Sprintf("continue parameter is too old to be honoured: %d, current: %d", token.Version, version))
				return
			}
			listVersion = token.Version
			rest := []object{}
			for _, obj := range items {
				if objectKey(resource, obj) > token.Start {
					rest = append(rest, obj)
				}
			}
			items = rest
		}

		meta := map[string]any{"resourceVersion": strconv.FormatInt(listVersion, 10)}
		if limit > 0 && len(items) > limit {
			// There is more, so the answer carries the cursor to it — and
			// only then. An empty continue on the last page is what tells a
			// client to stop, and a client that is handed one forever pages
			// forever.
			meta["continue"] = continueToken{Version: listVersion, Start: objectKey(resource, items[limit-1])}.encode()
			// A hint rather than a promise: it is what was left when this page
			// was cut, and kubectl prints it as "(N remaining)".
			meta["remainingItemCount"] = len(items) - limit
			items = items[:limit]
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":       kind,
			"apiVersion": "v1",
			// The list's own resourceVersion, which is not any item's: it is
			// where a watch started from this answer would begin, and it stays
			// the same across every page of one list.
			"metadata": meta,
			"items":    items,
		})
	}
}

// watching reports whether this request is a watch. ?watch=true is what every
// client sends; ?watch=1 is the same thing, which is why it is parsed rather
// than compared.
func watching(r *http.Request) bool {
	want, err := strconv.ParseBool(r.URL.Query().Get("watch"))
	return err == nil && want
}

// streamWatch holds the request open and writes one JSON event per change,
// until the client goes away.
//
// This is the endpoint everything else in Kubernetes is built on. Every
// controller, the scheduler, the kubelet and kube-proxy are all a cache filled
// from one of these and kept in step by it — nothing polls, and that is the
// only reason a cluster of any size works at all.
func streamWatch(w http.ResponseWriter, r *http.Request, objects *store, resource string, selected func(object) bool) {
	namespace := r.PathValue("namespace")
	id, events, initial, _ := objects.watchFrom(resource, namespace)
	defer objects.unwatch(id)

	// 200 and the headers go out before anything has happened, because the
	// client is waiting to learn the watch is open. Nothing here sets a
	// Content-Length: the length is not known, and never will be.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}
	// Flushed straight away, and this line matters more than it looks: Go
	// holds the header back until something is written, so a watch that opens
	// on an empty collection and then waits would leave the client blocked on
	// a response that never starts — a watch that has nothing to say yet is
	// still an open watch.
	flusher.Flush()
	send := func(kind string, obj object) bool {
		raw, err := json.Marshal(map[string]any{"type": kind, "object": obj})
		if err != nil {
			return false
		}
		// One event per line, and flushed: an event still sitting in a buffer
		// is an event the client has not been told about, and a watch that
		// arrives in batches whenever a buffer happens to fill is a watch
		// nothing can react to.
		if _, err := w.Write(append(raw, '\n')); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	// The state as it was when the watch opened, as ADDED events. A watch with
	// no resourceVersion says "I have nothing", so everything there is, is new
	// to it — the next stage is where a client gets to say otherwise.
	for _, obj := range initial {
		if selected(obj) && !send("ADDED", obj) {
			return
		}
	}
	for {
		select {
		case <-r.Context().Done():
			// The client hung up. Nothing to clean but the watcher, and the
			// deferred unwatch has that.
			return
		case event, open := <-events:
			if !open {
				// Dropped for being too slow. The stream ends, and the client
				// lists again and starts over.
				return
			}
			if event.Resource != resource || (namespace != "" && event.Namespace != namespace) {
				continue
			}
			// The same selectors as a list, on the same objects. This is what
			// makes a kubelet's watch proportional to its node rather than to
			// the cluster.
			if !selected(event.Object) {
				continue
			}
			if !send(event.Type, event.Object) {
				return
			}
		}
	}
}

// continueToken is the cursor a paged list hands back: where the next page
// starts, and the point in the store's history every page of this list
// describes.
//
// It is opaque to the client on purpose. A client stores it and sends it back
// untouched, which leaves the server free to change what is in it — the real
// one carries this same pair, a resource version and a key, as base64 JSON.
type continueToken struct {
	Version int64  `json:"rv"`
	Start   string `json:"start"`
}

func (t continueToken) encode() string {
	// Two scalars into JSON cannot fail, and a cursor is not worth an error
	// path that can never be taken.
	raw, _ := json.Marshal(t)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeContinue reads the cursor back, and returns nil for the first page of
// a list, which carries none.
//
// Anything that is not a cursor this server made is refused. Guessing at it
// would restart the list from the beginning, and a client paging through a
// collection would quietly see the first page over and over.
func decodeContinue(raw string) (*continueToken, error) {
	if raw == "" {
		return nil, nil
	}
	blob, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("continue parameter is invalid: %w", err)
	}
	var token continueToken
	if err := json.Unmarshal(blob, &token); err != nil {
		return nil, fmt.Errorf("continue parameter is invalid: %w", err)
	}
	if token.Start == "" || token.Version <= 0 {
		return nil, fmt.Errorf("continue parameter is invalid: %q", raw)
	}
	return &token, nil
}

// pageSize reads ?limit=, where absent means the whole collection.
func pageSize(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		return 0, fmt.Errorf("limit: Invalid value: %q: must be a non-negative integer", raw)
	}
	return limit, nil
}

// objectKey is where a stored object sits in the key space, which is the order
// a list comes back in and therefore the only thing a cursor can point at.
func objectKey(resource string, obj object) string {
	return registryKey(resource, metaString(obj, "namespace"), metaString(obj, "name"))
}

// parseSelectors builds the one test a list is narrowed by. A request can
// carry both selectors, and an object has to pass both to be listed: the
// fields are what the server indexes, the labels are what whoever created the
// object wrote on it.
func parseSelectors(r *http.Request) (func(object) bool, error) {
	query := r.URL.Query()
	fields, err := parseFieldSelector(query.Get("fieldSelector"))
	if err != nil {
		return nil, err
	}
	labels, err := parseLabelSelector(query.Get("labelSelector"))
	if err != nil {
		return nil, err
	}
	return matchAll([]func(object) bool{fields, labels}), nil
}

// parseLabelSelector turns ?labelSelector= into a test on an object's labels.
//
// Labels are not indexed and any of them can be selected on, which is the
// whole difference from a field selector: the value is the client's, so the
// server cannot have a list of the ones it will accept. The cost is a scan,
// and that is why the syntax is richer — set membership and existence as well
// as equality.
//
// This is the selector every controller in Kubernetes is built on. A Service
// finds its pods with one, a Deployment owns its ReplicaSets by one, and
// neither holds a list of names anywhere.
func parseLabelSelector(raw string) (func(object) bool, error) {
	var tests []func(object) bool
	for _, term := range splitSelector(raw) {
		test, err := labelRequirement(strings.TrimSpace(term))
		if err != nil {
			return nil, err
		}
		tests = append(tests, test)
	}
	return matchAll(tests), nil
}

// labelRequirement reads one term of a label selector.
//
// The forms are equality (key=value, key==value, key!=value), set membership
// (key in (a,b), key notin (a,b)) and existence (key, !key). The two negative
// forms — != and notin — match an object that has no such label at all, which
// is the rule people are surprised by and the one that makes "everything not
// in production" mean what it says.
func labelRequirement(term string) (func(object) bool, error) {
	if open := strings.Index(term, "("); open >= 0 {
		head := strings.Fields(term[:open])
		if len(head) != 2 || !strings.HasSuffix(term, ")") {
			return nil, fmt.Errorf("invalid selector: %q", term)
		}
		key, op := head[0], head[1]
		if op != "in" && op != "notin" {
			return nil, fmt.Errorf("invalid selector: %q: expected in or notin", term)
		}
		set := map[string]bool{}
		for _, value := range strings.Split(term[open+1:len(term)-1], ",") {
			set[strings.TrimSpace(value)] = true
		}
		outside := op == "notin"
		return func(obj object) bool {
			value, ok := labelsOf(obj)[key]
			return (ok && set[value]) != outside
		}, nil
	}
	if key, negated := strings.CutPrefix(term, "!"); negated {
		key = strings.TrimSpace(key)
		if err := validLabelKey(key, term); err != nil {
			return nil, err
		}
		return func(obj object) bool {
			_, ok := labelsOf(obj)[key]
			return !ok
		}, nil
	}
	if !strings.ContainsAny(term, "=!") {
		key := term
		if err := validLabelKey(key, term); err != nil {
			return nil, err
		}
		return func(obj object) bool {
			_, ok := labelsOf(obj)[key]
			return ok
		}, nil
	}
	key, op, want, err := requirement(term)
	if err != nil {
		return nil, err
	}
	negated := op == "!="
	return func(obj object) bool {
		value, ok := labelsOf(obj)[key]
		return (ok && value == want) != negated
	}, nil
}

// validLabelKey rejects what cannot be a key, which is what an empty term and
// a stray comma both come through as.
func validLabelKey(key, term string) error {
	if key == "" || strings.ContainsAny(key, " \t") {
		return fmt.Errorf("invalid selector: %q", term)
	}
	return nil
}

// labelsOf reads an object's labels as strings. A label whose value is not a
// string cannot be selected on and is not an error here: the object was stored
// before this stage existed, and a list is not the place to complain about it.
func labelsOf(obj object) map[string]string {
	meta, _ := obj["metadata"].(map[string]any)
	raw, _ := meta["labels"].(map[string]any)
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		if s, ok := value.(string); ok {
			out[key] = s
		}
	}
	return out
}

// parseFieldSelector turns ?fieldSelector= into a test on an object, and
// refuses to guess at a field it cannot answer for.
//
// Only a few fields are selectable, and that is the real server's rule rather
// than a shortcut here: a field selector is answered out of what the registry
// indexes, so anything else is a 400 instead of a scan. Every resource can be
// selected on metadata.name and metadata.namespace; the rest are declared per
// resource — status.phase for pods, spec.nodeName for the kubelet's own watch.
//
// An empty selector matches everything, which is what an absent parameter means.
func parseFieldSelector(raw string) (func(object) bool, error) {
	var tests []func(object) bool
	for _, term := range splitSelector(raw) {
		key, op, value, err := requirement(term)
		if err != nil {
			return nil, err
		}
		if key != "metadata.name" && key != "metadata.namespace" {
			return nil, fmt.Errorf("field label not supported: %s", key)
		}
		field, want, negated := strings.TrimPrefix(key, "metadata."), value, op == "!="
		tests = append(tests, func(obj object) bool {
			return (metaString(obj, field) == want) != negated
		})
	}
	return matchAll(tests), nil
}

// splitSelector cuts a selector into its terms. The comma between them is an
// AND, and there is no OR anywhere in this syntax: a client that wants a union
// makes two requests.
//
// The commas inside a set — key in (a,b) — belong to that term, so the split
// has to know where the parentheses are rather than cutting on every comma.
func splitSelector(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var terms []string
	depth, start := 0, 0
	for i, r := range raw {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				terms = append(terms, raw[start:i])
				start = i + 1
			}
		}
	}
	return append(terms, raw[start:])
}

// requirement splits one term into the field, the operator and the value it is
// compared with.
func requirement(term string) (key, op, value string, err error) {
	// != before ==, and == before =, or the longer operator is read as the
	// shorter one plus a value that starts with a stray character.
	for _, candidate := range []string{"!=", "==", "="} {
		if i := strings.Index(term, candidate); i > 0 {
			return strings.TrimSpace(term[:i]), candidate, strings.TrimSpace(term[i+len(candidate):]), nil
		}
	}
	return "", "", "", fmt.Errorf("invalid selector: %q", term)
}

// matchAll is what the comma means: every term has to hold.
func matchAll(tests []func(object) bool) func(object) bool {
	return func(obj object) bool {
		for _, test := range tests {
			if !test(obj) {
				return false
			}
		}
		return true
	}
}

// freshEnough reads the resourceVersion a client put on a read and decides
// whether this server can answer it, saying why itself when it cannot.
//
// The parameter is a floor, not a filter. It says "do not answer me with a
// cluster older than this one", and what comes back is the object as it is
// now — there is no way to ask this server what something used to look like.
// A client that has just written an object and reads it back through a
// different replica uses this to avoid being told its own write never happened.
func freshEnough(w http.ResponseWriter, r *http.Request, s *store) bool {
	raw := r.URL.Query().Get("resourceVersion")
	if raw == "" {
		return true
	}
	// 0 is the one special value: "whatever you have already, I am not waiting
	// for anything". It is how an informer does its cheap first list.
	want, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || want < 0 {
		writeStatus(w, http.StatusBadRequest, "BadRequest",
			fmt.Sprintf("resourceVersion: Invalid value: %q: must be a non-negative integer", raw))
		return false
	}
	if current := s.currentVersion(); want > current {
		// The client is describing a write this server has not made. In a real
		// cluster it may have read that version from another apiserver a
		// moment ago, so the answer is "ask again", not "you are mistaken".
		writeStatus(w, http.StatusGatewayTimeout, "Timeout",
			fmt.Sprintf("Too large resource version: %d, current: %d", want, current))
		return false
	}
	return true
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
