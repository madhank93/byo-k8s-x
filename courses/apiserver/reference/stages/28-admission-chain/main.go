// Your API server.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	history  []watchEvent // recent changes, for a watch that resumes
	floor    int64        // the oldest version a watch can still resume from
	// published is objects as of the last write, readable without the lock.
	// Every write replaces the map rather than changing it, so a published
	// map never changes; a check made from inside a write reads it.
	published atomic.Pointer[map[string]object]
}

// historyLimit is how far back a resuming watch can reach. It is the whole of
// what this server remembers, and the real one's answer is the same shape:
// etcd keeps a few minutes of revisions and compacts the rest, which is why a
// client that goes away for too long is told to start again rather than served
// a gap.
const historyLimit = 1000

func newStore(dir string) *store {
	empty := map[string]object{}
	s := &store{objects: empty, dir: dir, watchers: map[int]chan watchEvent{}}
	s.published.Store(&empty)
	return s
}

// view is the store as of the last write, without taking the lock: what the
// authorizer reads, since it is asked from inside writes as well as before
// them.
func (s *store) view() map[string]object {
	return *s.published.Load()
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

// watcher is one open watch: the live channel, and whatever the client has to
// be told before it starts.
type watcher struct {
	id      int
	events  chan watchEvent
	version int64        // where the store stood when the watch opened
	initial []object     // the state to send as ADDED — empty for a resuming watch
	replay  []watchEvent // the changes the client missed, oldest first
}

// watchFrom opens a watch and works out what it starts from, under one lock.
//
// Both halves together or neither: a snapshot taken before the watcher is
// registered misses whatever is written in between, and one taken after it
// shows an object that is also about to arrive as an event. Holding the lock
// across the pair is what makes "everything now, then everything after" true.
//
// since is the resourceVersion the client already has, or -1 for a client that
// has nothing and wants the current state first.
func (s *store) watchFrom(resource, namespace string, since int64) (*watcher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if since >= 0 && since < s.floor {
		// The client is asking to resume from a point this server can no
		// longer describe. Serving it from the current state would leave it
		// believing it had missed nothing, so it is told to start again.
		return nil, errExpired
	}
	s.nextID++
	// Buffered, because a write must not wait on a client's socket. A watcher
	// that fills it is disconnected rather than allowed to hold up the server.
	w := &watcher{id: s.nextID, events: make(chan watchEvent, 64), version: s.version}
	s.watchers[w.id] = w.events
	if since < 0 {
		w.initial = s.locked(resource, namespace)
		return w, nil
	}
	for _, event := range s.history {
		if event.Version > since {
			w.replay = append(w.replay, event)
		}
	}
	return w, nil
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
	s.history = append(s.history, event)
	if len(s.history) > historyLimit {
		drop := len(s.history) - historyLimit
		// What is dropped is what can no longer be replayed, so the floor
		// rises to the last change forgotten.
		s.floor = s.history[drop-1].Version
		s.history = append([]watchEvent(nil), s.history[drop:]...)
	}
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
		s.published.Store(&snap.Objects)
	}
	s.version = snap.Version
	// Nothing that happened before this process started can be replayed: the
	// objects were written down, the changes were not. A watch resuming from
	// anything older than this is told so rather than handed a gap, which is
	// the same answer the real server gives after a compaction.
	s.floor = s.version
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
	s.published.Store(&next)
	return nil
}

// create stores an object that must not be there already, stamping it with
// what only the server can know.
func (s *store) create(resource, namespace, name string, obj object) (object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[registryKey(resource, namespace, name)]; ok {
		return nil, errAlreadyExists
	}
	return s.insert(resource, namespace, name, obj)
}

// insert is the write half of create, for the callers that already hold the
// lock and have checked the name is free.
func (s *store) insert(resource, namespace, name string, obj object) (object, error) {
	key := registryKey(resource, namespace, name)
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
	return scan(s.objects, resource, namespace)
}

// scan is every object of a resource in a namespace, in key order.
func scan(objects map[string]object, resource, namespace string) []object {
	prefix := listPrefix(resource, namespace)
	var keys []string
	for key := range objects {
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
		items = append(items, objects[key])
	}
	return items
}

// modify rewrites an object that has to be there already, keeping the fields
// identity is made of and refusing a write built on a version that has moved on.
//
// change is handed what is stored now and called under the lock, which is what
// makes a patch safe to send without a resourceVersion: it is applied to the
// object as it is at the moment of the write, never to a copy read earlier.
// change must not modify the object it is given — that is the store's copy.
//
// admit, when there is one, sees what change produced before it is written;
// see upsert for what that costs.
func (s *store) modify(resource, namespace, name string, change func(old object) (object, error), admit admitFunc) (object, error) {
	obj, _, err := s.upsert(resource, namespace, name, func(old object) (object, error) {
		if old == nil {
			return nil, errNotFound
		}
		return change(old)
	}, admit)
	return obj, err
}

// upsert is modify for a write that may also create, and reports whether it
// did: change is handed nil when nothing is stored under the name. Deciding
// which of the two it is happens under the same lock as the write, or two
// creates of one name both think they are first.
//
// With admission the change cannot be worked out under the lock: a webhook is
// a network call, and a slow one would stall every reader. So change and admit
// run against a snapshot, and the write goes through only if the object is
// still the one they saw — otherwise the whole thing starts again from what is
// there now, the same optimistic loop the real server runs against etcd.
func (s *store) upsert(resource, namespace, name string, change func(old object) (object, error), admit admitFunc) (object, bool, error) {
	if admit == nil {
		return s.commit(resource, namespace, name, change)
	}
	for range 5 {
		old, _ := s.get(resource, namespace, name)
		obj, err := change(old)
		if err != nil {
			return nil, false, err
		}
		if obj, err = admit(obj, old); err != nil {
			return nil, false, err
		}
		stored, created, err := s.commit(resource, namespace, name, func(current object) (object, error) {
			if (current == nil) != (old == nil) || metaString(current, "resourceVersion") != metaString(old, "resourceVersion") {
				return nil, errMoved
			}
			return obj, nil
		})
		if !errors.Is(err, errMoved) {
			return stored, created, err
		}
	}
	return nil, false, errConflict
}

// commit is upsert with change worked out under the lock.
func (s *store) commit(resource, namespace, name string, change func(old object) (object, error)) (object, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := registryKey(resource, namespace, name)
	old, ok := s.objects[key]
	obj, err := change(old)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		stored, err := s.insert(resource, namespace, name, obj)
		return stored, err == nil, err
	}
	// An empty resourceVersion is a caller saying it does not care what is
	// there; one that disagrees is a caller describing an object that has
	// since been written, and letting it through erases whoever wrote it.
	if rv := metaString(obj, "resourceVersion"); rv != "" && rv != metaString(old, "resourceVersion") {
		return nil, false, errConflict
	}

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
	meta["resourceVersion"] = metaString(old, "resourceVersion")
	obj["metadata"] = meta
	// A write that changes nothing is not a write. Moving the version for it
	// would wake every watcher to look at an object that is what it was, and
	// a controller that applies its config on every loop would do that for
	// ever.
	if sameJSON(obj, old) {
		return old, false, nil
	}
	version := s.version + 1
	meta["resourceVersion"] = strconv.FormatInt(version, 10)

	next := maps.Clone(s.objects)
	next[key] = obj
	if err := s.write(next, version); err != nil {
		return nil, false, err
	}
	s.publish(watchEvent{Type: "MODIFIED", Object: obj, Resource: resource, Namespace: namespace, Version: version})
	return obj, false, nil
}

// sameJSON reports whether two objects would be stored as the same bytes.
func sameJSON(a, b object) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

// remove takes an object back out and returns it as it last was. A non-empty
// precondition is a resourceVersion the object must still be at, which is how
// a delete that was admitted is kept to the object that was admitted.
func (s *store) remove(resource, namespace, name, precondition string) (object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := registryKey(resource, namespace, name)
	old, ok := s.objects[key]
	if !ok {
		return nil, errNotFound
	}
	if precondition != "" && precondition != metaString(old, "resourceVersion") {
		return nil, errConflict
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
	cascaded := []watchEvent{}
	if resource == "namespaces" {
		for k, obj := range next {
			// /registry/<resource>/<namespace>/<name>
			parts := strings.SplitN(strings.TrimPrefix(k, "/registry/"), "/", 3)
			if len(parts) == 3 && parts[1] == name {
				cascaded = append(cascaded, watchEvent{Type: "DELETED", Object: obj, Resource: parts[0], Namespace: name})
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
	for _, event := range cascaded {
		event.Version = version
		s.publish(event)
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
	errExpired       = errors.New("too old resource version")
	errInvalidPatch  = errors.New("the patch cannot be applied")
	errMoved         = errors.New("the object changed while the write was being admitted")
)

// clone is a deep copy of an object, or of any part of one, for a change that
// has to be worked out without touching what the store holds.
func clone[T any](v T) T {
	raw, _ := json.Marshal(v)
	var out T
	_ = json.Unmarshal(raw, &out)
	return out
}

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
	tokenFile := flag.String("token-auth-file", "", "a CSV of token,user,uid[,groups] to authenticate bearer tokens against; empty turns authentication off")
	authzMode := flag.String("authorization-mode", "AlwaysAllow", "AlwaysAllow, or RBAC to decide each request from the roles and bindings stored in the server")
	certFile := flag.String("tls-cert-file", "", "the PEM certificate to serve HTTPS with; empty serves plain HTTP")
	keyFile := flag.String("tls-private-key-file", "", "the PEM private key for -tls-cert-file")
	clientCAFile := flag.String("client-ca-file", "", "a PEM bundle of CAs whose client certificates are users; empty accepts none")
	flag.Parse()

	if (*certFile == "") != (*keyFile == "") {
		return errors.New("-tls-cert-file and -tls-private-key-file are given together or not at all")
	}
	var tlsConfig *tls.Config
	if *certFile != "" {
		pair, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			return fmt.Errorf("load the serving certificate: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{pair}}
	}
	if *clientCAFile != "" {
		if tlsConfig == nil {
			return errors.New("-client-ca-file needs -tls-cert-file: a client certificate is only ever presented over TLS")
		}
		pem, err := os.ReadFile(*clientCAFile)
		if err != nil {
			return fmt.Errorf("read the client CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("the client CA file %s holds no PEM certificates", *clientCAFile)
		}
		// A client without a certificate is still let in, to present a token
		// instead; one with a certificate this pool did not sign is not.
		tlsConfig.ClientCAs = pool
		tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
	}

	var tokens map[string]userInfo
	if *tokenFile != "" {
		var err error
		if tokens, err = loadTokens(*tokenFile); err != nil {
			return err
		}
	}

	objects := newStore(*data)
	if err := objects.load(); err != nil {
		return err
	}
	if err := objects.bootstrap(); err != nil {
		return err
	}
	// nil is AlwaysAllow: every request that got past authentication goes on.
	var policy *rbac
	switch *authzMode {
	case "AlwaysAllow":
	case "RBAC":
		policy = &rbac{objects: objects}
		if err := policy.bootstrap(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("-authorization-mode %q: want AlwaysAllow or RBAC", *authzMode)
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
	// named group, each listed with the versions it serves.
	var groups apiGroups
	groups.add("authentication.k8s.io/v1", map[string]any{
		"name":         "selfsubjectreviews",
		"singularName": "selfsubjectreview",
		"namespaced":   false,
		"kind":         "SelfSubjectReview",
		"verbs":        []string{"create"},
	})
	groups.add("authorization.k8s.io/v1", map[string]any{
		"name":         "selfsubjectaccessreviews",
		"singularName": "selfsubjectaccessreview",
		"namespaced":   false,
		"kind":         "SelfSubjectAccessReview",
		"verbs":        []string{"create"},
	})
	for _, t := range namedResources {
		if policy != nil && t.group == rbacGroup {
			t.check = policy.escalation(t)
		}
		serveResource(mux, objects, t)
		groups.add(t.groupVersion(), t.discovery())
	}
	groups.serve(mux)
	// Who the server decided the caller is: what kubectl auth whoami asks,
	// and the first thing to check when a request is refused.
	mux.HandleFunc("POST /apis/authentication.k8s.io/v1/selfsubjectreviews", func(w http.ResponseWriter, r *http.Request) {
		user := r.Context().Value(userKey{}).(userInfo)
		info := map[string]any{"username": user.name, "groups": user.groups}
		if user.uid != "" {
			info["uid"] = user.uid
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"apiVersion": "authentication.k8s.io/v1",
			"kind":       "SelfSubjectReview",
			"metadata":   map[string]any{"creationTimestamp": nil},
			"status":     map[string]any{"userInfo": info},
		})
	})
	// May the caller do this: what kubectl auth can-i asks. The answer is the
	// same decision the server would make on the real request.
	mux.HandleFunc("POST /apis/authorization.k8s.io/v1/selfsubjectaccessreviews", func(w http.ResponseWriter, r *http.Request) {
		var review map[string]any
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil || review == nil {
			writeStatus(w, http.StatusBadRequest, "BadRequest", "the body is not a JSON object")
			return
		}
		attrs, err := reviewAttributes(review, r.Context().Value(userKey{}).(userInfo))
		if err != nil {
			writeStatus(w, http.StatusUnprocessableEntity, "Invalid", err.Error())
			return
		}
		status := map[string]any{"allowed": true}
		if policy != nil {
			allowed, reason := policy.decide(attrs)
			status["allowed"] = allowed
			if reason != "" {
				status["reason"] = reason
			}
		}
		review["apiVersion"], review["kind"] = "authorization.k8s.io/v1", "SelfSubjectAccessReview"
		review["metadata"] = map[string]any{"creationTimestamp": nil}
		review["status"] = status
		writeJSON(w, http.StatusCreated, review)
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
					"verbs":        []string{"create", "delete", "get", "list", "patch", "update", "watch"},
					"shortNames":   []string{"cm"},
				},
				map[string]any{
					"name":         "replicationcontrollers",
					"singularName": "replicationcontroller",
					"namespaced":   true,
					"kind":         "ReplicationController",
					"verbs":        []string{"create", "delete", "get", "list", "patch", "update", "watch"},
					"shortNames":   []string{"rc"},
				},
				map[string]any{
					"name":         "replicationcontrollers/status",
					"singularName": "",
					"namespaced":   true,
					"kind":         "ReplicationController",
					"verbs":        []string{"get", "update"},
				},
				// A subresource can be of another group's kind. Scale is the
				// one shape every scalable resource answers in, which is how
				// an autoscaler scales things it has no schema for.
				map[string]any{
					"name":         "replicationcontrollers/scale",
					"singularName": "",
					"namespaced":   true,
					"group":        "autoscaling",
					"version":      "v1",
					"kind":         "Scale",
					"verbs":        []string{"get", "patch", "update"},
				},
				// namespaced: false is the whole difference, and it is what
				// tells a client to build /api/v1/namespaces/<name> rather
				// than putting a namespace in front of it.
				map[string]any{
					"name":         "namespaces",
					"singularName": "namespace",
					"namespaced":   false,
					"kind":         "Namespace",
					"verbs":        []string{"create", "delete", "get", "list", "update", "watch"},
					"shortNames":   []string{"ns"},
				},
				// A subresource is listed as a resource of its own, which is
				// what lets a role grant it without the object it belongs to.
				map[string]any{
					"name":         "namespaces/status",
					"singularName": "",
					"namespaced":   false,
					"kind":         "Namespace",
					"verbs":        []string{"get", "update"},
				},
			},
		})
	})

	serveResource(mux, objects, resourceType{resource: "configmaps", kind: "ConfigMap"})
	serveResource(mux, objects, replicationControllers)
	serveStatus(mux, objects, replicationControllers)
	serveScale(mux, objects)

	// Namespaces are cluster-scoped: no namespace in their URLs, because they
	// are what a namespace in a URL refers to.
	mux.HandleFunc("POST /api/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		obj, ok := decodeObject(w, r, "", "v1", "Namespace")
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

	mux.HandleFunc("GET /api/v1/namespaces", listHandler(objects, resourceType{resource: "namespaces", kind: "Namespace", clusterScoped: true}))

	getNamespace := func(w http.ResponseWriter, r *http.Request) {
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
	}
	mux.HandleFunc("GET /api/v1/namespaces/{name}", getNamespace)

	mux.HandleFunc("PUT /api/v1/namespaces/{name}", putNamespace(objects, false))
	mux.HandleFunc("GET /api/v1/namespaces/{name}/status", getNamespace)
	mux.HandleFunc("PUT /api/v1/namespaces/{name}/status", putNamespace(objects, true))

	mux.HandleFunc("DELETE /api/v1/namespaces/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		removed, err := objects.remove("namespaces", "", name, "")
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

	serveOpenAPI(mux)

	// Anything this server does not serve is a 404 carrying a Status, not an
	// empty body: a client reads the reason out of it.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusNotFound, "NotFound", "the server could not find the requested resource: "+r.URL.Path)
	})

	srv := &http.Server{Addr: *addr, Handler: authenticate(tokens, *clientCAFile != "", authorize(policy, mux)), TLSConfig: tlsConfig}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	fmt.Println("serving on", *addr)
	serve := srv.ListenAndServe
	if tlsConfig != nil {
		// The certificate is already in TLSConfig, so no file names here.
		serve = func() error { return srv.ListenAndServeTLS("", "") }
	}
	if err := serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve on %s: %w", *addr, err)
	}
	return nil
}

// resourceType is what differs between two resources served the same way: the
// group and version they live in, the names, whether they sit in a namespace,
// what a create fills in, and whether status is a half of the object only its
// own endpoint may write.
//
// An empty group is the core group, served under /api/v1; every other group is
// under /apis/<group>/<version>, and an empty version means v1.
type resourceType struct {
	group, version string
	resource, kind string
	clusterScoped  bool
	defaults       func(obj object)
	hasStatus      bool
	// check refuses a write the caller may make but whose content they may
	// not, which is a question about the body rather than the URL.
	check func(r *http.Request, namespace string, obj object) error
}

// admit runs this type's content check, if it has one, on an object about to
// be stored.
func (t resourceType) admit(r *http.Request, namespace string, obj object) error {
	if t.check == nil {
		return nil
	}
	return t.check(r, namespace, obj)
}

// groupVersion is what an object of this type carries as its apiVersion.
func (t resourceType) groupVersion() string {
	version := cmp.Or(t.version, "v1")
	if t.group == "" {
		return version
	}
	return t.group + "/" + version
}

// root is where this type's group-version is served.
func (t resourceType) root() string {
	if t.group == "" {
		return "/api/" + t.groupVersion()
	}
	return "/apis/" + t.groupVersion()
}

// collection is the URL pattern of the collection a create is posted to.
func (t resourceType) collection() string {
	if t.clusterScoped {
		return t.root() + "/" + t.resource
	}
	return t.root() + "/namespaces/{namespace}/" + t.resource
}

// discovery is this type's entry in its group-version's APIResourceList.
func (t resourceType) discovery() map[string]any {
	return map[string]any{
		"name":         t.resource,
		"singularName": strings.ToLower(t.kind),
		"namespaced":   !t.clusterScoped,
		"kind":         t.kind,
		"verbs":        []string{"create", "delete", "get", "list", "patch", "update", "watch"},
	}
}

// namedResources are the stored resources served outside the core group. Each
// is plain data here: stored, listed and watched like a ConfigMap, and nothing
// acts on what it says.
var namedResources = []resourceType{
	{group: "rbac.authorization.k8s.io", resource: "roles", kind: "Role"},
	{group: "rbac.authorization.k8s.io", resource: "rolebindings", kind: "RoleBinding"},
	{group: "rbac.authorization.k8s.io", resource: "clusterroles", kind: "ClusterRole", clusterScoped: true},
	{group: "rbac.authorization.k8s.io", resource: "clusterrolebindings", kind: "ClusterRoleBinding", clusterScoped: true},
	{group: "admissionregistration.k8s.io", resource: "mutatingwebhookconfigurations", kind: "MutatingWebhookConfiguration", clusterScoped: true},
	{group: "admissionregistration.k8s.io", resource: "validatingwebhookconfigurations", kind: "ValidatingWebhookConfiguration", clusterScoped: true},
}

// apiGroups is discovery for /apis: each group-version served there, in the
// order first added, with the resources it offers. Every group here serves one
// version, so that version is also the one it prefers.
type apiGroups struct {
	order     []string
	resources map[string][]any
}

func (g *apiGroups) add(groupVersion string, entry map[string]any) {
	if g.resources == nil {
		g.resources = map[string][]any{}
	}
	if _, ok := g.resources[groupVersion]; !ok {
		g.order = append(g.order, groupVersion)
	}
	g.resources[groupVersion] = append(g.resources[groupVersion], entry)
}

// serve answers GET /apis and GET /apis/<group>/<version> for every
// group-version added so far.
func (g *apiGroups) serve(mux *http.ServeMux) {
	list := []any{}
	for _, gv := range g.order {
		name, version, _ := strings.Cut(gv, "/")
		v := map[string]any{"groupVersion": gv, "version": version}
		list = append(list, map[string]any{"name": name, "versions": []any{v}, "preferredVersion": v})
		resources := map[string]any{
			"kind":         "APIResourceList",
			"apiVersion":   "v1",
			"groupVersion": gv,
			"resources":    g.resources[gv],
		}
		mux.HandleFunc("GET /apis/"+gv, func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, resources)
		})
	}
	mux.HandleFunc("GET /apis", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"kind": "APIGroupList", "apiVersion": "v1", "groups": list})
	})
}

// replicationControllers is the core group's scalable resource: a count of
// pods and the template to make them from.
var replicationControllers = resourceType{
	resource:  "replicationcontrollers",
	kind:      "ReplicationController",
	hasStatus: true,
	defaults: func(obj object) {
		spec, _ := obj["spec"].(map[string]any)
		if spec == nil {
			spec = map[string]any{}
			obj["spec"] = spec
		}
		if _, ok := spec["replicas"]; !ok {
			// A float, as every number decoded from JSON is, so a stored
			// default compares equal to the same count sent by a client.
			spec["replicas"] = 1.0
		}
		if _, ok := spec["selector"]; !ok {
			template, _ := spec["template"].(map[string]any)
			meta, _ := template["metadata"].(map[string]any)
			if labels, ok := meta["labels"].(map[string]any); ok {
				spec["selector"] = clone(labels)
			}
		}
		// What is running is the controller's to report, never the creator's
		// to claim.
		obj["status"] = map[string]any{"replicas": 0.0}
	},
}

// keepStatus puts back the stored status on a write through the main
// endpoint, for a resource whose status has an endpoint of its own.
func (t resourceType) keepStatus(old, obj object) {
	if !t.hasStatus {
		return
	}
	if status, ok := old["status"]; ok {
		obj["status"] = clone(status)
	} else {
		delete(obj, "status")
	}
}

// serveResource serves one resource: the collection, and each object by name.
// A namespaced resource's collection is per namespace, and is also served
// across all of them.
//
// A cluster-scoped resource's URLs have no {namespace} in them, and an absent
// path value is the empty string — which every handler and the store already
// read as "no namespace".
func serveResource(mux *http.ServeMux, objects *store, t resourceType) {
	collection, item := t.collection(), t.collection()+"/{name}"
	mux.HandleFunc("POST "+collection, func(w http.ResponseWriter, r *http.Request) {
		namespace := r.PathValue("namespace")

		obj, ok := decodeObject(w, r, namespace, t.groupVersion(), t.kind)
		if !ok {
			return
		}
		name := metaString(obj, "name")
		if name == "" {
			// 422 rather than 400: the request was understood and the object
			// it carried is the thing that is wrong.
			writeStatus(w, http.StatusUnprocessableEntity, "Invalid",
				fmt.Sprintf("%s in version %q cannot be handled: metadata.name is required", t.kind, cmp.Or(t.version, "v1")))
			return
		}

		// The namespace has to be there first. It is an object like any other,
		// and writing into one that was never created leaves objects nothing
		// will ever clean up — no quota applies to them, no delete reaches
		// them, and nothing lists them but the namespace nobody made.
		if !namespaceExists(w, objects, t, namespace) {
			return
		}

		if t.defaults != nil {
			t.defaults(obj)
		}
		if err := t.admit(r, namespace, obj); err != nil {
			writeModifyError(w, err, t.resource, name)
			return
		}
		if admit := admitter(objects, r, t, ""); admit != nil {
			var err error
			if obj, err = admit(obj, nil); err != nil {
				writeModifyError(w, err, t.resource, name)
				return
			}
		}
		stored, err := objects.create(t.resource, namespace, name, obj)
		if errors.Is(err, errAlreadyExists) {
			writeStatus(w, http.StatusConflict, "AlreadyExists",
				fmt.Sprintf("%s %q already exists", t.resource, name))
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

	mux.HandleFunc("GET "+collection, listHandler(objects, t))

	mux.HandleFunc("GET "+item, func(w http.ResponseWriter, r *http.Request) {
		if !freshEnough(w, r, objects) {
			return
		}
		name := r.PathValue("name")
		obj, ok := objects.get(t.resource, r.PathValue("namespace"), name)
		if !ok {
			// The name goes in the message because a person reads that, and
			// the reason goes in the object because a program branches on it.
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("%s %q not found", t.resource, name))
			return
		}
		writeJSON(w, http.StatusOK, obj)
	})

	mux.HandleFunc("PUT "+item, func(w http.ResponseWriter, r *http.Request) {
		namespace, name := r.PathValue("namespace"), r.PathValue("name")

		obj, ok := decodeObject(w, r, namespace, t.groupVersion(), t.kind)
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

		stored, err := objects.modify(t.resource, namespace, name, func(old object) (object, error) {
			next := clone(obj)
			t.keepStatus(old, next)
			if err := t.admit(r, namespace, next); err != nil {
				return nil, err
			}
			return trackUpdate(old, next, updater(r)), nil
		}, admitter(objects, r, t, ""))
		if err != nil {
			writeModifyError(w, err, t.resource, name)
			return
		}
		// 200, not 201: a client that asked to update an object it had read
		// would otherwise have to wonder which of the two happened.
		writeJSON(w, http.StatusOK, stored)
	})

	// A patch is an update that says only what changed. The client sends no
	// copy of the object, so it has nothing stale to write back: the change is
	// applied to what is stored when the write happens.
	mux.HandleFunc("PATCH "+item, func(w http.ResponseWriter, r *http.Request) {
		namespace, name := r.PathValue("namespace"), r.PathValue("name")
		if mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mediaType == "application/apply-patch+yaml" {
			serverSideApply(w, r, objects, t, namespace, name)
			return
		}
		apply, ok := decodePatch(w, r)
		if !ok {
			return
		}
		stored, err := objects.modify(t.resource, namespace, name, func(old object) (object, error) {
			obj, err := apply(clone(old))
			if err != nil {
				return nil, err
			}
			obj["apiVersion"], obj["kind"] = t.groupVersion(), t.kind
			t.keepStatus(old, obj)
			if err := t.admit(r, namespace, obj); err != nil {
				return nil, err
			}
			return trackUpdate(old, obj, updater(r)), nil
		}, admitter(objects, r, t, ""))
		if err != nil {
			writeModifyError(w, err, t.resource, name)
			return
		}
		writeJSON(w, http.StatusOK, stored)
	})

	mux.HandleFunc("DELETE "+item, func(w http.ResponseWriter, r *http.Request) {
		namespace, name := r.PathValue("namespace"), r.PathValue("name")
		// A delete is admitted against the object as it is, and then removes
		// that object only: one written in between was never admitted.
		version := ""
		if admit := admitter(objects, r, t, ""); admit != nil {
			old, ok := objects.get(t.resource, namespace, name)
			if !ok {
				writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("%s %q not found", t.resource, name))
				return
			}
			if _, err := admit(nil, old); err != nil {
				writeModifyError(w, err, t.resource, name)
				return
			}
			version = metaString(old, "resourceVersion")
		}
		removed, err := objects.remove(t.resource, namespace, name, version)
		if errors.Is(err, errConflict) {
			writeModifyError(w, err, t.resource, name)
			return
		}
		if err != nil && !errors.Is(err, errNotFound) {
			writeStatus(w, http.StatusInternalServerError, "InternalError", "the object could not be removed: "+err.Error())
			return
		}
		if err != nil {
			// Deleting what is not there is a 404, and a cleanup that runs
			// twice depends on it: "I removed it" and "it was already gone"
			// have to be tellable apart without either being fatal.
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("%s %q not found", t.resource, name))
			return
		}
		// The object as it last was, so the caller learns what it deleted
		// rather than inferring it. The real server answers some resources
		// with a Status saying Success instead; both say the delete happened,
		// and an empty body says nothing.
		writeJSON(w, http.StatusOK, removed)
	})

	// The same resource with no namespace in the path: every object of it in
	// the cluster, which is what kubectl get -A asks for. A client tells the
	// two URLs apart from discovery alone.
	if !t.clusterScoped {
		mux.HandleFunc("GET "+t.root()+"/"+t.resource, listHandler(objects, t))
	}
}

// namespaceExists answers 404 for a write into a namespace that was never
// created, and reports whether the write can go on. A cluster-scoped resource
// is in no namespace, so there is nothing to check.
func namespaceExists(w http.ResponseWriter, objects *store, t resourceType, namespace string) bool {
	if t.clusterScoped {
		return true
	}
	if _, ok := objects.get("namespaces", "", namespace); !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("namespaces %q not found", namespace))
		return false
	}
	return true
}

// serveStatus serves the /status subresource of a resource: read as the whole
// object, written as its status alone.
func serveStatus(mux *http.ServeMux, objects *store, t resourceType) {
	path := t.collection() + "/{name}/status"
	mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		obj, ok := objects.get(t.resource, r.PathValue("namespace"), name)
		if !ok {
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("%s %q not found", t.resource, name))
			return
		}
		writeJSON(w, http.StatusOK, obj)
	})
	mux.HandleFunc("PUT "+path, func(w http.ResponseWriter, r *http.Request) {
		namespace, name := r.PathValue("namespace"), r.PathValue("name")
		obj, ok := decodeObject(w, r, namespace, t.groupVersion(), t.kind)
		if !ok {
			return
		}
		if got := metaString(obj, "name"); got != "" && got != name {
			writeStatus(w, http.StatusBadRequest, "BadRequest",
				fmt.Sprintf("the name of the object (%q) does not match the name on the URL (%q)", got, name))
			return
		}
		stored, err := objects.modify(t.resource, namespace, name, func(old object) (object, error) {
			next := clone(old)
			next["status"] = obj["status"]
			next["metadata"].(map[string]any)["resourceVersion"] = metaString(obj, "resourceVersion")
			return trackUpdate(old, next, updater(r)), nil
		}, admitter(objects, r, t, "status"))
		if err != nil {
			writeModifyError(w, err, t.resource, name)
			return
		}
		writeJSON(w, http.StatusOK, stored)
	})
}

// serveScale serves replicationcontrollers/scale: the replica count of a
// controller, read and written in the autoscaling/v1 Scale shape.
func serveScale(mux *http.ServeMux, objects *store) {
	const resource = "replicationcontrollers"
	path := "/api/v1/namespaces/{namespace}/" + resource + "/{name}/scale"
	write := func(w http.ResponseWriter, r *http.Request, change func(scale object) (object, error)) {
		namespace, name := r.PathValue("namespace"), r.PathValue("name")
		stored, err := objects.modify(resource, namespace, name, func(old object) (object, error) {
			scale, err := change(clone(scaleOf(old)))
			if err != nil {
				return nil, err
			}
			spec, _ := scale["spec"].(map[string]any)
			replicas, ok := spec["replicas"].(float64)
			if !ok || replicas < 0 || replicas != math.Trunc(replicas) {
				return nil, fmt.Errorf("%w: Scale.spec.replicas must be a whole number, zero or more; got %v", errInvalidPatch, spec["replicas"])
			}
			next := clone(old)
			next["spec"].(map[string]any)["replicas"] = replicas
			// The Scale's resourceVersion is the controller's, so a stale one
			// is the same stale read it would be on the object itself.
			next["metadata"].(map[string]any)["resourceVersion"] = metaString(scale, "resourceVersion")
			return trackUpdate(old, next, updater(r)), nil
		}, nil)
		if err != nil {
			writeModifyError(w, err, resource, name)
			return
		}
		writeJSON(w, http.StatusOK, scaleOf(stored))
	}

	mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		obj, ok := objects.get(resource, r.PathValue("namespace"), name)
		if !ok {
			writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("%s %q not found", resource, name))
			return
		}
		writeJSON(w, http.StatusOK, scaleOf(obj))
	})
	mux.HandleFunc("PUT "+path, func(w http.ResponseWriter, r *http.Request) {
		var body object
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body == nil {
			writeStatus(w, http.StatusBadRequest, "BadRequest", "the body is not a JSON object")
			return
		}
		if got := metaString(body, "name"); got != "" && got != r.PathValue("name") {
			writeStatus(w, http.StatusBadRequest, "BadRequest",
				fmt.Sprintf("the name of the object (%q) does not match the name on the URL (%q)", got, r.PathValue("name")))
			return
		}
		write(w, r, func(object) (object, error) { return body, nil })
	})
	mux.HandleFunc("PATCH "+path, func(w http.ResponseWriter, r *http.Request) {
		apply, ok := decodePatch(w, r)
		if !ok {
			return
		}
		write(w, r, apply)
	})
}

// scaleOf is a controller as a Scale: its identity, the count it asks for,
// and the count it has, with its selector as a label selector string.
func scaleOf(rc object) object {
	meta, _ := rc["metadata"].(map[string]any)
	spec, _ := rc["spec"].(map[string]any)
	status, _ := rc["status"].(map[string]any)
	selector, _ := spec["selector"].(map[string]any)
	pairs := []string{}
	for k, v := range selector {
		pairs = append(pairs, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(pairs)
	current, ok := status["replicas"]
	if !ok {
		current = 0
	}
	scaleMeta := map[string]any{}
	for _, field := range []string{"name", "namespace", "uid", "resourceVersion", "creationTimestamp"} {
		scaleMeta[field] = meta[field]
	}
	return object{
		"kind":       "Scale",
		"apiVersion": "autoscaling/v1",
		"metadata":   scaleMeta,
		"spec":       map[string]any{"replicas": spec["replicas"]},
		"status":     map[string]any{"replicas": current, "selector": strings.Join(pairs, ",")},
	}
}

// listHandler answers one collection URL: every object of a resource, in a
// namespace or across all of them, narrowed by whatever the client selected.
//
// One function serves both URL shapes. The namespaced one has a {namespace} in
// its pattern and the cluster-wide one does not, and an absent path value is
// the empty string — which is already what the store reads as "every
// namespace". The difference between the two URLs is the path, and nothing else.
func listHandler(objects *store, t resourceType) http.HandlerFunc {
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
			streamWatch(w, r, objects, t, selected)
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

		all, version := objects.list(t.resource, r.PathValue("namespace"))
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
				if objectKey(t.resource, obj) > token.Start {
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
			meta["continue"] = continueToken{Version: listVersion, Start: objectKey(t.resource, items[limit-1])}.encode()
			// A hint rather than a promise: it is what was left when this page
			// was cut, and kubectl prints it as "(N remaining)".
			meta["remainingItemCount"] = len(items) - limit
			items = items[:limit]
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"kind":       t.kind + "List",
			"apiVersion": t.groupVersion(),
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
func streamWatch(w http.ResponseWriter, r *http.Request, objects *store, t resourceType, selected func(object) bool) {
	namespace := r.PathValue("namespace")
	// The version the client says it already has. Absent and 0 both mean "I
	// have nothing" — 0 is not version zero, it is "whatever you have
	// already, and do not make me wait for it".
	since := int64(-1)
	if raw := r.URL.Query().Get("resourceVersion"); raw != "" && raw != "0" {
		// freshEnough has already refused anything that is not a number, or
		// that is ahead of this server.
		since, _ = strconv.ParseInt(raw, 10, 64)
	}
	watch, err := objects.watchFrom(t.resource, namespace, since)
	if errors.Is(err, errExpired) {
		// The error every informer is written to handle: its cache is too old
		// to be caught up, so it throws the cache away, lists again, and
		// watches from that list's version.
		writeStatus(w, http.StatusGone, "Expired",
			fmt.Sprintf("too old resource version: %d", since))
		return
	}
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, "InternalError", "the watch could not be opened: "+err.Error())
		return
	}
	defer objects.unwatch(watch.id)

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
	send := func(eventType string, obj object) bool {
		raw, err := json.Marshal(map[string]any{"type": eventType, "object": obj})
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
	// to it.
	for _, obj := range watch.initial {
		if selected(obj) && !send("ADDED", obj) {
			return
		}
	}
	// A client that did say what it has gets the changes since instead: no
	// synthetic ADDED for objects it already knows about, and no gap between
	// the list it built its cache from and the stream that keeps it current.
	// This pair — list, then watch from the list's own resourceVersion — is
	// how every informer in Kubernetes stays in step, and it is why the list
	// carries a version of its own at all.
	for _, event := range watch.replay {
		if event.Resource != t.resource || (namespace != "" && event.Namespace != namespace) {
			continue
		}
		if selected(event.Object) && !send(event.Type, event.Object) {
			return
		}
	}
	// Everything the client has been told about, as a version. A bookmark is
	// how that number is kept current when nothing this watch cares about is
	// happening, so it has to count the events actually sent.
	latest := watch.version
	for _, event := range watch.replay {
		if event.Version > latest {
			latest = event.Version
		}
	}

	// Bookmarks are sent only to a client that asked for them. One that did
	// not is a client whose library does not know the type, and an event it
	// cannot decode is worse than no event at all.
	var bookmarks <-chan time.Time
	if allowed, err := strconv.ParseBool(r.URL.Query().Get("allowWatchBookmarks")); err == nil && allowed {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		bookmarks = ticker.C
	}

	for {
		select {
		case <-bookmarks:
			// The store has moved, and none of it was for this client. Without
			// this, a watch on a quiet resource holds a version that ages
			// until it falls off the end of the history — and the reconnect
			// it eventually makes is answered with 410 and a full relist.
			//
			// The object carries a resourceVersion and nothing else: there is
			// no object here, only a promise that the client has seen
			// everything up to this number.
			if current := objects.currentVersion(); current > latest {
				latest = current
				if !send("BOOKMARK", object{
					"apiVersion": t.groupVersion(),
					"kind":       t.kind,
					"metadata":   map[string]any{"resourceVersion": strconv.FormatInt(current, 10)},
				}) {
					return
				}
			}
		case <-r.Context().Done():
			// The client hung up. Nothing to clean but the watcher, and the
			// deferred unwatch has that.
			return
		case event, open := <-watch.events:
			if !open {
				// Dropped for being too slow. The stream ends, and the client
				// lists again and starts over.
				return
			}
			if event.Resource != t.resource || (namespace != "" && event.Namespace != namespace) {
				continue
			}
			// The same selectors as a list, on the same objects. This is what
			// makes a kubelet's watch proportional to its node rather than to
			// the cluster.
			if !selected(event.Object) {
				continue
			}
			latest = event.Version
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
func decodeObject(w http.ResponseWriter, r *http.Request, namespace, apiVersion, kind string) (object, bool) {
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
	obj["apiVersion"], obj["kind"] = apiVersion, kind
	return obj, true
}

// writeModifyError answers a write to an existing object that did not happen.
func writeModifyError(w http.ResponseWriter, err error, resource, name string) {
	var conflicts *applyConflict
	var forbidden *forbiddenError
	var refused *statusError
	switch {
	case errors.As(err, &forbidden):
		writeStatus(w, http.StatusForbidden, "Forbidden", forbidden.message)
	case errors.As(err, &refused):
		writeStatus(w, refused.code, refused.reason, refused.message)
	case errors.As(err, &conflicts):
		writeJSON(w, http.StatusConflict, map[string]any{
			"kind": "Status", "apiVersion": "v1", "metadata": map[string]any{},
			"status": "Failure", "reason": "Conflict", "code": http.StatusConflict,
			"message": conflicts.Error(),
			"details": map[string]any{"name": name, "kind": resource, "causes": conflicts.causes},
		})
	case errors.Is(err, errNotFound):
		writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("%s %q not found", resource, name))
	case errors.Is(err, errConflict):
		// The conflict every controller retries on: read it again, apply the
		// change to what is there now, write it back.
		writeStatus(w, http.StatusConflict, "Conflict",
			fmt.Sprintf("Operation cannot be fulfilled on %s %q: the object has been modified; please apply your changes to the latest version and try again", resource, name))
	case errors.Is(err, errInvalidPatch):
		writeStatus(w, http.StatusUnprocessableEntity, "Invalid", err.Error())
	default:
		writeStatus(w, http.StatusInternalServerError, "InternalError", "the object could not be stored: "+err.Error())
	}
}

// decodePatch reads a PATCH body in the dialect its Content-Type names, and
// returns the change it describes, answering the client itself if it cannot.
//
// The header is the only thing that says how to read the body: the same JSON
// is a different change as a merge patch and as a strategic one, so a type
// this server does not know is refused rather than guessed at.
func decodePatch(w http.ResponseWriter, r *http.Request) (func(object) (object, error), bool) {
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	raw, err := io.ReadAll(r.Body)
	var patch any
	if err == nil {
		err = json.Unmarshal(raw, &patch)
	}
	if err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "the patch is not valid JSON: "+err.Error())
		return nil, false
	}
	switch mediaType {
	case "application/json-patch+json":
		var ops []patchOp
		if err := json.Unmarshal(raw, &ops); err != nil {
			writeStatus(w, http.StatusBadRequest, "BadRequest", "a JSON patch is a list of operations: "+err.Error())
			return nil, false
		}
		return func(obj object) (object, error) {
			patched, err := jsonPatch(map[string]any(obj), ops)
			if err != nil {
				return nil, err
			}
			result, ok := patched.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: the patched document is not an object", errInvalidPatch)
			}
			return result, nil
		}, true
	case "application/strategic-merge-patch+json":
		return func(obj object) (object, error) {
			merged, err := strategicMerge(map[string]any(obj), patch, "")
			if err != nil {
				return nil, err
			}
			result, ok := merged.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: a strategic merge patch of an object has to be an object", errInvalidPatch)
			}
			return result, nil
		}, true
	case "application/merge-patch+json":
		return func(obj object) (object, error) {
			merged, ok := mergePatch(map[string]any(obj), patch).(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: a merge patch of an object has to be an object", errInvalidPatch)
			}
			return merged, nil
		}, true
	}
	writeStatus(w, http.StatusUnsupportedMediaType, "UnsupportedMediaType",
		fmt.Sprintf("the body of the request was in an unknown format %q - accepted media types include: application/json-patch+json, application/merge-patch+json, application/strategic-merge-patch+json, application/apply-patch+yaml", mediaType))
	return nil, false
}

// serverSideApply answers an apply: the client sends the fields it wants to be
// true, under a manager's name, and the server works out what that means
// against what is stored — including what to take away.
//
// What a manager applied last time is the only record of what it meant, so it
// is kept on the object, in metadata.managedFields. A field the manager stops
// mentioning is removed, unless somebody else also owns it: that is how a
// config file can delete a key without anyone writing a delete.
func serverSideApply(w http.ResponseWriter, r *http.Request, objects *store, t resourceType, namespace, name string) {
	manager := r.URL.Query().Get("fieldManager")
	force := r.URL.Query().Get("force") == "true"
	if manager == "" {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "PATCH with application/apply-patch+yaml requires the fieldManager query parameter: ownership is recorded by manager, and an apply without one has nobody to own it")
		return
	}
	var config map[string]any
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil || config == nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "an apply is a whole object, in JSON")
		return
	}
	if config["apiVersion"] == nil || config["kind"] == nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "an applied object has to say its apiVersion and kind")
		return
	}
	if got := metaString(config, "name"); got != "" && got != name {
		writeStatus(w, http.StatusBadRequest, "BadRequest",
			fmt.Sprintf("the name of the object (%q) does not match the name on the URL (%q)", got, name))
		return
	}
	if got := metaString(config, "namespace"); got != "" && got != namespace {
		writeStatus(w, http.StatusBadRequest, "BadRequest",
			fmt.Sprintf("the namespace of the provided object does not match the namespace sent on the request: %q != %q", got, namespace))
		return
	}
	if !namespaceExists(w, objects, t, namespace) {
		return
	}

	applied := map[string][]string{}
	collectLeaves(config, nil, applied)
	stored, created, err := objects.upsert(t.resource, namespace, name, func(old object) (object, error) {
		obj := object{}
		if old != nil {
			obj = clone(old)
		}
		entries := managedEntries(obj)
		mine := slices.IndexFunc(entries, func(e map[string]any) bool { return e["manager"] == manager && e["operation"] == "Apply" })

		// Setting a field somebody else owns to the value it already has is
		// agreeing with them, and both own it after. Setting it to anything
		// else is overruling them, which only force may do.
		conflicts := &applyConflict{}
		for key, path := range applied {
			want, _ := leafValue(config, path)
			if have, ok := leafValue(obj, path); ok && reflect.DeepEqual(have, want) {
				continue
			}
			for i, e := range entries {
				if _, owns := entryLeaves(e)[key]; owns && i != mine {
					conflicts.add(e["manager"].(string), path)
					if force {
						disown(e, key)
					}
				}
			}
		}
		if len(conflicts.causes) > 0 && !force {
			return nil, conflicts
		}
		if mine >= 0 {
			for key, path := range entryLeaves(entries[mine]) {
				if _, still := applied[key]; !still && !ownedByOther(entries, mine, key) {
					removeLeaf(obj, path)
				}
			}
		}
		incoming := clone(config)
		if meta, ok := incoming["metadata"].(map[string]any); ok {
			delete(meta, "managedFields")
		}
		obj = mergePatch(map[string]any(obj), incoming).(map[string]any)
		obj["apiVersion"], obj["kind"] = t.groupVersion(), t.kind
		if old == nil && t.defaults != nil {
			t.defaults(obj)
		} else if old != nil {
			t.keepStatus(old, obj)
		}

		entry := map[string]any{
			"manager": manager, "operation": "Apply", "apiVersion": t.groupVersion(),
			"fieldsType": "FieldsV1", "fieldsV1": leafTree(applied),
			"time": time.Now().UTC().Format(time.RFC3339),
		}
		switch {
		case mine >= 0 && maps.Equal(keysOf(entryLeaves(entries[mine])), keysOf(applied)):
			// The same fields as last time: the entry is left as it was, time
			// and all, so an apply that changes nothing writes nothing.
		case mine >= 0:
			entries[mine] = entry
		default:
			entries = append(entries, entry)
		}
		setManagedEntries(obj, entries)
		if err := t.admit(r, namespace, obj); err != nil {
			return nil, err
		}
		return obj, nil
	}, admitter(objects, r, t, ""))
	if err != nil {
		writeModifyError(w, err, t.resource, name)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, stored)
}

// unowned are the fields that are the server's or the object's identity: they
// are in every config, and owning them would only mean every manager conflicts.
var unowned = map[string]bool{
	`["apiVersion"]`: true, `["kind"]`: true, `["metadata","name"]`: true, `["metadata","namespace"]`: true,
	`["metadata","uid"]`: true, `["metadata","resourceVersion"]`: true,
	`["metadata","creationTimestamp"]`: true, `["metadata","managedFields"]`: true,
}

// collectLeaves records every field a config sets, keyed by its path as JSON
// so a key with a dot in it is never mistaken for two. A map is a container
// and is descended into; anything else, a list included, is one leaf.
func collectLeaves(node any, at []string, out map[string][]string) {
	fields, ok := node.(map[string]any)
	if !ok || (len(fields) == 0 && at != nil) {
		key := pathKey(at)
		if !unowned[key] {
			out[key] = at
		}
		return
	}
	for k, v := range fields {
		collectLeaves(v, append(slices.Clone(at), k), out)
	}
}

func pathKey(path []string) string {
	raw, _ := json.Marshal(path)
	return string(raw)
}

func keysOf(leaves map[string][]string) map[string]bool {
	out := map[string]bool{}
	for k := range leaves {
		out[k] = true
	}
	return out
}

// leafTree is a set of fields in the shape metadata.managedFields keeps them:
// each key prefixed f:, nested as the object is, with {} at every leaf.
func leafTree(leaves map[string][]string) map[string]any {
	tree := map[string]any{}
	for _, path := range leaves {
		node := tree
		for _, k := range path {
			child, _ := node["f:"+k].(map[string]any)
			if child == nil {
				child = map[string]any{}
				node["f:"+k] = child
			}
			node = child
		}
	}
	return tree
}

// entryLeaves reads the fields one managedFields entry owns.
func entryLeaves(entry map[string]any) map[string][]string {
	out := map[string][]string{}
	var walk func(node map[string]any, at []string)
	walk = func(node map[string]any, at []string) {
		if len(node) == 0 && at != nil {
			out[pathKey(at)] = at
			return
		}
		for k, v := range node {
			child, _ := v.(map[string]any)
			walk(child, append(slices.Clone(at), strings.TrimPrefix(k, "f:")))
		}
	}
	tree, _ := entry["fieldsV1"].(map[string]any)
	walk(tree, nil)
	return out
}

func ownedByOther(entries []map[string]any, except int, key string) bool {
	for i, e := range entries {
		if _, ok := entryLeaves(e)[key]; ok && i != except {
			return true
		}
	}
	return false
}

func managedEntries(obj object) []map[string]any {
	meta, _ := obj["metadata"].(map[string]any)
	list, _ := meta["managedFields"].([]any)
	out := []map[string]any{}
	for _, e := range list {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// setManagedEntries stores the entries back, leaving out any that own
// nothing: a manager with no fields is not a manager of this object any more.
func setManagedEntries(obj object, entries []map[string]any) {
	list := []any{}
	for _, e := range entries {
		if len(entryLeaves(e)) > 0 {
			list = append(list, e)
		}
	}
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		obj["metadata"] = meta
	}
	meta["managedFields"] = list
}

// removeLeaf deletes one field from an object, if it is there.
func removeLeaf(obj object, path []string) {
	node := map[string]any(obj)
	for _, k := range path[:len(path)-1] {
		if node, _ = node[k].(map[string]any); node == nil {
			return
		}
	}
	delete(node, path[len(path)-1])
}

// mergePatch applies an RFC 7386 merge patch to target and returns the result.
//
// Objects are merged key by key, and a null removes the key — null is never a
// value that gets stored. Anything else, a list included, replaces what was
// there whole: a merge patch has no way to say which element of a list is
// which, so it does not try.
func mergePatch(target, patch any) any {
	fields, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	merged, ok := target.(map[string]any)
	if !ok {
		merged = map[string]any{}
	}
	for key, value := range fields {
		if value == nil {
			delete(merged, key)
		} else {
			merged[key] = mergePatch(merged[key], value)
		}
	}
	return merged
}

// mergeKeys says how each list a strategic merge patch can reach is merged:
// by the field named, or as a set of plain values where that is "". A list not
// named here is replaced whole, as a merge patch would.
//
// The real server reads this from struct tags on the Go type of every resource
// (patchStrategy and patchMergeKey), which is why a strategic patch works only
// on built-in types and never on a custom resource.
var mergeKeys = map[string]string{
	"metadata.finalizers":      "",
	"metadata.ownerReferences": "uid",
}

// strategicMerge applies a strategic merge patch to target and returns the
// result. It is a merge patch in everything but lists named in mergeKeys, and
// in the directives — keys starting with $ — which say what to take out of a
// list that merges, and are never stored.
func strategicMerge(target, patch any, path string) (any, error) {
	fields, ok := patch.(map[string]any)
	if !ok {
		return patch, nil
	}
	merged, ok := target.(map[string]any)
	if !ok {
		merged = map[string]any{}
	}
	for key, value := range fields {
		at := strings.TrimPrefix(path+"."+key, ".")
		switch {
		case strings.HasPrefix(key, "$deleteFromPrimitiveList/"):
			field := strings.TrimPrefix(key, "$deleteFromPrimitiveList/")
			remove, _ := value.([]any)
			if list, ok := merged[field].([]any); ok {
				merged[field] = slices.DeleteFunc(list, func(v any) bool {
					return slices.ContainsFunc(remove, func(r any) bool { return reflect.DeepEqual(v, r) })
				})
			}
		case strings.HasPrefix(key, "$"):
			// ponytail: $setElementOrder, $retainKeys and $patch on a map are
			// accepted and ignored; order is by arrival. Honour them if a
			// client ever depends on them.
		case value == nil:
			delete(merged, key)
		default:
			list, isList := value.([]any)
			mergeKey, declared := mergeKeys[at]
			var err error
			switch {
			case isList && declared && mergeKey == "":
				merged[key] = mergeSet(merged[key], list)
			case isList && declared:
				merged[key], err = mergeByKey(merged[key], list, mergeKey, at)
			default:
				merged[key], err = strategicMerge(merged[key], value, at)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return merged, nil
}

// mergeSet adds each value the patch lists that is not in the list already,
// after the ones that are.
func mergeSet(target any, patch []any) []any {
	list, _ := target.([]any)
	for _, value := range patch {
		if !slices.ContainsFunc(list, func(v any) bool { return reflect.DeepEqual(v, value) }) {
			list = append(list, value)
		}
	}
	return list
}

// mergeByKey merges each element of the patch into the element of the list
// with the same key, appends it if there is none, and removes the matching
// element instead if it carries $patch: delete.
func mergeByKey(target any, patch []any, key, path string) ([]any, error) {
	list, _ := target.([]any)
	for _, element := range patch {
		fields, _ := element.(map[string]any)
		id, ok := fields[key]
		if !ok {
			return nil, fmt.Errorf("%w: every element of %s needs its %s, which is how it is matched", errInvalidPatch, path, key)
		}
		i := slices.IndexFunc(list, func(v any) bool {
			m, _ := v.(map[string]any)
			return m != nil && reflect.DeepEqual(m[key], id)
		})
		if fields["$patch"] == "delete" {
			if i >= 0 {
				list = slices.Delete(list, i, i+1)
			}
			continue
		}
		var existing any
		if i >= 0 {
			existing = list[i]
		}
		merged, err := strategicMerge(existing, fields, path)
		if err != nil {
			return nil, err
		}
		if i >= 0 {
			list[i] = merged
		} else {
			list = append(list, merged)
		}
	}
	return list, nil
}

// patchOp is one step of an RFC 6902 JSON patch. Value is the operand of add,
// replace and test; From is the source of move and copy.
type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	From  string `json:"from"`
	Value any    `json:"value"`
}

// jsonPatch applies every operation in order and returns the result, or the
// first failure. It works on doc in place, so the caller hands it a copy: a
// patch applies whole or not at all, and a failure halfway leaves that copy
// half-changed.
func jsonPatch(doc any, ops []patchOp) (any, error) {
	for i, op := range ops {
		var err error
		doc, err = applyOp(doc, op)
		if err != nil {
			return nil, fmt.Errorf("%w: operation %d (%s %s): %v", errInvalidPatch, i, op.Op, op.Path, err)
		}
	}
	return doc, nil
}

// applyOp is one operation. Replace, move and copy are defined by the RFC in
// terms of the other three, and are written that way here.
func applyOp(doc any, op patchOp) (any, error) {
	switch op.Op {
	case "add":
		return pointerAdd(doc, op.Path, op.Value)
	case "remove":
		doc, _, err := pointerRemove(doc, op.Path)
		return doc, err
	case "replace":
		doc, _, err := pointerRemove(doc, op.Path)
		if err != nil {
			return nil, err
		}
		return pointerAdd(doc, op.Path, op.Value)
	case "move":
		doc, value, err := pointerRemove(doc, op.From)
		if err != nil {
			return nil, err
		}
		return pointerAdd(doc, op.Path, value)
	case "copy":
		value, err := pointerGet(doc, op.From)
		if err != nil {
			return nil, err
		}
		return pointerAdd(doc, op.Path, clone(value))
	case "test":
		value, err := pointerGet(doc, op.Path)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(value, op.Value) {
			return nil, fmt.Errorf("the value is %v, not %v", value, op.Value)
		}
		return doc, nil
	}
	return nil, fmt.Errorf("unknown op %q", op.Op)
}

// pointerAdd sets the value a JSON pointer names. In an object that adds or
// overwrites the key; in a list it inserts before the index, and "-" appends.
func pointerAdd(doc any, path string, value any) (any, error) {
	return walkPointer(doc, path, func(parent any, key string) (any, error) {
		switch node := parent.(type) {
		case map[string]any:
			node[key] = value
			return node, nil
		case []any:
			if key == "-" {
				return append(node, value), nil
			}
			i, err := listIndex(key, len(node))
			if err != nil {
				return nil, err
			}
			return slices.Insert(node, i, value), nil
		}
		return nil, fmt.Errorf("%s is inside something that is not an object or a list", path)
	})
}

// pointerRemove takes out the value a JSON pointer names, which has to be there.
func pointerRemove(doc any, path string) (any, any, error) {
	var removed any
	doc, err := walkPointer(doc, path, func(parent any, key string) (any, error) {
		switch node := parent.(type) {
		case map[string]any:
			value, ok := node[key]
			if !ok {
				return nil, fmt.Errorf("%s does not exist", path)
			}
			removed = value
			delete(node, key)
			return node, nil
		case []any:
			i, err := listIndex(key, len(node)-1)
			if err != nil {
				return nil, err
			}
			removed = node[i]
			return slices.Delete(node, i, i+1), nil
		}
		return nil, fmt.Errorf("%s is inside something that is not an object or a list", path)
	})
	return doc, removed, err
}

// pointerGet reads the value a JSON pointer names, which has to be there.
func pointerGet(doc any, path string) (any, error) {
	var found any
	_, err := walkPointer(doc, path, func(parent any, key string) (any, error) {
		switch node := parent.(type) {
		case map[string]any:
			value, ok := node[key]
			if !ok {
				return nil, fmt.Errorf("%s does not exist", path)
			}
			found = value
		case []any:
			i, err := listIndex(key, len(node)-1)
			if err != nil {
				return nil, err
			}
			found = node[i]
		default:
			return nil, fmt.Errorf("%s is inside something that is not an object or a list", path)
		}
		return parent, nil
	})
	return found, err
}

// walkPointer follows a JSON pointer down to the container its last token is
// in, and hands that container and token to leaf. What leaf returns is put
// back in the container's place, because appending to a list makes a new one.
//
// A pointer is a list of tokens, each one a key or a list index. / separates
// them, so a / inside a key is written ~1 and a ~ is written ~0 — which is how
// a label named app.kubernetes.io/name is reached at all.
func walkPointer(doc any, path string, leaf func(parent any, key string) (any, error)) (any, error) {
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("the path %q is not a JSON pointer below the root", path)
	}
	tokens := strings.Split(path[1:], "/")
	for i, token := range tokens {
		tokens[i] = strings.NewReplacer("~1", "/", "~0", "~").Replace(token)
	}
	var walk func(node any, tokens []string) (any, error)
	walk = func(node any, tokens []string) (any, error) {
		if len(tokens) == 1 {
			return leaf(node, tokens[0])
		}
		var child any
		switch n := node.(type) {
		case map[string]any:
			value, ok := n[tokens[0]]
			if !ok {
				return nil, fmt.Errorf("%s does not exist", path)
			}
			child = value
		case []any:
			i, err := listIndex(tokens[0], len(n)-1)
			if err != nil {
				return nil, err
			}
			child = n[i]
		default:
			return nil, fmt.Errorf("%s is inside something that is not an object or a list", path)
		}
		child, err := walk(child, tokens[1:])
		if err != nil {
			return nil, err
		}
		switch n := node.(type) {
		case map[string]any:
			n[tokens[0]] = child
		case []any:
			i, _ := strconv.Atoi(tokens[0])
			n[i] = child
		}
		return node, nil
	}
	return walk(doc, tokens)
}

// listIndex reads a list index out of a pointer token, which has to be a
// number from 0 up to highest.
func listIndex(token string, highest int) (int, error) {
	i, err := strconv.Atoi(token)
	if err != nil || i < 0 || i > highest {
		return 0, fmt.Errorf("%q is not an index of this list", token)
	}
	return i, nil
}

// putNamespace answers both writes to a namespace, which differ only in which
// half of the object they may change. The main endpoint keeps the stored
// status; /status keeps everything but the status.
//
// Status is what a controller observed and spec is what a user asked for. Two
// endpoints let a role grant one without the other, and stop a user's PUT of
// an object read a minute ago from erasing what a controller wrote since.
func putNamespace(objects *store, statusOnly bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var obj object
		if err := json.NewDecoder(r.Body).Decode(&obj); err != nil || obj == nil {
			writeStatus(w, http.StatusBadRequest, "BadRequest", "the body is not a JSON object")
			return
		}
		if got := metaString(obj, "name"); got != "" && got != name {
			writeStatus(w, http.StatusBadRequest, "BadRequest",
				fmt.Sprintf("the name of the object (%q) does not match the name on the URL (%q)", got, name))
			return
		}
		stored, err := objects.modify("namespaces", "", name, func(old object) (object, error) {
			next := obj
			if statusOnly {
				next = clone(old)
				next["status"] = obj["status"]
				// The precondition is the client's, whichever half it writes.
				next["metadata"].(map[string]any)["resourceVersion"] = metaString(obj, "resourceVersion")
			} else if status, ok := old["status"]; ok {
				next["status"] = clone(status)
			} else {
				delete(next, "status")
			}
			next["apiVersion"], next["kind"] = "v1", "Namespace"
			return trackUpdate(old, next, updater(r)), nil
		}, nil)
		if err != nil {
			writeModifyError(w, err, "namespaces", name)
			return
		}
		writeJSON(w, http.StatusOK, stored)
	}
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

// applyConflict is an apply refused because it would overrule other managers,
// one cause per field and manager — what a client shows the person who ran it.
type applyConflict struct {
	causes []map[string]any
}

func (c *applyConflict) add(manager string, path []string) {
	c.causes = append(c.causes, map[string]any{
		"type":    "FieldManagerConflict",
		"message": fmt.Sprintf("conflict with %q", manager),
		"field":   "." + strings.Join(path, "."),
	})
}

func (c *applyConflict) Error() string {
	lines := []string{}
	for _, cause := range c.causes {
		lines = append(lines, fmt.Sprintf("%s using v1: %s", cause["message"], cause["field"]))
	}
	sort.Strings(lines)
	return fmt.Sprintf("Apply failed with %d conflict(s): %s", len(lines), strings.Join(lines, "; "))
}

// leafValue reads the value at a path, and whether there is one.
func leafValue(obj map[string]any, path []string) (any, bool) {
	var node any = obj
	for _, k := range path {
		m, ok := node.(map[string]any)
		if !ok {
			return nil, false
		}
		if node, ok = m[k]; !ok {
			return nil, false
		}
	}
	return node, true
}

// disown takes one field out of a managedFields entry.
func disown(entry map[string]any, key string) {
	leaves := entryLeaves(entry)
	delete(leaves, key)
	entry["fieldsV1"] = leafTree(leaves)
}

// updater is who a PUT or a PATCH is on behalf of. A client that does not say
// is still recorded, as somebody, so the fields it changed stop being owned by
// whoever set them before.
func updater(r *http.Request) string {
	if m := r.URL.Query().Get("fieldManager"); m != "" {
		return m
	}
	return "unknown"
}

// trackUpdate records a write that is not an apply: every field whose value it
// changed now belongs to manager alone. An update never conflicts — it is an
// instruction, not a statement of intent — but it does mean the manager who
// applied the old value is told so the next time it applies it.
func trackUpdate(old, obj object, manager string) object {
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		obj["metadata"] = meta
	}
	// A client that never heard of managedFields sends an object without
	// them, and that is not a request to forget who owns what.
	if _, ok := meta["managedFields"]; !ok {
		if oldMeta, _ := old["metadata"].(map[string]any); oldMeta["managedFields"] != nil {
			meta["managedFields"] = clone(oldMeta["managedFields"])
		}
	}
	before, after := map[string][]string{}, map[string][]string{}
	collectLeaves(map[string]any(old), nil, before)
	collectLeaves(map[string]any(obj), nil, after)
	changed := map[string][]string{}
	for key, path := range before {
		if v, ok := leafValue(obj, path); !ok || !reflect.DeepEqual(v, mustLeaf(old, path)) {
			changed[key] = path
		}
	}
	for key, path := range after {
		if _, ok := before[key]; !ok {
			changed[key] = path
		}
	}
	if len(changed) == 0 {
		return obj
	}

	entries := managedEntries(obj)
	for _, e := range entries {
		for key := range changed {
			disown(e, key)
		}
	}
	mine := slices.IndexFunc(entries, func(e map[string]any) bool { return e["manager"] == manager && e["operation"] == "Update" })
	if mine < 0 {
		entries = append(entries, map[string]any{
			"manager": manager, "operation": "Update", "apiVersion": obj["apiVersion"],
			"fieldsType": "FieldsV1", "fieldsV1": map[string]any{},
		})
		mine = len(entries) - 1
	}
	owned := entryLeaves(entries[mine])
	for key, path := range changed {
		if _, ok := after[key]; ok {
			owned[key] = path
		}
	}
	entries[mine]["fieldsV1"] = leafTree(owned)
	entries[mine]["time"] = time.Now().UTC().Format(time.RFC3339)
	setManagedEntries(obj, entries)
	return obj
}

func mustLeaf(obj object, path []string) any {
	v, _ := leafValue(obj, path)
	return v
}

// serveOpenAPI publishes the schema of every resource this server has, in the
// OpenAPI v3 shape kubectl reads for explain and for validation.
//
// The document is built once, so it is the same bytes for the life of the
// process. Its hash goes in the URL the index hands out, and a URL that names
// its own content can be cached for ever: a client re-fetches only when the
// index points somewhere new.
func serveOpenAPI(mux *http.ServeMux) {
	doc, _ := json.Marshal(openAPIDocument())
	sum := sha512.Sum512(doc)
	hash := strings.ToUpper(hex.EncodeToString(sum[:]))
	etag := `"` + hash + `"`

	mux.HandleFunc("GET /openapi/v3", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"paths": map[string]any{
				"api/v1": map[string]any{"serverRelativeURL": "/openapi/v3/api/v1?hash=" + hash},
			},
		})
	})
	mux.HandleFunc("GET /openapi/v3/api/v1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		if r.URL.Query().Get("hash") == hash {
			w.Header().Set("Cache-Control", "public, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache, private")
		}
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(doc)
	})
}

// openAPIDocument is the schema of the core group as this server serves it.
// The x-kubernetes-patch-* extensions are the same table strategicMerge
// works from: a client reads them to know how a list will be merged.
func openAPIDocument() map[string]any {
	str := map[string]any{"type": "string"}
	strMap := map[string]any{"type": "object", "additionalProperties": str}
	integer := map[string]any{"type": "integer", "format": "int32"}
	object := func(props map[string]any) map[string]any {
		return map[string]any{"type": "object", "properties": props}
	}
	ref := func(name string) map[string]any {
		return map[string]any{"$ref": "#/components/schemas/" + name}
	}
	const meta = "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
	kind := func(k string, props map[string]any) map[string]any {
		props["apiVersion"], props["kind"], props["metadata"] = str, str, ref(meta)
		schema := object(props)
		schema["x-kubernetes-group-version-kind"] = []any{map[string]any{"group": "", "version": "v1", "kind": k}}
		return schema
	}

	schemas := map[string]any{
		meta: object(map[string]any{
			"name": str, "namespace": str, "uid": str, "resourceVersion": str, "creationTimestamp": str,
			"labels": strMap, "annotations": strMap,
			"finalizers": map[string]any{
				"type": "array", "items": str,
				"x-kubernetes-patch-strategy": "merge",
			},
			"ownerReferences": map[string]any{
				"type": "array", "items": object(map[string]any{
					"apiVersion": str, "kind": str, "name": str, "uid": str,
					"controller": map[string]any{"type": "boolean"},
				}),
				"x-kubernetes-patch-strategy":  "merge",
				"x-kubernetes-patch-merge-key": "uid",
			},
			"managedFields": map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		}),
		"io.k8s.api.core.v1.ConfigMap": kind("ConfigMap", map[string]any{
			"data": strMap,
			"binaryData": map[string]any{
				"type": "object", "additionalProperties": map[string]any{"type": "string", "format": "byte"},
			},
		}),
		"io.k8s.api.core.v1.Namespace": kind("Namespace", map[string]any{
			"spec":   object(map[string]any{"finalizers": map[string]any{"type": "array", "items": str}}),
			"status": object(map[string]any{"phase": str}),
		}),
		"io.k8s.api.core.v1.ReplicationController": kind("ReplicationController", map[string]any{
			"spec": object(map[string]any{
				"replicas": integer,
				"selector": strMap,
				"template": map[string]any{"type": "object"},
			}),
			"status": object(map[string]any{"replicas": integer}),
		}),
	}

	operations := func(verbs ...string) map[string]any {
		out := map[string]any{}
		for _, verb := range verbs {
			out[verb] = map[string]any{"responses": map[string]any{"200": map[string]any{"description": "OK"}}}
		}
		return out
	}
	paths := map[string]any{
		"/api/v1/namespaces":               operations("get", "post"),
		"/api/v1/namespaces/{name}":        operations("get", "put", "delete"),
		"/api/v1/namespaces/{name}/status": operations("get", "put"),
		"/api/v1/configmaps":               operations("get"),
		"/api/v1/replicationcontrollers":   operations("get"),
	}
	for _, resource := range []string{"configmaps", "replicationcontrollers"} {
		paths["/api/v1/namespaces/{namespace}/"+resource] = operations("get", "post")
		paths["/api/v1/namespaces/{namespace}/"+resource+"/{name}"] = operations("get", "put", "patch", "delete")
	}
	paths["/api/v1/namespaces/{namespace}/replicationcontrollers/{name}/status"] = operations("get", "put")
	paths["/api/v1/namespaces/{namespace}/replicationcontrollers/{name}/scale"] = operations("get", "put", "patch")

	return map[string]any{
		"openapi":    "3.0.0",
		"info":       map[string]any{"title": "Kubernetes", "version": "v1.0.0"},
		"paths":      paths,
		"components": map[string]any{"schemas": schemas},
	}
}

// userInfo is who a request is from, as far as the server can tell.
type userInfo struct {
	name, uid string
	groups    []string
}

type userKey struct{}

// anonymous is every caller while authentication is off.
var anonymous = userInfo{name: "system:anonymous", groups: []string{"system:unauthenticated"}}

// loadTokens reads a static token file, one token,user,uid[,groups] per line.
// A file that does not parse stops the server: running with fewer users than
// the operator wrote down locks someone out without saying who.
func loadTokens(path string) (map[string]userInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read the token file: %w", err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse the token file %s: %w", path, err)
	}
	tokens := map[string]userInfo{}
	for i, record := range records {
		if len(record) < 3 || len(record) > 4 || record[0] == "" || record[1] == "" {
			return nil, fmt.Errorf("the token file %s, line %d: want token,user,uid[,groups], got %d fields", path, i+1, len(record))
		}
		user := userInfo{name: record[1], uid: record[2]}
		if len(record) == 4 {
			for _, group := range strings.Split(record[3], ",") {
				if group = strings.TrimSpace(group); group != "" {
					user.groups = append(user.groups, group)
				}
			}
		}
		// Every user who proved who they are is in this group, which is
		// what a rule meaning "anyone signed in" is written against.
		user.groups = append(user.groups, "system:authenticated")
		tokens[record[0]] = user
	}
	return tokens, nil
}

// authenticate decides who each request is from before anything else sees it,
// and refuses one it cannot name once any authenticator is configured. With
// none, every request is anonymous and none is refused.
//
// A verified client certificate is asked first, then a bearer token, and the
// first that names somebody wins. The health endpoints answer anybody: whatever
// restarts this process has no credentials, and a server that cannot say it is
// alive gets restarted for ever.
func authenticate(tokens map[string]userInfo, certs bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := anonymous
		switch r.URL.Path {
		case "/healthz", "/livez", "/readyz":
		default:
			if known, ok := certUser(r); ok {
				user = known
				break
			}
			if tokens == nil && !certs {
				break
			}
			token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			known, ok := tokens[token]
			if !bearer || !ok {
				// 401 says "I do not know who you are"; 403, later, says "I
				// know, and no". A client retries the first with credentials.
				writeStatus(w, http.StatusUnauthorized, "Unauthorized", "Unauthorized")
				return
			}
			user = known
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey{}, user)))
	})
}

const rbacGroup = "rbac.authorization.k8s.io"

// attributes are everything authorization is allowed to know about a request:
// who, which verb, and what it is aimed at. The body is not among them, which
// is why what a role may contain is checked separately, by escalation.
type attributes struct {
	user     userInfo
	verb     string
	resource bool // false for a path that names no object, like /apis

	group, kind, subresource, namespace, name string
	path                                      string
}

// requestAttributes reads a request's attributes off its method and URL.
//
// /api/v1/<rest> is the core group and /apis/<group>/<version>/<rest> a named
// one; anything shorter or elsewhere is a non-resource path. A namespace's own
// URL puts the namespace in both places, which is what lets a RoleBinding in a
// namespace grant reading that namespace.
func requestAttributes(r *http.Request, user userInfo) attributes {
	a := attributes{user: user, path: r.URL.Path, verb: strings.ToLower(r.Method)}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case len(parts) >= 3 && parts[0] == "api":
		parts = parts[2:]
	case len(parts) >= 4 && parts[0] == "apis":
		a.group, parts = parts[1], parts[3:]
	default:
		return a
	}
	a.resource = true
	if parts[0] == "namespaces" && len(parts) > 1 {
		a.namespace = parts[1]
		if len(parts) > 2 && parts[2] != "status" {
			parts = parts[2:]
		}
	}
	a.kind = parts[0]
	if len(parts) > 1 {
		a.name = parts[1]
	}
	if len(parts) > 2 {
		a.subresource = parts[2]
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		// A watch and a list are one URL and two permissions: a role that may
		// read a collection once may not necessarily follow it for ever.
		switch {
		case watching(r):
			a.verb = "watch"
		case a.name == "":
			a.verb = "list"
		default:
			a.verb = "get"
		}
	case http.MethodPost:
		a.verb = "create"
	case http.MethodPut:
		a.verb = "update"
	case http.MethodDelete:
		a.verb = "delete"
		if a.name == "" {
			a.verb = "deletecollection"
		}
	}
	return a
}

// forbidden is the message a refused request is answered with, in the words
// the real server uses, which is what people paste into searches.
func (a attributes) forbidden() string {
	who := fmt.Sprintf("User %q", a.user.name)
	if !a.resource {
		return fmt.Sprintf("forbidden: %s cannot %s path %q", who, a.verb, a.path)
	}
	resource := a.kind
	if a.subresource != "" {
		resource += "/" + a.subresource
	}
	msg := fmt.Sprintf("%s cannot %s resource %q in API group %q", who, a.verb, resource, a.group)
	if a.namespace != "" {
		msg += fmt.Sprintf(" in the namespace %q", a.namespace)
	} else {
		msg += " at the cluster scope"
	}
	qualified := a.kind
	if a.group != "" {
		qualified += "." + a.group
	}
	if a.name != "" {
		return fmt.Sprintf("%s %q is forbidden: %s", qualified, a.name, msg)
	}
	return qualified + " is forbidden: " + msg
}

// policyRule is one rule of a Role or ClusterRole. Every list is an OR, the
// lists are ANDed together, and "*" matches anything in its list.
type policyRule struct {
	Verbs           []string `json:"verbs"`
	APIGroups       []string `json:"apiGroups"`
	Resources       []string `json:"resources"`
	ResourceNames   []string `json:"resourceNames"`
	NonResourceURLs []string `json:"nonResourceURLs"`

	grantedBy string // the binding and role this rule came through, for a reason
}

// binding is the part of a RoleBinding or ClusterRoleBinding that decides
// anything: who, and which role.
type binding struct {
	RoleRef struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"roleRef"`
	Subjects []struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"subjects"`
}

// subject is who in a binding the user is, or "" when it does not name them.
// ServiceAccount subjects are not supported: this server has no service
// accounts to authenticate.
func (b binding) subject(user userInfo) string {
	for _, s := range b.Subjects {
		switch {
		case s.Kind == "User" && s.Name == user.name:
			return fmt.Sprintf("User %q", s.Name)
		case s.Kind == "Group" && slices.Contains(user.groups, s.Name):
			return fmt.Sprintf("Group %q", s.Name)
		}
	}
	return ""
}

// rbac decides requests from the Roles, ClusterRoles and bindings in the
// store, read again on every request: a binding takes effect, or stops, on
// the next request after the write, with nothing cached to go stale. It reads
// the store's view, because a role is checked from inside the write storing it.
type rbac struct {
	objects *store
}

// mastersGroup is allowed everything, bindings or none. It is how the first
// administrator exists before there is a single role to bind them to.
const mastersGroup = "system:masters"

// decide answers whether the request may go on, and why when it may.
func (p *rbac) decide(a attributes) (bool, string) {
	if slices.Contains(a.user.groups, mastersGroup) {
		return true, ""
	}
	for _, rule := range p.rulesFor(a.user, a.namespace) {
		if rule.allows(a) {
			return true, "RBAC: allowed by " + rule.grantedBy
		}
	}
	return false, ""
}

// rulesFor is every rule granted to user that applies in namespace: those
// bound cluster-wide, and those bound in that namespace. An empty namespace
// is the cluster scope, where only cluster-wide bindings count.
func (p *rbac) rulesFor(user userInfo, namespace string) []policyRule {
	view := p.objects.view()
	var rules []policyRule
	grant := func(kind string, obj object, roleResource, roleNamespace string) {
		b := as[binding](obj)
		via := b.subject(user)
		if via == "" {
			return
		}
		role, ok := view[registryKey(roleResource, roleNamespace, b.RoleRef.Name)]
		if !ok {
			return
		}
		for _, rule := range roleRules(role) {
			rule.grantedBy = fmt.Sprintf("%s %q of %s %q to %s", kind, metaString(obj, "name"), b.RoleRef.Kind, b.RoleRef.Name, via)
			rules = append(rules, rule)
		}
	}
	for _, obj := range scan(view, "clusterrolebindings", "") {
		// A ClusterRoleBinding can only grant a ClusterRole: a Role belongs
		// to one namespace and has nothing to say about the others.
		if as[binding](obj).RoleRef.Kind == "ClusterRole" {
			grant("ClusterRoleBinding", obj, "clusterroles", "")
		}
	}
	if namespace == "" {
		return rules
	}
	for _, obj := range scan(view, "rolebindings", namespace) {
		// A RoleBinding to a ClusterRole grants that role's rules here and
		// nowhere else: one definition, reused namespace by namespace.
		switch as[binding](obj).RoleRef.Kind {
		case "Role":
			grant("RoleBinding", obj, "roles", namespace)
		case "ClusterRole":
			grant("RoleBinding", obj, "clusterroles", "")
		}
	}
	return rules
}

// as reads a decoded JSON value into a typed shape, leaving out whatever does
// not fit it.
func as[T any](v any) T {
	raw, _ := json.Marshal(v)
	var out T
	_ = json.Unmarshal(raw, &out)
	return out
}

// roleRules reads the rules out of a stored Role or ClusterRole.
func roleRules(role object) []policyRule {
	return as[[]policyRule](role["rules"])
}

// matches is a rule's list allowing a value: naming it, or "*".
func matches(list []string, value string) bool {
	return slices.Contains(list, value) || slices.Contains(list, "*")
}

// urlMatches is a nonResourceURLs list allowing a path, where an entry ending
// in * is a prefix.
func urlMatches(urls []string, path string) bool {
	for _, u := range urls {
		if u == path || (strings.HasSuffix(u, "*") && strings.HasPrefix(path, strings.TrimSuffix(u, "*"))) {
			return true
		}
	}
	return false
}

// resourceMatches is a rule's resources allowing one, where a subresource is
// its own entry: "replicationcontrollers" does not grant
// "replicationcontrollers/scale", and "*/scale" grants every scale.
func resourceMatches(resources []string, resource, subresource string) bool {
	combined := resource
	if subresource != "" {
		combined += "/" + subresource
	}
	for _, r := range resources {
		if r == "*" || r == combined || (subresource != "" && r == "*/"+subresource) {
			return true
		}
	}
	return false
}

func (rule policyRule) allows(a attributes) bool {
	if !matches(rule.Verbs, a.verb) {
		return false
	}
	if !a.resource {
		return urlMatches(rule.NonResourceURLs, a.path)
	}
	if !matches(rule.APIGroups, a.group) || !resourceMatches(rule.Resources, a.kind, a.subresource) {
		return false
	}
	// Names narrow a rule to particular objects, so a request that names no
	// object — a list, a watch, a create — is never covered by one.
	return len(rule.ResourceNames) == 0 || (a.name != "" && slices.Contains(rule.ResourceNames, a.name))
}

// authorize refuses every request the policy does not allow, after
// authentication has said who it is from and before any handler sees it.
// A nil policy is AlwaysAllow.
func authorize(policy *rbac, next http.Handler) http.Handler {
	if policy == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/livez", "/readyz":
		default:
			a := requestAttributes(r, r.Context().Value(userKey{}).(userInfo))
			if allowed, _ := policy.decide(a); !allowed {
				writeStatus(w, http.StatusForbidden, "Forbidden", a.forbidden())
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// forbiddenError is a write refused for what it contains.
type forbiddenError struct{ message string }

func (e *forbiddenError) Error() string { return e.message }

// escalation is the check on writing a role or a binding: nobody may hand out
// a permission they do not hold. Without it, permission to create Roles is
// permission to do anything, one Role and one RoleBinding away.
//
// The escalate verb on roles, and bind on the role a binding refers to, are
// the two ways round it, for whoever is trusted to grant what they do not use.
func (p *rbac) escalation(t resourceType) func(r *http.Request, namespace string, obj object) error {
	return func(r *http.Request, namespace string, obj object) error {
		user := r.Context().Value(userKey{}).(userInfo)
		if slices.Contains(user.groups, mastersGroup) {
			return nil
		}
		holds := func(verb, resource, name string) bool {
			allowed, _ := p.decide(attributes{user: user, verb: verb, resource: true, group: rbacGroup, kind: resource, namespace: namespace, name: name})
			return allowed
		}
		var granting []policyRule
		switch t.resource {
		case "roles", "clusterroles":
			if holds("escalate", t.resource, metaString(obj, "name")) {
				return nil
			}
			granting = roleRules(obj)
		default:
			ref := as[binding](obj).RoleRef
			roleResource, roleNamespace := "clusterroles", ""
			if ref.Kind == "Role" {
				roleResource, roleNamespace = "roles", namespace
			}
			if holds("bind", roleResource, ref.Name) {
				return nil
			}
			role, ok := p.objects.view()[registryKey(roleResource, roleNamespace, ref.Name)]
			if !ok {
				return &forbiddenError{fmt.Sprintf("%s.%s %q is forbidden: %s %q not found, and binding a role needs either every permission in it or the bind verb on it",
					t.resource, rbacGroup, metaString(obj, "name"), roleResource, ref.Name)}
			}
			granting = roleRules(role)
		}
		missing := notHeld(granting, p.rulesFor(user, namespace))
		if len(missing) == 0 {
			return nil
		}
		return &forbiddenError{fmt.Sprintf("%s.%s %q is forbidden: user %q (groups=%q) is attempting to grant RBAC permissions not currently held:\n%s",
			t.resource, rbacGroup, metaString(obj, "name"), user.name, user.groups, strings.Join(missing, "\n"))}
	}
}

// notHeld breaks each wanted rule into single permissions — one verb on one
// resource in one group, or on one path — and returns those none of the held
// rules covers. A wildcard wanted is only covered by the same wildcard held.
func notHeld(want, held []policyRule) []string {
	var missing []string
	covered := func(test func(policyRule) bool) bool {
		return slices.ContainsFunc(held, test)
	}
	for _, rule := range want {
		for _, verb := range rule.Verbs {
			for _, url := range rule.NonResourceURLs {
				if !covered(func(h policyRule) bool { return matches(h.Verbs, verb) && urlMatches(h.NonResourceURLs, url) }) {
					missing = append(missing, fmt.Sprintf("{NonResourceURLs:[%q], Verbs:[%q]}", url, verb))
				}
			}
			for _, group := range rule.APIGroups {
				for _, resource := range rule.Resources {
					names := rule.ResourceNames
					if len(names) == 0 {
						names = []string{""}
					}
					for _, name := range names {
						kind, sub, _ := strings.Cut(resource, "/")
						if !covered(func(h policyRule) bool {
							return matches(h.Verbs, verb) && matches(h.APIGroups, group) &&
								(slices.Contains(h.Resources, resource) || resourceMatches(h.Resources, kind, sub)) &&
								(len(h.ResourceNames) == 0 || (name != "" && slices.Contains(h.ResourceNames, name)))
						}) {
							missing = append(missing, fmt.Sprintf("{APIGroups:[%q], Resources:[%q], ResourceNames:[%q], Verbs:[%q]}", group, resource, name, verb))
						}
					}
				}
			}
		}
	}
	return missing
}

// reviewAttributes reads the request a SelfSubjectAccessReview asks about.
// The user is always the caller: a self review cannot ask about anyone else.
func reviewAttributes(review map[string]any, user userInfo) (attributes, error) {
	spec, _ := review["spec"].(map[string]any)
	field := func(m map[string]any, key string) string {
		s, _ := m[key].(string)
		return s
	}
	if ra, ok := spec["resourceAttributes"].(map[string]any); ok {
		return attributes{
			user: user, resource: true, verb: field(ra, "verb"),
			group: field(ra, "group"), kind: field(ra, "resource"), subresource: field(ra, "subresource"),
			namespace: field(ra, "namespace"), name: field(ra, "name"),
		}, nil
	}
	if nra, ok := spec["nonResourceAttributes"].(map[string]any); ok {
		return attributes{user: user, verb: field(nra, "verb"), path: field(nra, "path")}, nil
	}
	return attributes{}, errors.New("spec.resourceAttributes or spec.nonResourceAttributes is required: a review asks about one request")
}

// bootstrap creates the roles a cluster cannot work without, when they are
// missing. Without them a signed-in user with no bindings cannot even run
// discovery, and so cannot find out they are allowed nothing.
//
//   - system:discovery: GET on the discovery, OpenAPI, version and health paths
//   - system:basic-user: creating self reviews, so anyone can ask who they are
//     and what they may do
//
// both bound to system:authenticated.
func (p *rbac) bootstrap() error {
	labels := map[string]any{"kubernetes.io/bootstrapping": "rbac-defaults"}
	roles := map[string][]any{
		"system:discovery": {map[string]any{
			"verbs": []any{"get"},
			"nonResourceURLs": []any{"/api", "/api/*", "/apis", "/apis/*", "/healthz", "/livez",
				"/openapi", "/openapi/*", "/readyz", "/version", "/version/"},
		}},
		"system:basic-user": {
			map[string]any{"verbs": []any{"create"}, "apiGroups": []any{"authorization.k8s.io"}, "resources": []any{"selfsubjectaccessreviews"}},
			map[string]any{"verbs": []any{"create"}, "apiGroups": []any{"authentication.k8s.io"}, "resources": []any{"selfsubjectreviews"}},
		},
	}
	for _, name := range slices.Sorted(maps.Keys(roles)) {
		meta := map[string]any{"name": name, "labels": clone(labels)}
		writes := []struct {
			resource string
			obj      object
		}{
			{"clusterroles", object{"apiVersion": rbacGroup + "/v1", "kind": "ClusterRole", "metadata": meta, "rules": roles[name]}},
			{"clusterrolebindings", object{
				"apiVersion": rbacGroup + "/v1", "kind": "ClusterRoleBinding", "metadata": clone(meta),
				"roleRef":  map[string]any{"apiGroup": rbacGroup, "kind": "ClusterRole", "name": name},
				"subjects": []any{map[string]any{"apiGroup": rbacGroup, "kind": "Group", "name": "system:authenticated"}},
			}},
		}
		for _, write := range writes {
			if _, err := p.objects.create(write.resource, "", name, write.obj); err != nil && !errors.Is(err, errAlreadyExists) {
				return fmt.Errorf("create the %s %s: %w", write.resource, name, err)
			}
		}
	}
	return nil
}

// admitFunc is the last word on a write before it is stored: handed the object
// about to be written and the one it replaces, it returns what to write
// instead, or why not. obj is nil for a delete, old is nil for a create.
type admitFunc func(obj, old object) (object, error)

// statusError is a refusal that carries its own HTTP code and reason.
type statusError struct {
	code    int
	reason  string
	message string
}

func (e *statusError) Error() string { return e.message }

func internalError(format string, args ...any) error {
	return &statusError{code: http.StatusInternalServerError, reason: "InternalError",
		message: "Internal error occurred: " + fmt.Sprintf(format, args...)}
}

// webhookConfiguration is a MutatingWebhookConfiguration or a
// ValidatingWebhookConfiguration, in the fields this server acts on.
type webhookConfiguration struct {
	Webhooks []webhook `json:"webhooks"`
}

type webhook struct {
	Name         string `json:"name"`
	ClientConfig struct {
		URL      string `json:"url"`
		CABundle []byte `json:"caBundle"` // base64 in JSON, which []byte decodes
	} `json:"clientConfig"`
	Rules             []webhookRule  `json:"rules"`
	FailurePolicy     string         `json:"failurePolicy"`
	TimeoutSeconds    *int           `json:"timeoutSeconds"`
	NamespaceSelector *labelSelector `json:"namespaceSelector"`
	ObjectSelector    *labelSelector `json:"objectSelector"`
}

type webhookRule struct {
	Operations  []string `json:"operations"`
	APIGroups   []string `json:"apiGroups"`
	APIVersions []string `json:"apiVersions"`
	Resources   []string `json:"resources"`
	Scope       string   `json:"scope"`
}

type labelSelector struct {
	MatchLabels      map[string]string `json:"matchLabels"`
	MatchExpressions []struct {
		Key      string   `json:"key"`
		Operator string   `json:"operator"`
		Values   []string `json:"values"`
	} `json:"matchExpressions"`
}

// matches reports whether a set of labels is selected. An absent selector, and
// an empty one, select everything.
func (s *labelSelector) matches(labels map[string]string) bool {
	if s == nil {
		return true
	}
	for k, v := range s.MatchLabels {
		if got, ok := labels[k]; !ok || got != v {
			return false
		}
	}
	for _, e := range s.MatchExpressions {
		value, ok := labels[e.Key]
		var pass bool
		switch e.Operator {
		case "In":
			pass = ok && slices.Contains(e.Values, value)
		case "NotIn":
			pass = !ok || !slices.Contains(e.Values, value)
		case "Exists":
			pass = ok
		case "DoesNotExist":
			pass = !ok
		}
		if !pass {
			return false
		}
	}
	return true
}

// admitter is the admission chain for one request to one resource, or nil
// when no webhook is configured at all — which leaves every write exactly as
// it was before admission existed.
//
// The webhook configurations themselves are never sent to a webhook: one that
// refused them could never be removed.
func admitter(objects *store, r *http.Request, t resourceType, subresource string) admitFunc {
	if t.group == "admissionregistration.k8s.io" {
		return nil
	}
	mutating := webhooksOf(objects, "mutatingwebhookconfigurations")
	validating := webhooksOf(objects, "validatingwebhookconfigurations")
	if len(mutating) == 0 && len(validating) == 0 {
		return nil
	}
	a := &admission{objects: objects, r: r, t: t, subresource: subresource,
		namespace: r.PathValue("namespace"), name: r.PathValue("name")}
	return func(obj, old object) (object, error) {
		return a.admit(mutating, validating, obj, old)
	}
}

// webhooksOf reads every webhook of one kind of configuration, in the order
// they run: by configuration name, which is the order a list comes back in,
// then as listed inside each.
func webhooksOf(objects *store, resource string) []webhook {
	items, _ := objects.list(resource, "")
	var hooks []webhook
	for _, item := range items {
		var config webhookConfiguration
		raw, _ := json.Marshal(item)
		if json.Unmarshal(raw, &config) == nil {
			hooks = append(hooks, config.Webhooks...)
		}
	}
	return hooks
}

// admission is one request on its way through the chain.
type admission struct {
	objects         *store
	r               *http.Request
	t               resourceType
	subresource     string
	namespace, name string
}

// admit runs the chain: every mutating webhook in turn, each seeing what the
// one before it made; then the server's own validation of the result; then
// every validating webhook at once, all of which must allow it.
func (a *admission) admit(mutating, validating []webhook, obj, old object) (object, error) {
	// A create's name is in its body, not its URL.
	if a.name == "" {
		a.name = metaString(obj, "name")
	}
	op := "UPDATE"
	switch {
	case obj == nil:
		op = "DELETE"
	case old == nil:
		op = "CREATE"
	}
	for _, h := range mutating {
		if !a.matches(h, op, obj, old) {
			continue
		}
		next, err := a.call(h, true, op, obj, old)
		if err != nil {
			return nil, err
		}
		obj = next
	}
	if obj != nil {
		if err := a.validate(obj); err != nil {
			return nil, err
		}
	}
	// Validating webhooks cannot change anything, so nothing orders them.
	errs := make([]error, len(validating))
	var wg sync.WaitGroup
	for i, h := range validating {
		if !a.matches(h, op, obj, old) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = a.call(h, false, op, obj, old)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

// validate is the server's own say on an object a webhook may have changed. A
// webhook can add and alter fields, but not make the object invalid or move it
// somewhere else.
func (a *admission) validate(obj object) error {
	invalid := func(field string, value any, why string) error {
		return &statusError{code: http.StatusUnprocessableEntity, reason: "Invalid",
			message: fmt.Sprintf("%s %q is invalid: %s: Invalid value: %v: %s", a.t.kind, a.name, field, value, why)}
	}
	if got := metaString(obj, "name"); got != a.name {
		return invalid("metadata.name", got, "the object cannot be renamed")
	}
	// A create's namespace is only filled in from the URL as it is stored.
	if got := metaString(obj, "namespace"); got != "" && got != a.namespace {
		return invalid("metadata.namespace", got, "the object cannot be moved to another namespace")
	}
	if obj["apiVersion"] != a.t.groupVersion() || obj["kind"] != a.t.kind {
		return invalid("kind", obj["kind"], "the object cannot change its kind")
	}
	if a.t.resource == "replicationcontrollers" {
		spec, _ := obj["spec"].(map[string]any)
		if replicas, ok := spec["replicas"].(float64); !ok || replicas < 0 || replicas != math.Trunc(replicas) {
			return invalid("spec.replicas", spec["replicas"], "must be a whole number, zero or more")
		}
	}
	return nil
}

// matches reports whether a webhook wants to be told about this request.
func (a *admission) matches(h webhook, op string, obj, old object) bool {
	ruled := slices.ContainsFunc(h.Rules, func(rule webhookRule) bool {
		scoped := rule.Scope == "" || rule.Scope == "*" || (rule.Scope == "Cluster") == a.t.clusterScoped
		return scoped && anyOf(rule.Operations, op) && anyOf(rule.APIGroups, a.t.group) &&
			anyOf(rule.APIVersions, cmp.Or(a.t.version, "v1")) &&
			webhookResourceMatches(rule.Resources, a.t.resource, a.subresource)
	})
	if !ruled {
		return false
	}
	// A delete has no new object, and an update may change the labels: the
	// selector is satisfied by either side.
	if !h.ObjectSelector.matches(labelsOf(obj)) && !h.ObjectSelector.matches(labelsOf(old)) {
		return false
	}
	if a.namespace != "" && h.NamespaceSelector != nil {
		ns, _ := a.objects.get("namespaces", "", a.namespace)
		if !h.NamespaceSelector.matches(labelsOf(ns)) {
			return false
		}
	}
	return true
}

func anyOf(list []string, want string) bool {
	return slices.Contains(list, "*") || slices.Contains(list, want)
}

// webhookResourceMatches reads a rule's resources: "*" is every resource but none of
// their subresources, "*/*" is everything, "x/*" every subresource of x and
// "*/y" subresource y of anything.
func webhookResourceMatches(patterns []string, resource, subresource string) bool {
	for _, p := range patterns {
		res, sub, _ := strings.Cut(p, "/")
		if res != "*" && res != resource {
			continue
		}
		if sub == subresource || (sub == "*" && subresource != "") || p == "*/*" {
			return true
		}
	}
	return false
}

// call asks one webhook and reads its verdict. A webhook that cannot be
// reached, does not answer in time, or answers something else entirely has
// failed; failurePolicy says whether that refuses the request or is ignored.
func (a *admission) call(h webhook, mutating bool, op string, obj, old object) (object, error) {
	response, err := a.send(h, op, obj, old)
	if err != nil {
		if h.FailurePolicy == "Ignore" {
			return obj, nil
		}
		return nil, internalError("failed calling webhook %q: %v", h.Name, err)
	}
	if !response.Allowed {
		status := response.Status
		message := fmt.Sprintf("admission webhook %q denied the request", h.Name)
		switch {
		case status.Message != "":
			message += ": " + status.Message
		case status.Reason != "":
			message += ": " + status.Reason
		default:
			message += " without explanation"
		}
		return nil, &statusError{code: cmp.Or(status.Code, http.StatusForbidden), reason: status.Reason, message: message}
	}
	if !mutating || obj == nil || len(response.Patch) == 0 {
		return obj, nil
	}
	if response.PatchType != "JSONPatch" {
		return nil, internalError("webhook %q answered patchType %q, and JSONPatch is the only one there is", h.Name, response.PatchType)
	}
	var ops []patchOp
	if err := json.Unmarshal(response.Patch, &ops); err != nil {
		return nil, internalError("webhook %q answered a patch that is not a JSON patch: %v", h.Name, err)
	}
	patched, err := jsonPatch(map[string]any(clone(obj)), ops)
	result, ok := patched.(map[string]any)
	if err != nil || !ok {
		return nil, internalError("the patch from webhook %q cannot be applied: %v", h.Name, err)
	}
	return result, nil
}

type admissionResponse struct {
	UID     string `json:"uid"`
	Allowed bool   `json:"allowed"`
	Status  struct {
		Code    int    `json:"code"`
		Reason  string `json:"reason"`
		Message string `json:"message"`
	} `json:"status"`
	PatchType string `json:"patchType"`
	Patch     []byte `json:"patch"` // base64 in JSON
}

// send is the HTTPS round trip. The webhook's certificate is checked against
// its caBundle alone: anything able to answer on that address is not enough.
func (a *admission) send(h webhook, op string, obj, old object) (*admissionResponse, error) {
	if u, err := url.Parse(h.ClientConfig.URL); err != nil || u.Scheme != "https" {
		return nil, fmt.Errorf("clientConfig.url %q is not an https URL", h.ClientConfig.URL)
	}
	var roots *x509.CertPool
	if len(h.ClientConfig.CABundle) > 0 {
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(h.ClientConfig.CABundle) {
			return nil, errors.New("clientConfig.caBundle holds no PEM certificate")
		}
	}
	timeout := 10 * time.Second
	if h.TimeoutSeconds != nil {
		timeout = time.Duration(*h.TimeoutSeconds) * time.Second
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: timeout}

	user := a.r.Context().Value(userKey{}).(userInfo)
	who := map[string]any{"username": user.name, "groups": user.groups}
	if user.uid != "" {
		who["uid"] = user.uid
	}
	version := cmp.Or(a.t.version, "v1")
	kind := map[string]any{"group": a.t.group, "version": version, "kind": a.t.kind}
	resource := map[string]any{"group": a.t.group, "version": version, "resource": a.t.resource}
	uid := newUID()
	request := map[string]any{
		"uid": uid, "kind": kind, "resource": resource, "requestKind": kind, "requestResource": resource,
		"name": a.name, "namespace": a.namespace, "operation": op, "userInfo": who,
		"object": obj, "oldObject": old, "dryRun": false,
	}
	if a.subresource != "" {
		request["subResource"], request["requestSubResource"] = a.subresource, a.subresource
	}
	body, _ := json.Marshal(map[string]any{"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "request": request})

	res, err := client.Post(h.ClientConfig.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the webhook answered %d", res.StatusCode)
	}
	var review struct {
		Response *admissionResponse `json:"response"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 3<<20)).Decode(&review); err != nil {
		return nil, fmt.Errorf("the webhook's answer is not an AdmissionReview: %w", err)
	}
	if review.Response == nil {
		return nil, errors.New("the webhook's AdmissionReview has no response")
	}
	// The uid ties the verdict to this request; one for any other request is
	// no verdict on this one.
	if review.Response.UID != uid {
		return nil, fmt.Errorf("expected response.uid=%q, got %q", uid, review.Response.UID)
	}
	return review.Response, nil
}

// certUser is the user a verified client certificate names: the common name
// is the username and each organization a group. VerifiedChains is empty
// unless the TLS handshake checked the certificate against the client CAs.
func certUser(r *http.Request) (userInfo, bool) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
		return userInfo{}, false
	}
	subject := r.TLS.VerifiedChains[0][0].Subject
	if subject.CommonName == "" {
		return userInfo{}, false
	}
	groups := append(slices.Clone(subject.Organization), "system:authenticated")
	return userInfo{name: subject.CommonName, groups: groups}, true
}
