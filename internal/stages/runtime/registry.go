package runtime

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/madhank93/byo-k8s-x/internal/stages"
)

const (
	mediaIndex    = "application/vnd.oci.image.index.v1+json"
	mediaManifest = "application/vnd.oci.image.manifest.v1+json"
	mediaConfig   = "application/vnd.oci.image.config.v1+json"
	mediaLayer    = "application/vnd.oci.image.layer.v1.tar+gzip"
)

// registry is an OCI distribution registry the grader serves on the host and
// the learner's runtime pulls from inside the node. It speaks the token
// protocol Docker Hub does — a 401 naming a realm, a token from that realm,
// then Bearer on every request — and logs what it was asked, so a stage can
// judge requests as well as results.
type registry struct {
	host    string // host:port, as the node reaches it
	realm   string // absolute token URL, different every run
	service string
	srv     *http.Server

	mu        sync.Mutex
	users     map[string]string // user -> password, for private repos
	private   map[string]bool
	tokens    map[string]string // token -> the scope it was issued for
	tokenLog  []tokenRequest
	blobGets  []string // digests fetched through /v2/…/blobs/
	manifests map[string]map[string]stored
	blobs     map[string][]byte
	tampered  map[string][]byte // digest -> the wrong bytes served for it
}

type tokenRequest struct {
	service, scope, user string
	granted              bool
}

type stored struct {
	mediaType string
	data      []byte
}

func newRegistry() (*registry, error) {
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("listen for the registry: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	r := &registry{
		host:      fmt.Sprintf("%s:%d", stages.ExternalHost(), port),
		service:   "byok8s-registry",
		users:     map[string]string{},
		private:   map[string]bool{},
		tokens:    map[string]string{},
		manifests: map[string]map[string]stored{},
		blobs:     map[string][]byte{},
		tampered:  map[string][]byte{},
	}
	authPath := "/auth/" + randHex(6) + "/token"
	r.realm = "http://" + r.host + authPath
	mux := http.NewServeMux()
	mux.HandleFunc(authPath, r.token)
	mux.HandleFunc("/v2/", r.v2)
	mux.HandleFunc("/cdn/", r.cdn)
	r.srv = &http.Server{Handler: mux}
	go r.srv.Serve(ln)
	return r, nil
}

func (r *registry) close() { r.srv.Close() }

// ref names an image in this registry: host:port/repo:tag.
func (r *registry) ref(repo, tag string) string { return r.host + "/" + repo + ":" + tag }

func (r *registry) token(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	tr := tokenRequest{service: q.Get("service"), scope: q.Get("scope")}
	user, pass, hasBasic := req.BasicAuth()
	tr.user = user

	repo, ok := strings.CutPrefix(tr.scope, "repository:")
	repo, _, _ = strings.Cut(repo, ":")
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case tr.service != r.service:
		http.Error(w, `{"details":"unknown service"}`, http.StatusBadRequest)
	case !ok || !strings.HasSuffix(tr.scope, ":pull"):
		http.Error(w, `{"details":"scope must be repository:<name>:pull"}`, http.StatusBadRequest)
	case r.private[repo] && (!hasBasic || r.users[user] != pass || pass == ""):
		w.Header().Set("WWW-Authenticate", `Basic realm="byok8s-registry"`)
		http.Error(w, `{"details":"incorrect username or password"}`, http.StatusUnauthorized)
	default:
		tr.granted = true
		t := randHex(16)
		r.tokens[t] = tr.scope
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"token": t, "access_token": t, "expires_in": 300})
	}
	r.tokenLog = append(r.tokenLog, tr)
}

// challenge is the 401 that tells a client where to get a token.
func (r *registry) challenge(w http.ResponseWriter, scope string) {
	h := fmt.Sprintf(`Bearer realm=%q,service=%q`, r.realm, r.service)
	if scope != "" {
		h += fmt.Sprintf(`,scope=%q`, scope)
	}
	w.Header().Set("WWW-Authenticate", h)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	fmt.Fprint(w, `{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`)
}

func (r *registry) authorized(req *http.Request, repo string) bool {
	t, ok := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	scope, ok := r.tokens[t]
	return ok && (repo == "" || scope == "repository:"+repo+":pull")
}

func (r *registry) v2(w http.ResponseWriter, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/v2/")
	if path == "" {
		if !r.authorized(req, "") {
			r.challenge(w, "")
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if repo, ref, ok := strings.Cut(path, "/manifests/"); ok {
		if !r.authorized(req, repo) {
			r.challenge(w, "repository:"+repo+":pull")
			return
		}
		r.manifest(w, req, repo, ref)
		return
	}
	if repo, digest, ok := strings.Cut(path, "/blobs/"); ok {
		if !r.authorized(req, repo) {
			r.challenge(w, "repository:"+repo+":pull")
			return
		}
		r.mu.Lock()
		r.blobGets = append(r.blobGets, digest)
		_, known := r.blobs[digest]
		r.mu.Unlock()
		if !known {
			registryError(w, http.StatusNotFound, "BLOB_UNKNOWN", "blob unknown to registry")
			return
		}
		// Real registries hand blobs off to storage; a client has to follow.
		http.Redirect(w, req, "/cdn/"+digest, http.StatusTemporaryRedirect)
		return
	}
	registryError(w, http.StatusNotFound, "NAME_UNKNOWN", "no such endpoint")
}

func (r *registry) manifest(w http.ResponseWriter, req *http.Request, repo, ref string) {
	r.mu.Lock()
	m, ok := r.manifests[repo][ref]
	bad, isTampered := r.tampered[digestOf(m.data)]
	r.mu.Unlock()
	if !ok {
		registryError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest unknown")
		return
	}
	if !strings.Contains(req.Header.Get("Accept"), m.mediaType) {
		registryError(w, http.StatusNotFound, "MANIFEST_UNKNOWN",
			"this manifest is a "+m.mediaType+", which the request's Accept header does not list")
		return
	}
	w.Header().Set("Content-Type", m.mediaType)
	w.Header().Set("Docker-Content-Digest", digestOf(m.data))
	body := m.data
	if isTampered {
		body = bad
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	if req.Method == http.MethodHead {
		return
	}
	w.Write(body)
}

func (r *registry) cdn(w http.ResponseWriter, req *http.Request) {
	digest := strings.TrimPrefix(req.URL.Path, "/cdn/")
	r.mu.Lock()
	data, ok := r.blobs[digest]
	if bad, isTampered := r.tampered[digest]; isTampered {
		data = bad
	}
	r.mu.Unlock()
	if !ok {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(data)
}

func registryError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"code": code, "message": msg}}})
}

// blobGetCount is how many blob requests the registry has answered.
func (r *registry) blobGetCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.blobGets)
}

func (r *registry) scopeOf(token string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.tokens[token]
	return s, ok
}

func (r *registry) tokenRequests() []tokenRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]tokenRequest(nil), r.tokenLog...)
}

// tamper makes the registry serve wrong bytes for a digest, the way a
// corrupted cache or a man in the middle would.
func (r *registry) tamper(digest string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	src := r.blobs[digest]
	if m := r.findManifest(digest); m != nil {
		src = m
	}
	bad := append([]byte(nil), src...)
	bad = append(bad, ' ')
	r.tampered[digest] = bad
}

func (r *registry) findManifest(digest string) []byte {
	for _, refs := range r.manifests {
		if m, ok := refs[digest]; ok {
			return m.data
		}
	}
	return nil
}

// image is one platform's image: a config and its layers, ready to push.
type image struct {
	config   map[string]any
	layers   [][]byte // gzipped tars, bottom first
	diffIDs  []string
	manifest []byte
}

// pushed is what a stage needs to know about an image after pushing it.
type pushed struct {
	manifest string   // the platform manifest's digest
	config   string   // the config blob's digest
	layers   []string // layer blob digests, bottom first
}

// push stores img under repo, by tag and by digest, and returns its digests.
func (r *registry) push(repo, tag string, img *image) pushed {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pushLocked(repo, tag, img)
}

func (r *registry) pushLocked(repo, tag string, img *image) pushed {
	cfg := map[string]any{}
	for k, v := range img.config {
		cfg[k] = v
	}
	cfg["rootfs"] = map[string]any{"type": "layers", "diff_ids": img.diffIDs}
	cfgJSON, _ := json.Marshal(cfg)
	p := pushed{config: digestOf(cfgJSON)}
	r.blobs[p.config] = cfgJSON
	var layers []map[string]any
	for _, l := range img.layers {
		d := digestOf(l)
		r.blobs[d] = l
		p.layers = append(p.layers, d)
		layers = append(layers, map[string]any{"mediaType": mediaLayer, "digest": d, "size": len(l)})
	}
	img.manifest, _ = json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     mediaManifest,
		"config":        map[string]any{"mediaType": mediaConfig, "digest": p.config, "size": len(cfgJSON)},
		"layers":        layers,
	})
	p.manifest = digestOf(img.manifest)
	r.store(repo, p.manifest, mediaManifest, img.manifest)
	if tag != "" {
		r.store(repo, tag, mediaManifest, img.manifest)
	}
	return p
}

// pushIndex pushes one image per platform under repo and tags an image index
// listing them in the order given. It returns what was pushed for each
// architecture.
func (r *registry) pushIndex(repo, tag string, arches []string, byArch map[string]*image) map[string]pushed {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]pushed{}
	var entries []map[string]any
	for _, arch := range arches {
		img := byArch[arch]
		p := r.pushLocked(repo, "", img)
		out[arch] = p
		entries = append(entries, map[string]any{
			"mediaType": mediaManifest,
			"digest":    p.manifest,
			"size":      len(img.manifest),
			"platform":  map[string]string{"architecture": arch, "os": "linux"},
		})
	}
	idx, _ := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": mediaIndex, "manifests": entries})
	r.store(repo, tag, mediaIndex, idx)
	r.store(repo, digestOf(idx), mediaIndex, idx)
	return out
}

func (r *registry) store(repo, ref, mediaType string, data []byte) {
	if r.manifests[repo] == nil {
		r.manifests[repo] = map[string]stored{}
	}
	r.manifests[repo][ref] = stored{mediaType, data}
}

// entry is one file in a layer tarball.
type entry struct {
	name     string
	typ      byte // tar.TypeReg, TypeDir, TypeSymlink, TypeLink
	mode     int64
	body     []byte
	link     string
	uid, gid int
}

func dir(name string) entry { return entry{name: name, typ: tar.TypeDir, mode: 0o755} }
func file(name, body string, mode int64) entry {
	return entry{name: name, typ: tar.TypeReg, mode: mode, body: []byte(body)}
}

// layer builds a gzipped tarball and returns it with its diff ID, the digest
// of the uncompressed tar that an image config lists.
func layer(entries ...entry) ([]byte, string) {
	var raw bytes.Buffer
	tw := tar.NewWriter(&raw)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Typeflag: e.typ, Mode: e.mode, Uid: e.uid, Gid: e.gid, Linkname: e.link, Format: tar.FormatPAX}
		if e.typ == tar.TypeReg {
			h.Size = int64(len(e.body))
		}
		tw.WriteHeader(h)
		if e.typ == tar.TypeReg {
			tw.Write(e.body)
		}
	}
	tw.Close()
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write(raw.Bytes())
	zw.Close()
	return gz.Bytes(), digestOf(raw.Bytes())
}

func newImage(config map[string]any, layers ...[]entry) *image {
	img := &image{config: config}
	for _, l := range layers {
		gz, diff := layer(l...)
		img.layers = append(img.layers, gz)
		img.diffIDs = append(img.diffIDs, diff)
	}
	return img
}

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// dockerConfig is a config.json carrying credentials for host, the file a
// client finds through DOCKER_CONFIG.
func dockerConfig(host, user, pass string) []byte {
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	b, _ := json.Marshal(map[string]any{"auths": map[string]any{host: map[string]string{"auth": auth}}})
	return b
}
