// Package runtime holds the assertions for the "Build your own container
// runtime" course — one function per stage, registered by slug.
//
// The learner's program is built for Linux and runs inside a kind worker
// node, as root, through runner.Node. Images come from a registry the grader
// serves on the host; the program pulls them over the kind network. Every
// image runs the same probe, which reports what it can see from inside, so a
// stage judges the container the runtime built rather than the runtime's code.
package runtime

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/cluster"
	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

var stageRegistry = map[string]Stage{}

func register(s Stage) { stageRegistry[s.Slug] = s }

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := stageRegistry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "registry-auth", Run: stageRegistryAuth})
	register(Stage{Slug: "manifest-fetch", Run: stageManifestFetch})
	register(Stage{Slug: "layer-pull", Run: stageLayerPull})
	register(Stage{Slug: "unpack", Run: stageUnpack})
	register(Stage{Slug: "overlay-rootfs", Run: stageOverlayRootfs})
	register(Stage{Slug: "image-config", Run: stageImageConfig})
	register(Stage{Slug: "spawn", Run: stageSpawn})
	register(Stage{Slug: "mount-ns", Run: stageMountNS})
	register(Stage{Slug: "pivot-root", Run: stagePivotRoot})
	register(Stage{Slug: "pid-ns", Run: stagePidNS})
}

// The node the course runs in: a worker, so nothing the learner's runtime
// does can reach the control plane's own processes.
const node = runner.Node(cluster.Name + "-worker")

// baseDir holds one directory per stage, passed to the program as --root.
const baseDir = "/var/lib/byok8s-runtime"

// work is one stage's fixtures: the node, the stage's --root, a registry, and
// a token that makes this run's images unlike any earlier run's.
type work struct {
	ctx   context.Context
	root  string
	reg   *registry
	token string
	probe []byte
}

func newWork(ctx context.Context, slug, bin string) (*work, error) {
	if err := node.Install(ctx, bin); err != nil {
		return nil, err
	}
	probe, err := probeBinary(ctx)
	if err != nil {
		return nil, err
	}
	w := &work{ctx: ctx, root: baseDir + "/" + slug, token: randHex(4), probe: probe}
	if err := w.clean(); err != nil {
		return nil, err
	}
	if w.reg, err = newRegistry(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *work) close() {
	w.reg.close()
	w.clean()
}

// clean kills anything a previous run left in the node, then unmounts and
// removes the stage's root, deepest mount first.
func (w *work) clean() error {
	if err := node.Sweep(w.ctx); err != nil {
		return err
	}
	script := fmt.Sprintf(`for m in $(findmnt -rn -o TARGET | grep '^%[1]s' | sort -r); do umount -l "$m"; done; rm -rf %[1]s`, w.root)
	res, err := node.Exec(w.ctx, time.Minute, "sh", "-c", script)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("clear %s in node %s: %s", w.root, node, res.Stderr)
	}
	return nil
}

// app runs the learner's program in the node.
func (w *work) app(env []string, args ...string) (*runner.Result, error) {
	return node.Invoke(w.ctx, env, time.Minute, args...)
}

// sh runs a harness script in the node and returns its stdout, failing on a
// non-zero exit.
func (w *work) sh(script string) (string, error) {
	res, err := node.Exec(w.ctx, time.Minute, "sh", "-c", script)
	if err != nil {
		return "", err
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("in node %s, %q failed: %s", node, script, res.Stderr)
	}
	return res.Stdout, nil
}

//go:embed probe/main.go
var probeSource []byte

var (
	probeOnce sync.Once
	probeBin  []byte
	probeErr  error
)

// probeBinary builds the program every image runs, once per grading run, for
// the node's architecture.
func probeBinary(ctx context.Context) ([]byte, error) {
	probeOnce.Do(func() {
		dir, err := os.MkdirTemp("", "byok8s-probe-")
		if err != nil {
			probeErr = err
			return
		}
		defer os.RemoveAll(dir)
		os.WriteFile(filepath.Join(dir, "main.go"), probeSource, 0o644)
		os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module probe\n\ngo 1.25\n"), 0o644)
		arch, err := runner.NodeArch(ctx)
		if err != nil {
			probeErr = err
			return
		}
		bin, err := runner.BuildFor(ctx, dir, "linux", arch)
		if err != nil {
			probeErr = fmt.Errorf("build the image probe: %w", err)
			return
		}
		defer os.RemoveAll(filepath.Dir(bin))
		probeBin, probeErr = os.ReadFile(bin)
	})
	return probeBin, probeErr
}

// demo is the image most stages pull: two layers, the second deleting and
// replacing things the first put down, with the probe as its entrypoint.
func (w *work) demo() *image {
	return w.variant("demo", nil)
}

// variant is demo with a different marker and extra environment, so its
// digests differ from demo's while its shape stays the same.
func (w *work) variant(name string, env []string) *image {
	base := []entry{
		dir("bin/"),
		{name: "bin/probe", typ: '0', mode: 0o755, body: w.probe},
		{name: "bin/probe-hard", typ: '1', link: "bin/probe"},
		{name: "bin/probe-link", typ: '2', mode: 0o777, link: "probe"},
		dir("etc/"),
		file("etc/byok8s-image", "base layer", 0o644),
		{name: "etc/secret", typ: '0', mode: 0o600, body: []byte("s"), uid: 1000, gid: 1000},
		dir("data/"),
		file("data/old.txt", "old", 0o644),
		file("data/keep.txt", "keep", 0o644),
		dir("opaque/"),
		file("opaque/a.txt", "a", 0o644),
		dir("work/"),
	}
	top := []entry{
		dir("etc/"),
		file("etc/byok8s-image", w.marker(name), 0o644),
		dir("data/"),
		file("data/.wh.old.txt", "", 0o644),
		file("data/new.txt", "new", 0o644),
		dir("opaque/"),
		file("opaque/.wh..wh..opq", "", 0o644),
		file("opaque/b.txt", "b", 0o644),
	}
	cfg := map[string]any{
		"architecture": "",
		"os":           "linux",
		"config": map[string]any{
			"Entrypoint": []string{"/bin/probe"},
			"Cmd":        []string{"from-cmd"},
			"Env":        append([]string{"PATH=/bin", "GREETING=" + w.greeting()}, env...),
			"WorkingDir": "/work",
		},
	}
	return newImage(cfg, base, top)
}

func (w *work) marker(name string) string { return name + " " + w.token }
func (w *work) greeting() string          { return "hello " + w.token }

// pushDemo pushes demo under repo:v1 as an index, beside a decoy built for
// another architecture, and returns what was pushed for the node's.
func (w *work) pushDemo(repo string, img *image) (pushed, error) {
	arch, err := runner.NodeArch(w.ctx)
	if err != nil {
		return pushed{}, err
	}
	other := "amd64"
	if arch == other {
		other = "arm64"
	}
	img.config["architecture"] = arch
	decoy := w.variant("decoy", []string{"DECOY=wrong-platform"})
	decoy.config["architecture"] = other
	// The decoy comes first, so taking the first entry is never right.
	return w.reg.pushIndex(repo, "v1", []string{other, arch}, map[string]*image{arch: img, other: decoy})[arch], nil
}

// unpackedTree is find's view of the demo image once its layers are applied:
// type, mode, owner, path and link target, sorted.
var unpackedTree = []string{
	"d 755 0:0 bin",
	"f 755 0:0 bin/probe",
	"f 755 0:0 bin/probe-hard",
	"l 777 0:0 bin/probe-link probe",
	"d 755 0:0 data",
	"f 644 0:0 data/keep.txt",
	"f 644 0:0 data/new.txt",
	"d 755 0:0 etc",
	"f 644 0:0 etc/byok8s-image",
	"f 600 1000:1000 etc/secret",
	"d 755 0:0 opaque",
	"f 644 0:0 opaque/b.txt",
	"d 755 0:0 work",
}

// tree lists a directory in the node the way unpackedTree is written.
func (w *work) tree(dir string) ([]string, error) {
	out, err := w.sh(fmt.Sprintf(`cd %q && find . -mindepth 1 -printf '%%y %%m %%U:%%G %%P %%l\n'`, dir))
	if err != nil {
		return nil, err
	}
	var got []string
	for _, l := range lines(out) {
		got = append(got, strings.TrimSpace(l))
	}
	slices.SortFunc(got, func(a, b string) int { return strings.Compare(pathOf(a), pathOf(b)) })
	return got, nil
}

// pathOf is the path column of a tree line.
func pathOf(line string) string {
	if f := strings.Fields(line); len(f) > 3 {
		return f[3]
	}
	return line
}

func (w *work) wantTree(dir, what string) error {
	got, err := w.tree(dir)
	if err != nil {
		return err
	}
	if !slices.Equal(got, unpackedTree) {
		return fmt.Errorf("%s does not hold what the image's layers add up to. Expected (type mode uid:gid path link):\n  %s\ngot:\n  %s",
			what, strings.Join(unpackedTree, "\n  "), strings.Join(got, "\n  "))
	}
	marker, err := node.ReadFile(w.ctx, dir+"/etc/byok8s-image")
	if err != nil {
		return err
	}
	if marker != w.marker("demo") {
		return fmt.Errorf("%s/etc/byok8s-image holds %q; the top layer replaced it with %q", what, marker, w.marker("demo"))
	}
	return nil
}

// mountsUnder lists the mount points under the stage root that the node's
// own mount namespace can see.
func (w *work) mountsUnder() ([]string, error) {
	out, err := w.sh(fmt.Sprintf(`findmnt -rn -o TARGET | grep '^%s' || true`, w.root))
	if err != nil {
		return nil, err
	}
	return lines(out), nil
}

func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

func tail(s string) string {
	ls := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(ls) > 40 {
		ls = append([]string{"…"}, ls[len(ls)-40:]...)
	}
	return "  " + strings.Join(ls, "\n  ")
}

func wantExit(res *runner.Result, code int) error {
	if res.ExitCode != code {
		return fmt.Errorf("expected exit %d, got %d\nstdout:\n%s\nstderr:\n%s", code, res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return nil
}

// wantError is a refusal: exit 1 with a reason on stderr, which a crash
// (exit 2, a stack trace) is not.
func wantError(res *runner.Result, why string) error {
	if res.ExitCode != 1 || strings.TrimSpace(res.Stderr) == "" {
		return fmt.Errorf("%s, so expected exit 1 with an error message on stderr, got exit %d\nstdout:\n%s\nstderr:\n%s",
			why, res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return nil
}

// stageRegistryAuth — the program follows the registry's 401 to its token
// realm, asks for a pull token for one repository, sends the credentials in
// DOCKER_CONFIG when it has them, and prints the token it was given.
func stageRegistryAuth(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "registry-auth", bin)
	if err != nil {
		return err
	}
	defer w.close()

	user, pass := "learner", "pw-"+w.token
	w.reg.private["private/demo"] = true
	w.reg.users[user] = pass
	w.reg.push("public/demo", "v1", w.demo())
	w.reg.push("private/demo", "v1", w.demo())

	wantToken := func(res *runner.Result, repo string) error {
		if err := wantExit(res, 0); err != nil {
			return err
		}
		out := lines(res.Stdout)
		if len(out) == 0 {
			return fmt.Errorf("auth printed nothing; it should print the token")
		}
		tok := strings.TrimSpace(out[len(out)-1])
		scope, ok := w.reg.scopeOf(tok)
		if !ok {
			return fmt.Errorf("%q is not a token the registry's realm issued. The realm is in the WWW-Authenticate header of the 401 that GET /v2/ answers with", tok)
		}
		if want := "repository:" + repo + ":pull"; scope != want {
			return fmt.Errorf("the token was issued for scope %q; pulling %s needs %q", scope, repo, want)
		}
		return nil
	}

	noCreds := w.root + "/no-docker-config"
	res, err := w.app([]string{"DOCKER_CONFIG=" + noCreds}, "auth", w.reg.ref("public/demo", "v1"))
	if err != nil {
		return err
	}
	if err := wantToken(res, "public/demo"); err != nil {
		return fmt.Errorf("an anonymous token for a public repository, with no config.json in DOCKER_CONFIG: %w", err)
	}

	good := w.root + "/docker"
	if err := node.WriteFile(ctx, good+"/config.json", dockerConfig(w.reg.host, user, pass), 0o600); err != nil {
		return err
	}
	res, err = w.app([]string{"DOCKER_CONFIG=" + good}, "auth", w.reg.ref("private/demo", "v1"))
	if err != nil {
		return err
	}
	if err := wantToken(res, "private/demo"); err != nil {
		return fmt.Errorf("a private repository, with credentials for %s in $DOCKER_CONFIG/config.json: %w", w.reg.host, err)
	}
	sent := false
	for _, tr := range w.reg.tokenRequests() {
		sent = sent || (tr.granted && tr.user == user)
	}
	if !sent {
		return fmt.Errorf("the private token was never requested with the config.json credentials as HTTP Basic auth")
	}

	res, err = w.app([]string{"DOCKER_CONFIG=" + noCreds}, "auth", w.reg.ref("private/demo", "v1"))
	if err != nil {
		return err
	}
	if err := wantError(res, "a private repository with no credentials gets no token"); err != nil {
		return err
	}

	bad := w.root + "/bad-docker"
	if err := node.WriteFile(ctx, bad+"/config.json", dockerConfig(w.reg.host, user, "wrong"), 0o600); err != nil {
		return err
	}
	res, err = w.app([]string{"DOCKER_CONFIG=" + bad}, "auth", w.reg.ref("private/demo", "v1"))
	if err != nil {
		return err
	}
	return wantError(res, "the realm refuses a wrong password with 401")
}

// manifestLines is what `manifest` prints for an image.
func manifestLines(p pushed) []string {
	out := []string{"manifest " + p.manifest, "config " + p.config}
	for _, l := range p.layers {
		out = append(out, "layer "+l)
	}
	return out
}

func wantLines(stdout string, want []string, what string) error {
	if got := lines(stdout); strings.Join(got, "\n") != strings.Join(want, "\n") {
		return fmt.Errorf("%s: expected exactly these lines, in this order:\n  %s\ngot:\n%s",
			what, strings.Join(want, "\n  "), tail(stdout))
	}
	return nil
}

// stageManifestFetch — a tag resolves to an image index, the index to this
// platform's manifest, and the manifest to a config and layers; every
// manifest fetched by digest is checked against that digest.
func stageManifestFetch(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "manifest-fetch", bin)
	if err != nil {
		return err
	}
	defer w.close()

	demo, err := w.pushDemo("public/demo", w.demo())
	if err != nil {
		return err
	}
	ref := w.reg.ref("public/demo", "v1")
	res, err := w.app(nil, "manifest", ref)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantLines(res.Stdout, manifestLines(demo), "the v1 tag is an image index; pick the linux manifest for this node's architecture"); err != nil {
		return err
	}

	byDigest := w.reg.host + "/public/demo@" + demo.manifest
	res, err = w.app(nil, "manifest", byDigest)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("a reference by digest (%s): %w", byDigest, err)
	}
	if err := wantLines(res.Stdout, manifestLines(demo), "a reference by digest names the manifest itself"); err != nil {
		return err
	}

	single := w.variant("single", []string{"VARIANT=single"})
	arch, _ := runner.NodeArch(ctx)
	single.config["architecture"] = arch
	sp := w.reg.push("public/single", "v1", single)
	res, err = w.app(nil, "manifest", w.reg.ref("public/single", "v1"))
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("a tag that points straight at an image manifest, with no index: %w", err)
	}
	if err := wantLines(res.Stdout, manifestLines(sp), "a tag that points straight at an image manifest"); err != nil {
		return err
	}

	t := w.variant("tampered", []string{"VARIANT=tampered"})
	tp, err := w.pushDemo("public/tampered", t)
	if err != nil {
		return err
	}
	w.reg.tamper(tp.manifest)
	res, err = w.app(nil, "manifest", w.reg.ref("public/tampered", "v1"))
	if err != nil {
		return err
	}
	return wantError(res, "the registry served bytes for "+tp.manifest+" that do not hash to it")
}

// stageLayerPull — the config and every layer land in a content-addressed
// store under --root, verified against their digests; a second pull fetches
// nothing it already has, and a corrupt layer is never stored.
func stageLayerPull(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "layer-pull", bin)
	if err != nil {
		return err
	}
	defer w.close()

	demo, err := w.pushDemo("public/demo", w.demo())
	if err != nil {
		return err
	}
	ref := w.reg.ref("public/demo", "v1")
	res, err := w.app(nil, "pull", "--root", w.root, ref)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	for _, d := range append([]string{demo.config}, demo.layers...) {
		hex := strings.TrimPrefix(d, "sha256:")
		path := w.root + "/blobs/sha256/" + hex
		out, err := w.sh(fmt.Sprintf(`sha256sum %q 2>/dev/null || true`, path))
		if err != nil {
			return err
		}
		if !strings.HasPrefix(out, hex) {
			return fmt.Errorf("after pull, %s should hold blob %s, verified; sha256sum says %q", path, d, strings.TrimSpace(out))
		}
	}

	before := w.reg.blobGetCount()
	res, err = w.app(nil, "pull", "--root", w.root, ref)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if n := w.reg.blobGetCount() - before; n != 0 {
		return fmt.Errorf("a second pull of the same image fetched %d blob(s) again; every one was already in the store", n)
	}

	bad := w.variant("corrupt", []string{"VARIANT=corrupt"})
	bp, err := w.pushDemo("public/corrupt", bad)
	if err != nil {
		return err
	}
	top := bp.layers[len(bp.layers)-1]
	w.reg.tamper(top)
	res, err = w.app(nil, "pull", "--root", w.root, w.reg.ref("public/corrupt", "v1"))
	if err != nil {
		return err
	}
	if err := wantError(res, "the registry served bytes for layer "+top+" that do not hash to it"); err != nil {
		return err
	}
	out, err := w.sh(fmt.Sprintf(`ls %s/blobs/sha256/%s 2>/dev/null || true`, w.root, strings.TrimPrefix(top, "sha256:")))
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) != "" {
		return fmt.Errorf("the corrupt layer was stored under its digest anyway; verify before the blob takes its final name")
	}
	return nil
}

// stageUnpack — layers applied bottom to top make one directory: modes,
// owners, symlinks and hard links as the tarballs say, whiteouts deleting
// what lower layers added, and nothing written outside the destination.
func stageUnpack(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "unpack", bin)
	if err != nil {
		return err
	}
	defer w.close()

	if _, err := w.pushDemo("public/demo", w.demo()); err != nil {
		return err
	}
	dest := w.root + "/rootfs"
	res, err := w.app(nil, "unpack", "--root", w.root, w.reg.ref("public/demo", "v1"), dest)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := w.wantTree(dest, "the unpacked directory"); err != nil {
		return err
	}
	inodes, err := w.sh(fmt.Sprintf(`stat -c %%i %[1]s/bin/probe %[1]s/bin/probe-hard`, dest))
	if err != nil {
		return err
	}
	if f := strings.Fields(inodes); len(f) != 2 || f[0] != f[1] {
		return fmt.Errorf("bin/probe-hard is a hard link to bin/probe in the layer, so both names should share one inode; got inodes %v", f)
	}

	// A layer naming a path above its root must not write there.
	escapee := "escaped-" + w.token
	evil := newImage(map[string]any{"architecture": "", "os": "linux", "config": map[string]any{}},
		[]entry{dir("etc/"), file("../../"+escapee, "outside", 0o644), file("etc/ok", "ok", 0o644)})
	if _, err := w.pushDemo("public/evil", evil); err != nil {
		return err
	}
	evilDest := w.root + "/evil/rootfs"
	if _, err := w.app(nil, "unpack", "--root", w.root, w.reg.ref("public/evil", "v1"), evilDest); err != nil {
		return err
	}
	found, err := w.sh(fmt.Sprintf(`find %s -name %q | grep -v '^%s/' || true`, w.root, escapee, evilDest))
	if err != nil {
		return err
	}
	if strings.TrimSpace(found) != "" {
		return fmt.Errorf("a layer entry named ../../%s was written outside the destination, at %s. Clean every name against the root before joining it",
			escapee, strings.TrimSpace(found))
	}
	return nil
}

// overlayMount is one overlay mount as the node's mount table shows it.
type overlayMount struct {
	lower          []string
	upper, workdir string
}

func (w *work) overlayAt(target string) (*overlayMount, error) {
	out, err := w.sh(fmt.Sprintf(`findmnt -n -o FSTYPE,OPTIONS --mountpoint %q || true`, target))
	if err != nil {
		return nil, err
	}
	f := strings.Fields(out)
	if len(f) != 2 || f[0] != "overlay" {
		return nil, fmt.Errorf("no overlay is mounted at %s (findmnt says %q)", target, strings.TrimSpace(out))
	}
	m := &overlayMount{}
	for _, opt := range strings.Split(f[1], ",") {
		k, v, _ := strings.Cut(opt, "=")
		switch k {
		case "lowerdir":
			m.lower = strings.Split(v, ":")
		case "upperdir":
			m.upper = v
		case "workdir":
			m.workdir = v
		}
	}
	return m, nil
}

// stageOverlayRootfs — each layer is extracted once into its own directory,
// and a container's root is an overlay of them with a writable layer of its
// own on top; two containers share the layers and not each other's writes.
func stageOverlayRootfs(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "overlay-rootfs", bin)
	if err != nil {
		return err
	}
	defer w.close()

	demo, err := w.pushDemo("public/demo", w.demo())
	if err != nil {
		return err
	}
	ref := w.reg.ref("public/demo", "v1")
	mount := func(id string) (string, *overlayMount, error) {
		res, err := w.app(nil, "mount", "--root", w.root, ref, id)
		if err != nil {
			return "", nil, err
		}
		if err := wantExit(res, 0); err != nil {
			return "", nil, err
		}
		out := lines(res.Stdout)
		if len(out) == 0 {
			return "", nil, fmt.Errorf("mount printed nothing; it should print the rootfs path")
		}
		path := strings.TrimSpace(out[len(out)-1])
		m, err := w.overlayAt(path)
		return path, m, err
	}

	first, m1, err := mount("c1")
	if err != nil {
		return err
	}
	if len(m1.lower) != len(demo.layers) || m1.upper == "" || m1.workdir == "" {
		return fmt.Errorf("the overlay at %s should have one lowerdir per layer (%d) and an upperdir and workdir; it has lowerdir=%v upperdir=%q workdir=%q",
			first, len(demo.layers), m1.lower, m1.upper, m1.workdir)
	}
	if err := w.wantTree(first, "the container's rootfs"); err != nil {
		return fmt.Errorf("%w\n(an overlay whiteout is a 0/0 character device; an opaque directory carries the xattr trusted.overlay.opaque=y)", err)
	}

	if _, err := w.sh(fmt.Sprintf(`echo written > %q/work/written`, first)); err != nil {
		return err
	}
	if _, err := w.sh(fmt.Sprintf(`test -f %q/work/written`, m1.upper)); err != nil {
		return fmt.Errorf("a write into the rootfs should land in its upperdir %s", m1.upper)
	}
	for _, l := range m1.lower {
		if _, err := w.sh(fmt.Sprintf(`test ! -e %q/work/written`, l)); err != nil {
			return fmt.Errorf("a write into the container's rootfs reached the layer directory %s; layers are shared and must stay read-only", l)
		}
	}

	second, m2, err := mount("c2")
	if err != nil {
		return err
	}
	if !slices.Equal(m1.lower, m2.lower) {
		return fmt.Errorf("a second container from the same image should reuse the extracted layers; c1 has lowerdir=%v and c2 has %v", m1.lower, m2.lower)
	}
	if m2.upper == m1.upper {
		return fmt.Errorf("two containers share the upperdir %s; each needs its own", m1.upper)
	}
	if _, err := w.sh(fmt.Sprintf(`test ! -e %q/work/written`, second)); err != nil {
		return fmt.Errorf("c2 sees the file c1 wrote; its writable layer must be its own")
	}

	for _, c := range []struct{ id, path string }{{"c1", first}, {"c2", second}} {
		res, err := w.app(nil, "umount", "--root", w.root, c.id)
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return fmt.Errorf("umount %s: %w", c.id, err)
		}
		if _, err := w.sh(fmt.Sprintf(`test ! -e %q`, c.path)); err != nil {
			return fmt.Errorf("after umount %s, %s should be unmounted and removed along with the container's directory", c.id, c.path)
		}
	}
	return nil
}

// spec is what `config` prints: the process an image means.
type spec struct {
	args []string
	cwd  string
	env  []string
}

func parseSpec(stdout string) (spec, error) {
	var s spec
	for _, l := range lines(stdout) {
		k, v, _ := strings.Cut(l, " ")
		switch k {
		case "args":
			if err := json.Unmarshal([]byte(v), &s.args); err != nil {
				return s, fmt.Errorf("args should be a JSON array of strings: %q: %v", v, err)
			}
		case "cwd":
			s.cwd = v
		case "env":
			s.env = append(s.env, v)
		}
	}
	return s, nil
}

// stageImageConfig — the image config becomes a process: entrypoint plus
// command, with arguments on the command line replacing the command, the
// image's environment and working directory, and defaults where it is silent.
func stageImageConfig(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "image-config", bin)
	if err != nil {
		return err
	}
	defer w.close()

	if _, err := w.pushDemo("public/demo", w.demo()); err != nil {
		return err
	}
	bare := w.variant("bare", nil)
	bare.config["config"] = map[string]any{"Cmd": []string{"/bin/probe", "only-cmd"}}
	if _, err := w.pushDemo("public/bare", bare); err != nil {
		return err
	}
	defaultPath := "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

	cases := []struct {
		repo string
		args []string
		want spec
		why  string
	}{
		{"public/demo", nil, spec{[]string{"/bin/probe", "from-cmd"}, "/work", []string{"PATH=/bin", "GREETING=" + w.greeting()}},
			"Entrypoint followed by Cmd"},
		{"public/demo", []string{"x", "y"}, spec{[]string{"/bin/probe", "x", "y"}, "/work", []string{"PATH=/bin", "GREETING=" + w.greeting()}},
			"arguments replace Cmd and keep Entrypoint"},
		{"public/bare", nil, spec{[]string{"/bin/probe", "only-cmd"}, "/", []string{defaultPath}},
			"no Entrypoint: Cmd alone; no WorkingDir: /; no PATH: the default one"},
		{"public/bare", []string{"/bin/probe-link", "z"}, spec{[]string{"/bin/probe-link", "z"}, "/", []string{defaultPath}},
			"no Entrypoint: arguments replace Cmd entirely"},
	}
	for _, c := range cases {
		res, err := w.app(nil, append([]string{"config", "--root", w.root, w.reg.ref(c.repo, "v1")}, c.args...)...)
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return err
		}
		got, err := parseSpec(res.Stdout)
		if err != nil {
			return err
		}
		if !slices.Equal(got.args, c.want.args) || got.cwd != c.want.cwd || !slices.Equal(got.env, c.want.env) {
			return fmt.Errorf("config %s %v (%s): expected\n  args %q\n  cwd %s\n  env %q\ngot\n  args %q\n  cwd %s\n  env %q",
				c.repo, c.args, c.why, c.want.args, c.want.cwd, c.want.env, got.args, got.cwd, got.env)
		}
	}
	return nil
}

// report is what the probe prints about the process it finds itself in.
type report struct {
	args    []string
	env     []string
	cwd     string
	pid     string
	marker  string
	exists  map[string]string
	mntns   string
	procs   string
	mounts  []string
	escaped string
}

func parseReport(stdout string) (*report, error) {
	r := &report{exists: map[string]string{}}
	done := false
	for _, l := range lines(stdout) {
		k, v, _ := strings.Cut(l, "=")
		switch {
		case k == "args":
			json.Unmarshal([]byte(v), &r.args)
		case k == "env":
			r.env = append(r.env, v)
		case k == "cwd":
			r.cwd = v
		case k == "pid":
			r.pid = v
		case k == "marker":
			r.marker = v
		case strings.HasPrefix(k, "exists "):
			r.exists[strings.TrimPrefix(k, "exists ")] = v
		case k == "mntns":
			r.mntns = v
		case k == "procs":
			r.procs = v
		case k == "mount":
			r.mounts = append(r.mounts, v)
		case k == "escaped":
			r.escaped = v
		case l == "end":
			done = true
		}
	}
	if !done {
		return nil, fmt.Errorf("the image's /bin/probe never finished its report; its stdout should reach the runtime's stdout:\n%s", tail(stdout))
	}
	return r, nil
}

// run is `app run` one-shot, returning the result and the probe's report.
func (w *work) run(env []string, args ...string) (*runner.Result, *report, error) {
	res, err := w.app(env, append([]string{"run", "--root", w.root, w.reg.ref("public/demo", "v1")}, args...)...)
	if err != nil {
		return nil, nil, err
	}
	r, err := parseReport(res.Stdout)
	if err != nil {
		return res, nil, fmt.Errorf("run exited %d: %w\nstderr:\n%s", res.ExitCode, err, tail(res.Stderr))
	}
	return res, r, nil
}

// stageSpawn — `run` turns the image into a running process: the config's
// arguments, environment and working directory, the program's output passed
// through, its exit code returned, and its rootfs gone when it exits.
func stageSpawn(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "spawn", bin)
	if err != nil {
		return err
	}
	defer w.close()
	if _, err := w.pushDemo("public/demo", w.demo()); err != nil {
		return err
	}

	res, r, err := w.run([]string{"LEAK=from-the-runtime"}, "exit=3", "hello")
	if err != nil {
		return err
	}
	if err := wantExit(res, 3); err != nil {
		return fmt.Errorf("the container exited 3, and run should exit with its code: %w", err)
	}
	if !slices.Equal(r.args, []string{"exit=3", "hello"}) {
		return fmt.Errorf("the probe was given %q; run's arguments after the image replace its Cmd", r.args)
	}
	for _, want := range []string{"PATH=/bin", "GREETING=" + w.greeting()} {
		if !slices.Contains(r.env, want) {
			return fmt.Errorf("the container's environment %q lacks %s from the image config", r.env, want)
		}
	}
	for _, e := range r.env {
		if strings.HasPrefix(e, "LEAK=") {
			return fmt.Errorf("the container inherited the runtime's own environment (%s); it gets the image's, nothing else", e)
		}
	}
	if !strings.HasSuffix(r.cwd, "/work") {
		return fmt.Errorf("the container started in %s; the image's WorkingDir is /work", r.cwd)
	}

	dirs := func() (string, error) {
		return w.sh(fmt.Sprintf(`find %s -mindepth 1 -maxdepth 3 -type d | sort`, w.root))
	}
	after1, err := dirs()
	if err != nil {
		return err
	}
	res, r, err = w.run(nil)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if !slices.Equal(r.args, []string{"from-cmd"}) {
		return fmt.Errorf("with no arguments the container should get the image's Cmd [from-cmd], got %q", r.args)
	}
	after2, err := dirs()
	if err != nil {
		return err
	}
	if after1 != after2 {
		return fmt.Errorf("each run leaves a directory behind in --root; once the container exits its rootfs and writable layer should go:\nafter one run:\n%s\nafter two:\n%s", after1, after2)
	}
	if m, err := w.mountsUnder(); err != nil {
		return err
	} else if len(m) > 0 {
		return fmt.Errorf("mounts are left behind after the container exited: %v", m)
	}
	return nil
}

// running is a container started with `run … sleep`, its report, and the
// pid the node knows its probe by.
type running struct {
	p      *runner.Process
	report *report
	pid    string
}

func (w *work) start(args ...string) (*running, error) {
	tag := "token=" + randHex(4)
	p, err := node.Start(w.ctx, nil, append(append([]string{"run", "--root", w.root, w.reg.ref("public/demo", "v1")}, args...), "sleep", tag)...)
	if err != nil {
		return nil, err
	}
	c := &running{p: p}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if strings.Contains(p.Stdout(), "\nend\n") {
			break
		}
		if exited, res := p.Exited(); exited {
			return nil, fmt.Errorf("run exited %d before the container finished its report\nstdout:\n%s\nstderr:\n%s", res.ExitCode, tail(res.Stdout), tail(res.Stderr))
		}
		if time.Now().After(deadline) {
			res := p.Stop(5 * time.Second)
			return nil, fmt.Errorf("the container never finished its report\nstdout:\n%s\nstderr:\n%s", tail(res.Stdout), tail(res.Stderr))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if c.report, err = parseReport(p.Stdout()); err != nil {
		p.Stop(5 * time.Second)
		return nil, err
	}
	out, err := w.sh(fmt.Sprintf(`for p in $(pgrep -f %q); do [ "$(cat /proc/$p/comm)" = probe ] && echo $p; done; true`, tag))
	if err != nil {
		p.Stop(5 * time.Second)
		return nil, err
	}
	if f := strings.Fields(out); len(f) == 1 {
		c.pid = f[0]
	} else {
		p.Stop(5 * time.Second)
		return nil, fmt.Errorf("expected one probe process in the node for this container, found %v", f)
	}
	return c, nil
}

// stop stops the container's runtime and requires the probe gone with it.
func (w *work) stop(c *running) error {
	c.p.Stop(5 * time.Second)
	if _, err := w.sh(fmt.Sprintf(`test ! -d /proc/%s`, c.pid)); err != nil {
		return fmt.Errorf("the container's process %s outlived the runtime being stopped", c.pid)
	}
	return nil
}

// stageMountNS — the container gets its own mount namespace: its rootfs is
// mounted inside it, so the node's mount table never shows it, and it is gone
// when the container's last process exits.
func stageMountNS(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "mount-ns", bin)
	if err != nil {
		return err
	}
	defer w.close()
	if _, err := w.pushDemo("public/demo", w.demo()); err != nil {
		return err
	}

	c, err := w.start()
	if err != nil {
		return err
	}
	// Read from the node's side: until stage pivot-root there is no /proc
	// inside the container for the probe to read its own namespace from.
	ns, err := w.sh(fmt.Sprintf(`readlink /proc/1/ns/mnt /proc/%s/ns/mnt`, c.pid))
	if err != nil {
		w.stop(c)
		return err
	}
	if f := strings.Fields(ns); len(f) != 2 || f[0] == f[1] {
		w.stop(c)
		return fmt.Errorf("the container is in the node's mount namespace (%s); start it with CLONE_NEWNS", strings.Join(f, " vs "))
	}
	leaked, err := w.mountsUnder()
	if err != nil {
		w.stop(c)
		return err
	}
	if len(leaked) > 0 {
		w.stop(c)
		return fmt.Errorf("while the container runs, the node's own mount table shows %v. Mount the rootfs from inside the new namespace, after making / private", leaked)
	}
	inside, err := w.sh(fmt.Sprintf(`grep -c ' - overlay ' /proc/%s/mountinfo || true`, c.pid))
	if err != nil {
		w.stop(c)
		return err
	}
	if strings.TrimSpace(inside) == "0" {
		w.stop(c)
		return fmt.Errorf("the container's mount namespace has no overlay mounted; its rootfs should be one")
	}
	if err := w.stop(c); err != nil {
		return err
	}
	if m, err := w.mountsUnder(); err != nil {
		return err
	} else if len(m) > 0 {
		return fmt.Errorf("mounts are left in the node after the container stopped: %v", m)
	}
	return nil
}

// stagePivotRoot — the container's / is its rootfs: the node's root is not
// beneath it, not reachable by climbing out of a chroot, and not in its mount
// table; /proc is mounted, and it starts in the image's WorkingDir.
func stagePivotRoot(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "pivot-root", bin)
	if err != nil {
		return err
	}
	defer w.close()
	if _, err := w.pushDemo("public/demo", w.demo()); err != nil {
		return err
	}

	res, r, err := w.run(nil, "escape", "exists=/kind", "exists=/bin/probe-link")
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	switch {
	case r.cwd != "/work":
		return fmt.Errorf("the container started in %q; inside its own root that is /work", r.cwd)
	case r.marker != w.marker("demo"):
		return fmt.Errorf("/etc/byok8s-image in the container reads %q, not the image's %q: / is not the rootfs", r.marker, w.marker("demo"))
	case r.exists["/bin/probe-link"] != "true":
		return fmt.Errorf("/bin/probe-link should exist inside the container's root")
	case r.exists["/kind"] != "false":
		return fmt.Errorf("the container can see /kind, which is the node's; its root should be the image")
	case r.mntns == "":
		return fmt.Errorf("the container cannot read /proc/self; mount a proc filesystem at /proc inside the rootfs")
	case r.escaped == "yes":
		return fmt.Errorf("from inside the container, chroot into a subdirectory and climb with .. reached the node's root. chroot moves / for one process; pivot_root moves it for the mount namespace, and the old root has to be unmounted")
	case r.escaped != "no":
		return fmt.Errorf("the escape check could not run inside the container: %s", r.escaped)
	}
	for _, m := range r.mounts {
		if m != "/" && !strings.HasPrefix(m, "/proc") && !strings.HasPrefix(m, "/dev") && !strings.HasPrefix(m, "/sys") {
			return fmt.Errorf("the container's mount table shows %s. After pivot_root, unmount the old root (MNT_DETACH) and remove its directory; mounts: %v", m, r.mounts)
		}
	}
	return nil
}

// stagePidNS — the container has its own pid namespace: the image's process
// is pid 1 and sees no process but itself, while the node sees it as an
// ordinary process it can stop.
func stagePidNS(ctx context.Context, _ *kube.Env, bin string) error {
	w, err := newWork(ctx, "pid-ns", bin)
	if err != nil {
		return err
	}
	defer w.close()
	if _, err := w.pushDemo("public/demo", w.demo()); err != nil {
		return err
	}

	res, r, err := w.run(nil, "exit=5")
	if err != nil {
		return err
	}
	if err := wantExit(res, 5); err != nil {
		return fmt.Errorf("pid 1 exited 5: %w", err)
	}
	if r.pid != "1" {
		return fmt.Errorf("the image's process is pid %s; in a new pid namespace the first process is 1. Exec it from the process cloned with CLONE_NEWPID rather than forking it", r.pid)
	}
	if r.procs != "1" {
		return fmt.Errorf("the container's /proc lists %s processes; a proc mounted inside the new pid namespace shows only the container's own", r.procs)
	}

	c, err := w.start()
	if err != nil {
		return err
	}
	if c.report.pid != "1" || c.pid == "1" {
		w.stop(c)
		return fmt.Errorf("inside, the probe is pid %s; the node knows it as %s", c.report.pid, c.pid)
	}
	return w.stop(c)
}
