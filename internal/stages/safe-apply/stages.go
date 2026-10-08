// Package safeapply holds the assertions for the "Build your own diff and safe
// apply" course — one function per stage, registered by slug.
//
// Every stage runs the learner's built binary against a manifest the stage
// writes, and judges it by exit code, stdout and the state the cluster is left
// in. The diff command follows kubectl diff's exit codes throughout: 0 means no
// change, 1 means changes, anything higher means the diff itself failed.
package safeapply

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/yaml"

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
	register(Stage{Slug: "manifest-load", Run: stageManifestLoad})
	register(Stage{Slug: "live-fetch", Run: stageLiveFetch})
	register(Stage{Slug: "three-way-merge", Run: stageThreeWayMerge})
}

// stageManifestLoad — every object in a multi-document file, in order, with
// empty and comment-only documents skipped and a malformed one refused.
func stageManifestLoad(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	good := w.file("good.yaml", `# a comment-only document, as editors leave behind
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: first
data:
  greeting: hello
---
---
{"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "second"}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: third
`)
	res, err := w.run("objects", "-f", good)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	got := lines(res.Stdout)
	want := []string{"ConfigMap/first", "Secret/second", "Deployment/third"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		return fmt.Errorf("expected exactly these lines, in file order:\n  %s\ngot:\n%s",
			strings.Join(want, "\n  "), tail(res.Stdout))
	}

	for name, body := range map[string]string{
		"no-kind.yaml": "apiVersion: v1\nmetadata:\n  name: x\n",
		"broken.yaml":  "apiVersion: v1\nkind: ConfigMap\nmetadata: [unclosed\n",
	} {
		res, err := w.run("objects", "-f", w.file(name, body))
		if err != nil {
			return err
		}
		if res.ExitCode == 0 {
			return fmt.Errorf("%s is not a usable manifest, but the program exited 0 with:\n%s", name, tail(res.Stdout))
		}
	}
	return nil
}

// stageLiveFetch — each object is looked up where it would be applied: the
// manifest's namespace if it names one, the context's if not, and none at all
// for a cluster-scoped kind.
func stageLiveFetch(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := env.SeedConfigMap(ctx, "exists"); err != nil {
		return err
	}

	m := w.file("m.yaml", fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: exists
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: missing
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns
  namespace: kube-system
---
apiVersion: v1
kind: Namespace
metadata:
  name: %s
`, env.Namespace))
	res, err := w.run("get", "-f", m)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	want := []string{
		"ConfigMap/exists present",
		"ConfigMap/missing absent",
		"ConfigMap/coredns present",
		"Namespace/" + env.Namespace + " present",
	}
	if got := lines(res.Stdout); strings.Join(got, "\n") != strings.Join(want, "\n") {
		return fmt.Errorf("expected:\n  %s\ngot:\n%s\n(coredns lives in kube-system, which the manifest names; a Namespace has no namespace of its own)",
			strings.Join(want, "\n  "), tail(res.Stdout))
	}

	unknown := w.file("unknown.yaml", "apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: w\n")
	res, err = w.run("get", "-f", unknown)
	if err != nil {
		return err
	}
	if res.ExitCode == 0 {
		return fmt.Errorf("the server serves no example.com/v1 Widget, so that is an error, not an absent object; got exit 0 and:\n%s", tail(res.Stdout))
	}
	return nil
}

// seedLastApplied sets up an object the way a client-side apply followed by
// someone else's edit leaves it: byok8s owns a and b, other-tool added c, and
// the last-applied annotation records {a, b}. The annotation belongs to
// other-tool, so a server-side apply by byok8s that omits it leaves it alone.
func seedLastApplied(w *work, name string, mine map[string]string, theirs map[string]string) error {
	body, _ := json.Marshal(mine)
	if err := w.applyAs("byok8s", fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: %s\ndata: %s\n", name, body)); err != nil {
		return err
	}
	last, _ := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name}, "data": mine,
	})
	other := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "annotations": map[string]string{
			"kubectl.kubernetes.io/last-applied-configuration": string(last),
		}},
	}
	if len(theirs) > 0 {
		other["data"] = theirs
	}
	b, _ := json.Marshal(other)
	return w.applyAs("other-tool", string(b))
}

// stageThreeWayMerge — the patch takes live to desired, removing only what the
// last-applied record says this manifest used to set.
func stageThreeWayMerge(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := seedLastApplied(w, "web", map[string]string{"a": "1", "b": "2"}, map[string]string{"c": "3"}); err != nil {
		return err
	}
	if err := seedLastApplied(w, "same", map[string]string{"x": "1"}, nil); err != nil {
		return err
	}

	m := w.file("m.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: web
data:
  a: 1x
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: same
data:
  x: "1"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: new
data:
  n: "1"
`)
	res, err := w.run("patch", "-f", m)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	got := map[string]string{}
	for _, l := range lines(res.Stdout) {
		k, v, _ := strings.Cut(l, " ")
		got[k] = v
	}
	if got["ConfigMap/new"] != "create" {
		return fmt.Errorf("ConfigMap/new does not exist yet, so expected the line \"ConfigMap/new create\", got:\n%s", tail(res.Stdout))
	}
	for name, want := range map[string]string{
		"ConfigMap/web":  `{"data":{"a":"1x","b":null}}`,
		"ConfigMap/same": `{}`,
	} {
		if err := sameJSON(got[name], want); err != nil {
			return fmt.Errorf("%s: expected the patch %s, got %q: %v\n(b was last applied and is gone from the manifest, so it is deleted; c was added by someone else, so it is kept)\nstdout:\n%s",
				name, want, got[name], err, tail(res.Stdout))
		}
	}
	return nil
}

// sameJSON compares two JSON documents by value, not by spelling.
func sameJSON(got, want string) error {
	var g, x any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		return fmt.Errorf("not JSON")
	}
	if err := json.Unmarshal([]byte(want), &x); err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(g, x) {
		return fmt.Errorf("different")
	}
	return nil
}

// gvrs are the kinds the stages seed. The harness knows them by name rather
// than through discovery, because the seeding must not depend on anything the
// learner is building.
var gvrs = map[string]schema.GroupVersionResource{
	"ConfigMap":  {Version: "v1", Resource: "configmaps"},
	"Secret":     {Version: "v1", Resource: "secrets"},
	"Deployment": {Group: "apps", Version: "v1", Resource: "deployments"},
}

// applyAs server-side applies a YAML or JSON object into the stage namespace
// as some other field manager, forcing, so a stage can stage ownership.
func (w *work) applyAs(manager, body string) error {
	js, err := yaml.YAMLToJSON([]byte(body))
	if err != nil {
		panic(err)
	}
	var meta struct {
		Kind     string
		Metadata struct{ Name string }
	}
	if err := json.Unmarshal(js, &meta); err != nil {
		panic(err)
	}
	dyn, err := dynamic.NewForConfig(w.env.Config)
	if err != nil {
		return err
	}
	force := true
	_, err = dyn.Resource(gvrs[meta.Kind]).Namespace(w.env.Namespace).Patch(w.ctx, meta.Metadata.Name,
		types.ApplyPatchType, js, metav1.PatchOptions{FieldManager: manager, Force: &force})
	if err != nil {
		return fmt.Errorf("seed %s/%s as %s: %w", meta.Kind, meta.Metadata.Name, manager, err)
	}
	return nil
}

// work is one stage's scratch space: a directory for manifests and the
// environment that points the program at the stage's namespace.
type work struct {
	ctx context.Context
	env *kube.Env
	bin string
	dir string
	kc  []string
}

func newWork(ctx context.Context, env *kube.Env, bin string) (*work, error) {
	dir, err := os.MkdirTemp("", "byok8s-safe-apply-")
	if err != nil {
		return nil, err
	}
	kc, err := env.KubeconfigScoped(dir)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	return &work{ctx: ctx, env: env, bin: bin, dir: dir, kc: []string{"KUBECONFIG=" + kc}}, nil
}

func (w *work) close() { os.RemoveAll(w.dir) }

// file writes a manifest and returns its path. A failed write is a harness
// fault, so it panics rather than threading an error through every stage.
func (w *work) file(name, body string) string {
	path := filepath.Join(w.dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		panic(err)
	}
	return path
}

func (w *work) run(args ...string) (*runner.Result, error) {
	return runner.InvokeEnv(w.ctx, w.bin, w.kc, stages.Timeout, args...)
}

func wantExit(res *runner.Result, code int) error {
	if res.ExitCode != code {
		return fmt.Errorf("expected exit %d, got %d\nstdout:\n%s\nstderr:\n%s", code, res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return nil
}

// lines is stdout split into trimmed, non-empty lines.
func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// tail trims captured output to something readable in a failure message.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "  (nothing)"
	}
	l := strings.Split(s, "\n")
	if len(l) > 20 {
		l = append([]string{"…"}, l[len(l)-20:]...)
	}
	return "  " + strings.Join(l, "\n  ")
}
