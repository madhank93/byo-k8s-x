// Package kustomize holds the assertions for the "Build your own kustomize"
// course — one function per stage, registered by slug.
//
// Every stage writes kustomization trees to a temporary directory and runs
// `build DIR` on both the learner's program and the kustomize built into the
// pinned kubectl. Until the last stage the two outputs are compared as data,
// document by document and in order; the last stage compares them byte for
// byte. No cluster is involved.
package kustomize

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

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
	register(Stage{Slug: "kustomization-load", Run: stageKustomizationLoad})
	register(Stage{Slug: "resources", Run: stageResources})
}

// tree is a kustomization directory as slash paths to contents.
type tree map[string]string

// check is one directory to build, and what it teaches when it fails.
type check struct {
	why  string
	root string // the directory to build, relative to the tree
	t    tree
}

// stageKustomizationLoad — the kustomization file is found under any of its
// three names, read strictly, and the files it lists are printed back as one
// YAML stream; every way the file can be wrong is refused.
func stageKustomizationLoad(ctx context.Context, _ *kube.Env, bin string) error {
	cm := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\ndata:\n  mode: fast\n  retries: \"3\"\n"
	svc := "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\nspec:\n  ports:\n  - port: 80\n    targetPort: 8080\n"
	return runChecks(ctx, bin, []check{
		{"kustomization.yaml lists two files, printed in the order listed", ".", tree{
			"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n- cm.yaml\n- svc.yaml\n",
			"cm.yaml":            cm, "svc.yaml": svc,
		}},
		{"kustomization.yml is the second name kustomize looks for, and apiVersion and kind are optional", ".", tree{
			"kustomization.yml": "resources:\n- svc.yaml\n", "svc.yaml": svc,
		}},
		{"Kustomization, capitalised and with no extension, is the third", ".", tree{
			"Kustomization": "resources: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"a resource may leave out apiVersion; kind and metadata.name are what kustomize insists on", ".", tree{
			"kustomization.yaml": "resources: [bare.yaml]\n", "bare.yaml": "kind: ConfigMap\nmetadata:\n  name: bare\n",
		}},
		{"a resource file can live in a subdirectory", ".", tree{
			"kustomization.yaml": "resources: [manifests/cm.yaml]\n", "manifests/cm.yaml": cm,
		}},
		{"there is no kustomization file at all", ".", tree{"cm.yaml": cm}},
		{"kustomization.yaml and kustomization.yml both exist, and kustomize will not guess", ".", tree{
			"kustomization.yaml": "resources: [cm.yaml]\n", "kustomization.yml": "resources: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"the kustomization file is empty", ".", tree{"kustomization.yaml": ""}},
		{"`resourcse` is not a field kustomize knows, and an unknown field is an error, not a typo to skip", ".", tree{
			"kustomization.yaml": "resourcse: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"resources is a list, not a string", ".", tree{
			"kustomization.yaml": "resources: cm.yaml\n", "cm.yaml": cm,
		}},
		{"kind must be Kustomization", ".", tree{
			"kustomization.yaml": "kind: Kustomisation\nresources: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"a Kustomization's apiVersion must be kustomize.config.k8s.io/v1beta1", ".", tree{
			"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v2\nkind: Kustomization\nresources: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"a listed file that does not exist", ".", tree{
			"kustomization.yaml": "resources: [cm.yaml, missing.yaml]\n", "cm.yaml": cm,
		}},
		{"a resource with no kind", ".", tree{
			"kustomization.yaml": "resources: [r.yaml]\n", "r.yaml": "apiVersion: v1\nmetadata:\n  name: x\n",
		}},
		{"a resource with no metadata.name", ".", tree{
			"kustomization.yaml": "resources: [r.yaml]\n", "r.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  labels:\n    a: b\n",
		}},
	})
}

// mixed is one file holding a little of everything, in no particular order,
// for the stages that judge kustomize's output order.
const mixed = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: nginx:1.27
---
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  selector:
    app: web
  ports:
  - port: 80
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: guard
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: b-settings
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: a-settings
  namespace: zeta
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: a-settings
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: a-settings
  namespace: alpha
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: deny
spec:
  podSelector: {}
---
apiVersion: v1
kind: Pod
metadata:
  name: solo
spec:
  containers:
  - name: c
    image: busybox
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: agent
---
apiVersion: v1
kind: Namespace
metadata:
  name: tools
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: reader
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: runner
`

// stageResources — resources come from multi-document files, from List
// objects, and from other kustomization directories built recursively; the
// whole result is ordered the way kustomize orders it, and two resources with
// the same identity, a cycle, or a file outside the root are refused.
func stageResources(ctx context.Context, _ *kube.Env, bin string) error {
	cm := func(name string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\n"
	}
	return runChecks(ctx, bin, []check{
		{"one file holding many documents comes out in kustomize's order: Namespace, then the kinds kustomize ranks first, then everything else by group, version and kind, webhooks last, and by name then namespace within a kind", ".", tree{
			"kustomization.yaml": "resources: [all.yaml]\n", "all.yaml": mixed,
		}},
		{"empty documents and comment-only documents are not resources", ".", tree{
			"kustomization.yaml": "resources: [a.yaml]\n",
			"a.yaml":             "---\n" + cm("one") + "---\n# only a comment\n---\n\n---\n" + cm("two"),
		}},
		{"a List is unwrapped into its items, and so is any kind ending in List", ".", tree{
			"kustomization.yaml": "resources: [list.yaml, cmlist.yaml]\n",
			"list.yaml":          "apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: Service\n  metadata:\n    name: s1\n- apiVersion: v1\n  kind: ConfigMap\n  metadata:\n    name: c1\n",
			"cmlist.yaml":        "apiVersion: v1\nkind: ConfigMapList\nitems:\n- apiVersion: v1\n  kind: ConfigMap\n  metadata:\n    name: c2\n",
		}},
		{"a directory in resources is another kustomization, built on its own and added in", "app", tree{
			"app/kustomization.yaml":        "resources:\n- ../base\n- extra.yaml\n",
			"app/extra.yaml":                cm("extra"),
			"base/kustomization.yaml":       "resources:\n- deploy.yaml\n- nested\n",
			"base/deploy.yaml":              "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: api\n",
			"base/nested/kustomization.yml": "resources: [cm.yaml]\n",
			"base/nested/cm.yaml":           cm("nested"),
		}},
		{"the same name in two namespaces is two resources", ".", tree{
			"kustomization.yaml": "resources: [a.yaml, b.yaml]\n",
			"a.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: same\n  namespace: a\n",
			"b.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: same\n  namespace: b\n",
		}},
		{"two resources with the same kind, name and namespace are one identity twice, which kustomize refuses", ".", tree{
			"kustomization.yaml": "resources: [a.yaml, b.yaml]\n", "a.yaml": cm("same"), "b.yaml": cm("same"),
		}},
		{"a duplicate arriving from a sub-kustomization is refused the same way", "app", tree{
			"app/kustomization.yaml": "resources: [../base, cm.yaml]\n", "app/cm.yaml": cm("shared"),
			"base/kustomization.yaml": "resources: [cm.yaml]\n", "base/cm.yaml": cm("shared"),
		}},
		{"a directory with no kustomization file cannot be a resource", ".", tree{
			"kustomization.yaml": "resources: [plain]\n", "plain/cm.yaml": cm("x"),
		}},
		{"a and b include each other, which is a cycle", "a", tree{
			"a/kustomization.yaml": "resources: [../b]\n", "b/kustomization.yaml": "resources: [../a]\n",
		}},
		{"a file outside the kustomization's own directory is refused: only directories may reach outside", "app", tree{
			"app/kustomization.yaml": "resources: [../shared/cm.yaml]\n", "shared/cm.yaml": cm("shared"),
		}},
		{"a file that is not YAML at all", ".", tree{
			"kustomization.yaml": "resources: [bad.yaml]\n", "bad.yaml": "kind: ConfigMap\nmetadata: {name: [\n",
		}},
	})
}

// runChecks builds every check with both programs and compares them as data.
func runChecks(ctx context.Context, bin string, checks []check) error {
	w, err := newWork(ctx, bin)
	if err != nil {
		return err
	}
	defer w.close()
	for i, c := range checks {
		if err := w.same(c, fmt.Sprintf("case%02d", i+1), false); err != nil {
			return err
		}
	}
	return nil
}

// work is one stage's scratch space.
type work struct {
	ctx context.Context
	bin string
	dir string
}

func newWork(ctx context.Context, bin string) (*work, error) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		return nil, fmt.Errorf("this course is graded against `kubectl kustomize`, and there is no kubectl on PATH: install one (mise install provisions the version the course is graded with)")
	}
	dir, err := os.MkdirTemp("", "byok8s-kustomize-")
	if err != nil {
		return nil, err
	}
	w := &work{ctx: ctx, bin: bin, dir: dir}
	res, err := runner.Invoke(ctx, "kubectl", stages.Timeout, "version", "--client", "-o", "json")
	if err != nil {
		w.close()
		return nil, err
	}
	var v struct {
		Kustomize string `json:"kustomizeVersion"`
	}
	if json.Unmarshal([]byte(res.Stdout), &v) != nil || !strings.HasPrefix(v.Kustomize, "v5.") {
		w.close()
		return nil, fmt.Errorf("this course is graded against the kustomize v5 built into kubectl; `kubectl version --client` reports kustomize %q", v.Kustomize)
	}
	return w, nil
}

func (w *work) close() { os.RemoveAll(w.dir) }

// write lays a tree out under name and returns its directory. A failed write
// is a harness fault, so it panics rather than threading an error through.
func (w *work) write(name string, t tree) string {
	root := filepath.Join(w.dir, name)
	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}
	for p, body := range t {
		path := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			panic(err)
		}
	}
	return root
}

// same builds one check with kubectl kustomize and with the program. Where
// kustomize refuses, the program must exit 1 with a reason on stderr and
// nothing on stdout; otherwise it must print the same documents in the same
// order — as data, or byte for byte when exact is set.
func (w *work) same(c check, name string, exact bool) error {
	dir := filepath.Join(w.write(name, c.t), filepath.FromSlash(c.root))
	want, err := runner.Invoke(w.ctx, "kubectl", stages.Timeout, "kustomize", dir)
	if err != nil {
		return err
	}
	got, err := runner.Invoke(w.ctx, w.bin, stages.Timeout, "build", dir)
	if err != nil {
		return err
	}
	shown := describe(c)
	if want.ExitCode != 0 {
		if got.ExitCode != 1 || strings.TrimSpace(got.Stderr) == "" || got.Stdout != "" {
			return fmt.Errorf("%s\nkustomize refuses this (%s), so the program must exit 1 with a reason on stderr and print nothing; it exited %d\nstdout:\n%s\nstderr:\n%s",
				shown, firstLine(want.Stderr, w.dir), got.ExitCode, tail(got.Stdout), tail(got.Stderr))
		}
		return nil
	}
	if got.ExitCode != 0 {
		return fmt.Errorf("%s\nkustomize builds this, but the program exited %d:\n%s", shown, got.ExitCode, tail(got.Stderr))
	}
	if exact {
		if got.Stdout != want.Stdout {
			return fmt.Errorf("%s\nexpected exactly (between the lines):\n-----\n%s-----\ngot:\n-----\n%s-----", shown, want.Stdout, got.Stdout)
		}
		return nil
	}
	return sameDocs(shown, got.Stdout, want.Stdout)
}

// sameDocs compares two YAML streams document by document.
func sameDocs(shown, got, want string) error {
	g, err := docs(got)
	if err != nil {
		return fmt.Errorf("%s\nthe output is not a YAML stream: %v\n%s", shown, err, tail(got))
	}
	e, err := docs(want)
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(e) || i < len(g); i++ {
		switch {
		case i >= len(g):
			return fmt.Errorf("%s\nexpected %d documents, got %d; the first missing one is:\n%s\nexpected the whole stream:\n%s", shown, len(e), len(g), indent(show(e[i])), indent(want))
		case i >= len(e):
			return fmt.Errorf("%s\nexpected %d documents, got %d; the first extra one is:\n%s", shown, len(e), len(g), indent(show(g[i])))
		case !reflect.DeepEqual(g[i], e[i]):
			return fmt.Errorf("%s\ndocument %d differs; expected:\n%s\ngot:\n%s\nexpected the whole stream:\n%s", shown, i+1, indent(show(e[i])), indent(show(g[i])), indent(want))
		}
	}
	return nil
}

// docs splits a stream on `---` lines and parses each non-empty document.
func docs(s string) ([]any, error) {
	var out []any
	for _, d := range strings.Split("\n"+s, "\n---\n") {
		var v any
		if err := yaml.Unmarshal([]byte(d), &v); err != nil {
			return nil, err
		}
		if v != nil {
			out = append(out, v)
		}
	}
	return out, nil
}

func show(v any) string {
	b, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// describe is a check as the learner needs to see it: the reason, then every
// file of the tree.
func describe(c check) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s — building %s of:\n", c.why, c.root)
	for _, p := range sortedKeys(c.t) {
		fmt.Fprintf(&b, "  ┌ %s\n", p)
		for _, l := range strings.Split(strings.TrimRight(c.t[p], "\n"), "\n") {
			fmt.Fprintf(&b, "  │ %s\n", l)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func sortedKeys(t tree) []string {
	var ks []string
	for k := range t {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// firstLine is kustomize's error, its first line only, with the scratch
// directory hidden.
func firstLine(s, dir string) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, dir, "DIR"))
	for _, l := range strings.Split(s, "\n") {
		if !strings.HasPrefix(l, "# Warning") {
			return l
		}
	}
	return s
}

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n  ")
}

// tail trims captured output to something readable in a failure message.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "  (nothing)"
	}
	l := strings.Split(s, "\n")
	if len(l) > 40 {
		l = append([]string{"…"}, l[len(l)-40:]...)
	}
	return "  " + strings.Join(l, "\n  ")
}
