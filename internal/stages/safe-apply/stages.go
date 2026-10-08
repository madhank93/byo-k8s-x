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

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	register(Stage{Slug: "structured-diff", Run: stageStructuredDiff})
	register(Stage{Slug: "diff-render", Run: stageDiffRender})
	register(Stage{Slug: "server-dry-run", Run: stageServerDryRun})
	register(Stage{Slug: "admission-preview", Run: stageAdmissionPreview})
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
  added: "1"
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

// stageStructuredDiff — one line per changed field, nothing for an unchanged
// object, and kubectl diff's exit codes.
func stageStructuredDiff(ctx context.Context, env *kube.Env, bin string) error {
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
	same := `apiVersion: v1
kind: ConfigMap
metadata:
  name: same
data:
  x: "1"
`
	m := w.file("m.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: web
data:
  a: 1x
  d: "4"
---
`+same+`---
apiVersion: v1
kind: ConfigMap
metadata:
  name: new
data:
  added: "1"
`)
	res, err := w.run("diff", "-f", m)
	if err != nil {
		return err
	}
	if err := wantExit(res, 1); err != nil {
		return fmt.Errorf("something would change, so diff exits 1: %w", err)
	}
	d := parseDiff(res.Stdout)
	if err := hasLines(d, "ConfigMap/web", `~ .data.a: "1" -> "1x"`, `- .data.b: "2"`, `+ .data.d: "4"`); err != nil {
		return fmt.Errorf("%w\nstdout:\n%s", err, tail(res.Stdout))
	}
	for _, l := range d["ConfigMap/web"] {
		if strings.Contains(l, ".data.c") {
			return fmt.Errorf("data.c was set by another manager and is not this manifest's to remove, but the diff says %q", l)
		}
	}
	if err := hasLines(d, "ConfigMap/new", `+ .data.added: "1"`); err != nil {
		return fmt.Errorf("%w\nstdout:\n%s", err, tail(res.Stdout))
	}
	if _, ok := d["ConfigMap/same"]; ok {
		return fmt.Errorf("ConfigMap/same would not change and should not be listed, got:\n%s", tail(res.Stdout))
	}

	res, err = w.run("diff", "-f", w.file("same.yaml", same))
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("nothing would change, so diff exits 0: %w", err)
	}
	if strings.TrimSpace(res.Stdout) != "" {
		return fmt.Errorf("nothing would change, so diff prints nothing, got:\n%s", tail(res.Stdout))
	}
	return nil
}

// stageDiffRender — the same comparison as a unified diff of YAML, the way
// kubectl diff hands it to diff(1).
func stageDiffRender(ctx context.Context, env *kube.Env, bin string) error {
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
	res, err := w.run("diff", "--unified", "-f", w.file("m.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: web
data:
  a: 1x
`))
	if err != nil {
		return err
	}
	if err := wantExit(res, 1); err != nil {
		return err
	}
	var headers, minusB, plusA int
	for _, l := range strings.Split(res.Stdout, "\n") {
		switch {
		case strings.HasPrefix(l, "---") || strings.HasPrefix(l, "+++"):
			headers++
		case (strings.HasPrefix(l, "-") || strings.HasPrefix(l, "+")) && strings.Contains(l, " c: "):
			return fmt.Errorf("data.c belongs to someone else and must not change, but the diff has %q", l)
		case strings.HasPrefix(l, "-") && strings.Contains(l, `b: "2"`):
			minusB++
		case strings.HasPrefix(l, "+") && strings.Contains(l, "a: 1x"):
			plusA++
		}
	}
	if headers < 2 || minusB != 1 || plusA != 1 {
		return fmt.Errorf("expected a unified diff with ---/+++ headers, a removed line for b: \"2\" and an added line for a: 1x, got:\n%s", tail(res.Stdout))
	}

	res, err = w.run("diff", "--unified", "-f", w.file("same.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: same\ndata:\n  x: \"1\"\n"))
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if strings.TrimSpace(res.Stdout) != "" {
		return fmt.Errorf("nothing would change, so the unified diff is empty, got:\n%s", tail(res.Stdout))
	}
	return nil
}

// deployment is a manifest for a Deployment of pause containers. Replicas are
// kept at zero wherever the stage allows, so the cluster runs no pods for it.
func deployment(name string, replicas int, image string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
spec:
  replicas: %[2]d
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
      containers:
      - name: app
        image: %[3]s
`, name, replicas, image)
}

const pause = "registry.k8s.io/pause:3.9"

// stageServerDryRun — the would-be object is the server's answer, so it carries
// the defaults only the server knows, and nothing is written.
func stageServerDryRun(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := w.applyAs("byok8s", deployment("old", 0, pause)); err != nil {
		return err
	}
	res, err := w.run("diff", "-f", w.file("m.yaml", deployment("old", 2, pause)+"---\n"+deployment("fresh", 0, pause)))
	if err != nil {
		return err
	}
	if err := wantExit(res, 1); err != nil {
		return err
	}
	d := parseDiff(res.Stdout)
	if err := hasLines(d, "Deployment/old", "~ .spec.replicas: 0 -> 2"); err != nil {
		return fmt.Errorf("%w\nstdout:\n%s", err, tail(res.Stdout))
	}
	if err := hasLines(d, "Deployment/fresh", "+ .spec.revisionHistoryLimit: 10", "+ .spec.progressDeadlineSeconds: 600"); err != nil {
		return fmt.Errorf("%w\nthe manifest sets neither field; the server defaults them, so only a server-side dry-run shows them\nstdout:\n%s", err, tail(res.Stdout))
	}

	dep, err := env.Deployment(ctx, "old")
	if err != nil {
		return err
	}
	if *dep.Spec.Replicas != 0 {
		return fmt.Errorf("diff changed the cluster: Deployment/old now has %d replicas — a dry-run must never be stored", *dep.Spec.Replicas)
	}
	if _, err := env.Deployment(ctx, "fresh"); err == nil {
		return fmt.Errorf("diff created Deployment/fresh — a dry-run must never be stored")
	}
	return nil
}

// denyMessage is what the stage's admission policy says when it refuses.
const denyMessage = "a ConfigMap here may not carry the key forbidden"

// stageAdmissionPreview — admission runs on a dry-run, so the diff shows what
// it would add and reports what it would refuse, without stopping the rest.
func stageAdmissionPreview(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	_, err = env.Client.CoreV1().LimitRanges(env.Namespace).Create(ctx, &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Name: "defaults"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type:    corev1.LimitTypeContainer,
			Default: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")},
		}}},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("seed LimitRange: %w", err)
	}
	cleanup, err := denyForbiddenKey(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := w.run("diff", "-f", w.file("m.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: bad
data:
  forbidden: "yes"
---
apiVersion: v1
kind: Pod
metadata:
  name: pod
spec:
  containers:
  - name: app
    image: `+pause+`
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: good
data:
  fine: "yes"
`))
	if err != nil {
		return err
	}
	if err := wantExit(res, 2); err != nil {
		return fmt.Errorf("the server refused one object, so the diff is incomplete and exits 2: %w", err)
	}
	var reported bool
	for _, l := range lines(res.Stdout) {
		if strings.HasPrefix(l, "ConfigMap/bad") && strings.Contains(l, "rejected") && strings.Contains(l, denyMessage) {
			reported = true
		}
	}
	if !reported {
		return fmt.Errorf("expected a line starting \"ConfigMap/bad\" that says rejected and carries the policy's message %q, got:\n%s", denyMessage, tail(res.Stdout))
	}
	d := parseDiff(res.Stdout)
	if err := hasLines(d, "ConfigMap/good", `+ .data.fine: "yes"`); err != nil {
		return fmt.Errorf("one refusal must not hide the rest of the diff: %w", err)
	}
	if !strings.Contains(strings.Join(d["Pod/pod"], "\n"), "250m") {
		return fmt.Errorf("the LimitRange defaults the container's cpu limit to 250m at admission, so the dry-run Pod carries it; got:\n%s", tail(res.Stdout))
	}
	return nil
}

// denyForbiddenKey installs a ValidatingAdmissionPolicy, bound to the stage's
// namespace only, and waits until the API server enforces it.
func denyForbiddenKey(ctx context.Context, env *kube.Env) (func(), error) {
	name := "byok8s-safe-apply-" + env.Namespace
	adm := env.Client.AdmissionregistrationV1()
	cleanup := func() {
		bg := context.Background()
		adm.ValidatingAdmissionPolicyBindings().Delete(bg, name, metav1.DeleteOptions{})
		adm.ValidatingAdmissionPolicies().Delete(bg, name, metav1.DeleteOptions{})
	}
	cleanup()
	fail := admissionv1.Fail
	_, err := adm.ValidatingAdmissionPolicies().Create(ctx, &admissionv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: admissionv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: &fail,
			MatchConstraints: &admissionv1.MatchResources{ResourceRules: []admissionv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionv1.RuleWithOperations{
					Operations: []admissionv1.OperationType{admissionv1.Create, admissionv1.Update},
					Rule:       admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"configmaps"}},
				},
			}}},
			Validations: []admissionv1.Validation{{
				Expression: "!has(object.data) || !('forbidden' in object.data)",
				Message:    denyMessage,
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create admission policy: %w", err)
	}
	_, err = adm.ValidatingAdmissionPolicyBindings().Create(ctx, &admissionv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: admissionv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        name,
			ValidationActions: []admissionv1.ValidationAction{admissionv1.Deny},
			MatchResources: &admissionv1.MatchResources{NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"kubernetes.io/metadata.name": env.Namespace},
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("bind admission policy: %w", err)
	}
	probe := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "probe"}, Data: map[string]string{"forbidden": "x"}}
	err = kube.WaitFor(ctx, "the admission policy to take effect", func(ctx context.Context) (bool, error) {
		_, err := env.Client.CoreV1().ConfigMaps(env.Namespace).Create(ctx, probe, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		return err != nil && strings.Contains(err.Error(), denyMessage), nil
	})
	if err != nil {
		cleanup()
		return nil, err
	}
	return cleanup, nil
}

// parseDiff groups diff output by object: an unindented line names an object,
// and the indented lines after it are its changes.
func parseDiff(out string) map[string][]string {
	d := map[string][]string{}
	cur := ""
	for _, l := range strings.Split(out, "\n") {
		if strings.TrimSpace(l) == "" {
			continue
		}
		if l[0] != ' ' && l[0] != '\t' {
			cur = strings.TrimSpace(l)
			d[cur] = d[cur]
			continue
		}
		d[cur] = append(d[cur], strings.TrimSpace(l))
	}
	return d
}

// hasLines checks an object is in the diff with each of the given changes.
func hasLines(d map[string][]string, obj string, want ...string) error {
	got, ok := d[obj]
	if !ok {
		return fmt.Errorf("expected %s in the diff, and it is not there", obj)
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			found = found || g == w
		}
		if !found {
			return fmt.Errorf("expected the line %q under %s, got:\n  %s", w, obj, strings.Join(got, "\n  "))
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
