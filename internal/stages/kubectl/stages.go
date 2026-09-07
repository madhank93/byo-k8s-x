// Package kubectl holds the assertions for the "Build your own kubectl"
// course — one function per stage, registered by slug.
//
// Every stage runs the learner's already-built binary and judges it by its
// exit code and stdout. Nothing here inspects the learner's source: a stage
// that passes for the wrong reason is a bad stage, but a stage that greps for
// an API call is a worse one, because it forbids a correct alternative.
package kubectl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// StageTimeout bounds one stage.
const StageTimeout = stages.Timeout

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
	register(Stage{Slug: "server-url", Run: stageServerURL})
	register(Stage{Slug: "context-override", Run: stageContextOverride})
	register(Stage{Slug: "server-version", Run: stageServerVersion})
	register(Stage{Slug: "list-pods", Run: stageListPods})
	register(Stage{Slug: "namespace-flag", Run: stageNamespaceFlag})
	register(Stage{Slug: "table-output", Run: stageTableOutput})
	register(Stage{Slug: "age-column", Run: stageAgeColumn})
	register(Stage{Slug: "output-json", Run: stageOutputJSON})
	register(Stage{Slug: "output-yaml", Run: stageOutputYAML})
	register(Stage{Slug: "all-namespaces", Run: stageAllNamespaces})
	register(Stage{Slug: "get-by-name", Run: stageGetByName})
	register(Stage{Slug: "api-resources", Run: stageAPIResources})
	register(Stage{Slug: "rest-mapper", Run: stageRESTMapper})
	register(Stage{Slug: "dynamic-client", Run: stageDynamicClient})
	register(Stage{Slug: "output-wide", Run: stageOutputWide})
	register(Stage{Slug: "label-selector", Run: stageLabelSelector})
	register(Stage{Slug: "field-selector", Run: stageFieldSelector})
	register(Stage{Slug: "sorted-output", Run: stageSortedOutput})
	register(Stage{Slug: "watch", Run: stageWatch})
	register(Stage{Slug: "delete", Run: stageDelete})
	register(Stage{Slug: "create-from-file", Run: stageCreateFromFile})
	register(Stage{Slug: "apply-ssa", Run: stageApplySSA})
	register(Stage{Slug: "apply-conflict", Run: stageApplyConflict})
	register(Stage{Slug: "patch", Run: stagePatch})
	register(Stage{Slug: "scale", Run: stageScale})
	register(Stage{Slug: "describe", Run: stageDescribe})
	register(Stage{Slug: "describe-events", Run: stageDescribeEvents})
	register(Stage{Slug: "logs", Run: stageLogs})
	register(Stage{Slug: "exec", Run: stageExec})
	register(Stage{Slug: "port-forward", Run: stagePortForward})
}

// stageServerURL — the program prints the API server it would talk to,
// discovered through the standard kubeconfig loading rules.
func stageServerURL(ctx context.Context, env *kube.Env, bin string) error {
	res, err := runner.Invoke(ctx, bin, StageTimeout)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	want := env.ServerURL()
	if !strings.Contains(res.Stdout, want) {
		return fmt.Errorf("expected the server URL %q in stdout, got:\n%s", want, tail(res.Stdout))
	}
	return nil
}

// stageContextOverride — --context selects a context, and an unknown one is an
// error rather than a silent fallback to the current context.
func stageContextOverride(ctx context.Context, env *kube.Env, bin string) error {
	ok, err := runner.Invoke(ctx, bin, StageTimeout, "--context", "kind-byok8s")
	if err != nil {
		return err
	}
	if ok.ExitCode != 0 {
		return fmt.Errorf("with --context kind-byok8s: expected exit 0, got %d\nstderr:\n%s", ok.ExitCode, tail(ok.Stderr))
	}
	if !strings.Contains(ok.Stdout, env.ServerURL()) {
		return fmt.Errorf("with --context kind-byok8s: expected the server URL %q, got:\n%s", env.ServerURL(), tail(ok.Stdout))
	}

	bad, err := runner.Invoke(ctx, bin, StageTimeout, "--context", "no-such-context")
	if err != nil {
		return err
	}
	if bad.ExitCode == 0 {
		return fmt.Errorf("with --context no-such-context: expected a non-zero exit, got 0 and stdout:\n%s", tail(bad.Stdout))
	}
	return nil
}

var versionRE = regexp.MustCompile(`v\d+\.\d+\.\d+`)

// stageServerVersion — the program builds a client and asks the server what it
// is. Asserted by shape, never by value: the kind node image moves.
func stageServerVersion(ctx context.Context, env *kube.Env, bin string) error {
	res, err := runner.Invoke(ctx, bin, StageTimeout, "version")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if !versionRE.MatchString(res.Stdout) {
		return fmt.Errorf("expected a server version like v1.34.0 in stdout, got:\n%s", tail(res.Stdout))
	}
	return nil
}

// tail trims captured output to something readable in a failure message.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "(nothing)"
	}
	lines := strings.Split(s, "\n")
	if len(lines) > 12 {
		lines = append([]string{"…"}, lines[len(lines)-12:]...)
	}
	return "  " + strings.Join(lines, "\n  ")
}

// scoped seeds pods and hands back the env entry pointing the program at a
// kubeconfig whose context carries this stage's namespace.
func scoped(ctx context.Context, env *kube.Env, pods ...string) ([]string, func(), error) {
	dir, err := os.MkdirTemp("", "byok8s-stage-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }

	if err := env.SeedPods(ctx, pods...); err != nil {
		cleanup()
		return nil, nil, err
	}
	path, err := env.KubeconfigScoped(dir)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return []string{"KUBECONFIG=" + path}, cleanup, nil
}

// stageListPods — the program lists pods in the namespace its context names.
func stageListPods(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "alpha", "beta")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	for _, want := range []string{"alpha", "beta"} {
		if !strings.Contains(res.Stdout, want) {
			return fmt.Errorf("expected pod %q in the output, got:\n%s", want, tail(res.Stdout))
		}
	}
	return nil
}

// stageNamespaceFlag — -n beats the context, and its absence does not.
func stageNamespaceFlag(ctx context.Context, env *kube.Env, bin string) error {
	if err := env.SeedPods(ctx, "gamma"); err != nil {
		return err
	}

	// With the flag, the program must look where it was told.
	with, err := runner.Invoke(ctx, bin, StageTimeout, "get", "pods", "-n", env.Namespace)
	if err != nil {
		return err
	}
	if with.ExitCode != 0 {
		return fmt.Errorf("with -n %s: expected exit 0, got %d\nstderr:\n%s", env.Namespace, with.ExitCode, tail(with.Stderr))
	}
	if !strings.Contains(with.Stdout, "gamma") {
		return fmt.Errorf("with -n %s: expected pod \"gamma\", got:\n%s", env.Namespace, tail(with.Stdout))
	}

	// Without it, the default namespace — where that pod is not.
	without, err := runner.Invoke(ctx, bin, StageTimeout, "get", "pods")
	if err != nil {
		return err
	}
	if without.ExitCode != 0 {
		return fmt.Errorf("without -n: expected exit 0, got %d\nstderr:\n%s", without.ExitCode, tail(without.Stderr))
	}
	if strings.Contains(without.Stdout, "gamma") {
		return fmt.Errorf("without -n the program listed %s's pods; it should have used the default namespace:\n%s", env.Namespace, tail(without.Stdout))
	}
	return nil
}

var wsRun = regexp.MustCompile(`\s+`)

// stageTableOutput — a header and one aligned row per pod.
func stageTableOutput(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "delta")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	lines := nonEmptyLines(res.Stdout)
	if len(lines) < 2 {
		return fmt.Errorf("expected a header and at least one row, got:\n%s", tail(res.Stdout))
	}
	header := wsRun.Split(strings.TrimSpace(lines[0]), -1)
	if len(header) < 2 || header[0] != "NAME" || header[1] != "AGE" {
		return fmt.Errorf("expected a header starting NAME then AGE, got %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "delta") {
		return fmt.Errorf("expected the row to start with the pod name, got %q", lines[1])
	}
	return nil
}

var ageRE = regexp.MustCompile(`^\d+[smhd]$`)

// stageAgeColumn — the age is humanised, not a timestamp.
//
// Asserted by shape rather than value: the pod is seconds old when the stage
// runs and any exact number would be a race.
func stageAgeColumn(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "epsilon")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	lines := nonEmptyLines(res.Stdout)
	if len(lines) < 2 {
		return fmt.Errorf("expected a header and a row, got:\n%s", tail(res.Stdout))
	}
	cols := wsRun.Split(strings.TrimSpace(lines[1]), -1)
	if len(cols) < 2 {
		return fmt.Errorf("expected a NAME and an AGE column, got %q", lines[1])
	}
	if !ageRE.MatchString(cols[1]) {
		return fmt.Errorf("expected an age like 5s, 3m or 2d; got %q in row %q", cols[1], lines[1])
	}
	return nil
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

// stageOutputJSON — -o json emits the API's own object, not a summary of it.
//
// Asserted by decoding rather than by string match: any valid encoding of the
// list passes, which leaves the learner free to marshal it however they like.
func stageOutputJSON(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "zeta")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods", "-o", "json")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}

	var out struct {
		Kind  string `json:"kind"`
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &out); err != nil {
		return fmt.Errorf("stdout is not valid JSON (%v), got:\n%s", err, tail(res.Stdout))
	}
	if out.Kind != "PodList" {
		return fmt.Errorf("expected kind PodList, got %q — the list object carries its own kind", out.Kind)
	}
	if len(out.Items) != 1 || out.Items[0].Metadata.Name != "zeta" {
		return fmt.Errorf("expected one item named zeta, got %d items", len(out.Items))
	}
	return nil
}

// stageOutputYAML — the same object through the YAML serializer.
func stageOutputYAML(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "eta")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods", "-o", "yaml")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if strings.HasPrefix(strings.TrimSpace(res.Stdout), "{") {
		return fmt.Errorf("that is JSON, not YAML — the serializer differs, the object does not:\n%s", tail(res.Stdout))
	}
	for _, want := range []string{"kind: PodList", "name: eta"} {
		if !strings.Contains(res.Stdout, want) {
			return fmt.Errorf("expected %q in the YAML, got:\n%s", want, tail(res.Stdout))
		}
	}
	return nil
}

// stageAllNamespaces — -A changes both the request and the shape of the table.
func stageAllNamespaces(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "theta")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods", "-A")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	lines := nonEmptyLines(res.Stdout)
	if len(lines) < 2 {
		return fmt.Errorf("expected a header and rows, got:\n%s", tail(res.Stdout))
	}
	header := wsRun.Split(strings.TrimSpace(lines[0]), -1)
	if len(header) < 3 || header[0] != "NAMESPACE" {
		return fmt.Errorf("expected NAMESPACE as the first column with -A, got %q", lines[0])
	}
	if !strings.Contains(res.Stdout, "theta") {
		return fmt.Errorf("expected the seeded pod in the output, got:\n%s", tail(res.Stdout))
	}
	// kube-system always has pods; seeing only our namespace means the request
	// was still scoped.
	if !strings.Contains(res.Stdout, "kube-system") {
		return fmt.Errorf("expected pods from other namespaces too — -A must drop the namespace from the request, got:\n%s", tail(res.Stdout))
	}
	return nil
}

// stageGetByName — one object, and a real error when it is not there.
func stageGetByName(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "iota", "kappa")
	if err != nil {
		return err
	}
	defer cleanup()

	one, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pod", "iota")
	if err != nil {
		return err
	}
	if one.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", one.ExitCode, tail(one.Stderr))
	}
	if !strings.Contains(one.Stdout, "iota") {
		return fmt.Errorf("expected pod \"iota\" in the output, got:\n%s", tail(one.Stdout))
	}
	if strings.Contains(one.Stdout, "kappa") {
		return fmt.Errorf("asking for one pod listed the others too:\n%s", tail(one.Stdout))
	}

	missing, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pod", "nope")
	if err != nil {
		return err
	}
	if missing.ExitCode == 0 {
		return fmt.Errorf("a pod that does not exist should be an error, got exit 0 and:\n%s", tail(missing.Stdout))
	}
	return nil
}

// stageAPIResources — ask the server what it serves, rather than assuming.
func stageAPIResources(ctx context.Context, env *kube.Env, bin string) error {
	res, err := runner.Invoke(ctx, bin, StageTimeout, "api-resources")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	lines := nonEmptyLines(res.Stdout)
	if len(lines) < 2 {
		return fmt.Errorf("expected a header and rows, got:\n%s", tail(res.Stdout))
	}
	header := wsRun.Split(strings.TrimSpace(lines[0]), -1)
	if len(header) < 3 || header[0] != "NAME" {
		return fmt.Errorf("expected a header starting with NAME, got %q", lines[0])
	}
	// A resource from the core group, one from a named group, and a shortname:
	// enough to show discovery walked more than one groupVersion.
	for _, want := range []string{"pods", "configmaps", "deployments", "po"} {
		if !strings.Contains(res.Stdout, want) {
			return fmt.Errorf("expected %q in the output — discovery should cover every served group, got:\n%s", want, tail(res.Stdout))
		}
	}
	return nil
}

// stageRESTMapper — every spelling of a resource resolves to the same thing.
func stageRESTMapper(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "lambda")
	if err != nil {
		return err
	}
	defer cleanup()

	for _, spelling := range []string{"po", "pods", "pod", "Pod"} {
		res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", spelling)
		if err != nil {
			return err
		}
		if res.ExitCode != 0 {
			return fmt.Errorf("get %s: expected exit 0, got %d\nstderr:\n%s", spelling, res.ExitCode, tail(res.Stderr))
		}
		if !strings.Contains(res.Stdout, "lambda") {
			return fmt.Errorf("get %s: expected the pod, got:\n%s", spelling, tail(res.Stdout))
		}
	}
	return nil
}

// stageDynamicClient — a resource the program has no compiled-in type for.
func stageDynamicClient(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := env.SeedConfigMap(ctx, "settings"); err != nil {
		return err
	}
	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "configmaps")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if !strings.Contains(res.Stdout, "settings") {
		return fmt.Errorf("expected the ConfigMap \"settings\", got:\n%s", tail(res.Stdout))
	}

	// And the same command must still work for pods, through the same path.
	if err := env.SeedPods(ctx, "mu"); err != nil {
		return err
	}
	pods, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods")
	if err != nil {
		return err
	}
	if pods.ExitCode != 0 || !strings.Contains(pods.Stdout, "mu") {
		return fmt.Errorf("pods stopped working once any resource did:\n%s", tail(pods.Stdout+pods.Stderr))
	}
	return nil
}

// stageOutputWide — more columns, same objects.
func stageOutputWide(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "nu")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods", "-o", "wide")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	lines := nonEmptyLines(res.Stdout)
	if len(lines) < 2 {
		return fmt.Errorf("expected a header and a row, got:\n%s", tail(res.Stdout))
	}
	header := wsRun.Split(strings.TrimSpace(lines[0]), -1)
	if len(header) < 4 {
		return fmt.Errorf("expected -o wide to add columns beyond NAME and AGE, got %q", lines[0])
	}
	joined := strings.Join(header, " ")
	if !strings.Contains(joined, "NODE") {
		return fmt.Errorf("expected a NODE column with -o wide, got %q", lines[0])
	}
	return nil
}

// stageLabelSelector — -l filters on the server, not here.
func stageLabelSelector(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := env.SeedLabeledPod(ctx, "web-one", map[string]string{"app": "web"}); err != nil {
		return err
	}
	if err := env.SeedLabeledPod(ctx, "db-one", map[string]string{"app": "db"}); err != nil {
		return err
	}

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods", "-l", "app=web")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if !strings.Contains(res.Stdout, "web-one") {
		return fmt.Errorf("expected the matching pod, got:\n%s", tail(res.Stdout))
	}
	if strings.Contains(res.Stdout, "db-one") {
		return fmt.Errorf("the selector did not filter — db-one should not appear:\n%s", tail(res.Stdout))
	}
	return nil
}

// stageFieldSelector — a different axis, and one the server indexes.
func stageFieldSelector(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "xi", "omicron")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods", "--field-selector", "metadata.name=xi")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if !strings.Contains(res.Stdout, "xi") {
		return fmt.Errorf("expected the matching pod, got:\n%s", tail(res.Stdout))
	}
	if strings.Contains(res.Stdout, "omicron") {
		return fmt.Errorf("the field selector did not filter:\n%s", tail(res.Stdout))
	}
	return nil
}

// stageSortedOutput — the same command twice must print the same order.
func stageSortedOutput(ctx context.Context, env *kube.Env, bin string) error {
	// Seeded deliberately out of order: creation order is not name order.
	kc, cleanup, err := scoped(ctx, env, "zulu", "alpha", "mike")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "get", "pods")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	lines := nonEmptyLines(res.Stdout)
	var names []string
	for _, l := range lines[1:] {
		names = append(names, wsRun.Split(strings.TrimSpace(l), -1)[0])
	}
	want := []string{"alpha", "mike", "zulu"}
	if len(names) != len(want) {
		return fmt.Errorf("expected %d rows, got %d:\n%s", len(want), len(names), tail(res.Stdout))
	}
	for i := range want {
		if names[i] != want[i] {
			return fmt.Errorf("expected rows sorted by name %v, got %v", want, names)
		}
	}
	return nil
}

// stageWatch — the program prints what exists, then keeps printing as things
// change.
//
// It never exits on its own, so the harness bounds it and creates an object
// while it is running. The exit code is meaningless here: we are the ones who
// stopped it.
func stageWatch(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "rho")
	if err != nil {
		return err
	}
	defer cleanup()

	created := make(chan error, 1)
	go func() {
		select {
		case <-time.After(4 * time.Second):
			created <- env.SeedPods(ctx, "sigma")
		case <-ctx.Done():
			created <- ctx.Err()
		}
	}()

	res, err := runner.InvokeEnv(ctx, bin, kc, 20*time.Second, "get", "pods", "-w")
	if err != nil {
		return err
	}
	if err := <-created; err != nil {
		return fmt.Errorf("seeding the pod the watch should have seen: %w", err)
	}
	if !strings.Contains(res.Stdout, "rho") {
		return fmt.Errorf("expected the existing pod before the stream starts, got:\n%s", tail(res.Stdout))
	}
	if !strings.Contains(res.Stdout, "sigma") {
		return fmt.Errorf("a pod created while watching never appeared — the program listed and stopped:\n%s", tail(res.Stdout))
	}
	return nil
}

// writeFile drops a manifest in a temp dir and returns its path.
func writeFile(dir, name, body string) (string, error) {
	path := filepath.Join(dir, name)
	return path, os.WriteFile(path, []byte(body), 0o644)
}

// stageDelete — the object goes away, and asking for one that is not there is
// an error rather than a shrug.
func stageDelete(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "tau")
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "delete", "pod", "tau")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if err := kube.WaitFor(ctx, "the pod to go away", func(ctx context.Context) (bool, error) {
		return env.Gone(ctx, "tau")
	}); err != nil {
		return fmt.Errorf("the command exited 0 but the pod is still there: %w", err)
	}

	missing, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "delete", "pod", "never-existed")
	if err != nil {
		return err
	}
	if missing.ExitCode == 0 {
		return fmt.Errorf("deleting something that does not exist should fail, got exit 0")
	}
	return nil
}

// stageCreateFromFile — a manifest becomes an object.
func stageCreateFromFile(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	dir, err := os.MkdirTemp("", "byok8s-manifest-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	path, err := writeFile(dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: from-file
data:
  greeting: hello
`)
	if err != nil {
		return err
	}

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "create", "-f", path)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	cm, err := env.ConfigMap(ctx, "from-file")
	if err != nil {
		return fmt.Errorf("the command exited 0 but no ConfigMap was created: %w", err)
	}
	if cm.Data["greeting"] != "hello" {
		return fmt.Errorf("expected the data from the manifest, got %v", cm.Data)
	}

	// The manifest names no namespace, so the object must land in the one the
	// context selected rather than in default.
	if cm.Namespace != env.Namespace {
		return fmt.Errorf("expected the ConfigMap in %s, found it in %s", env.Namespace, cm.Namespace)
	}
	return nil
}

// stageApplySSA — apply is idempotent where create is not, and it records who
// owns each field.
func stageApplySSA(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	dir, err := os.MkdirTemp("", "byok8s-manifest-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	path, err := writeFile(dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: applied
data:
  greeting: hello
`)
	if err != nil {
		return err
	}

	for i := 1; i <= 2; i++ {
		res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "apply", "-f", path)
		if err != nil {
			return err
		}
		if res.ExitCode != 0 {
			return fmt.Errorf("apply #%d: expected exit 0, got %d — apply on an object that exists is not an error, which is the difference from create\nstderr:\n%s",
				i, res.ExitCode, tail(res.Stderr))
		}
	}

	cm, err := env.ConfigMap(ctx, "applied")
	if err != nil {
		return fmt.Errorf("no ConfigMap after two applies: %w", err)
	}
	if cm.Data["greeting"] != "hello" {
		return fmt.Errorf("expected the manifest's data, got %v", cm.Data)
	}
	var applied bool
	for _, mf := range cm.ManagedFields {
		if mf.Operation == "Apply" {
			applied = true
		}
	}
	if !applied {
		return fmt.Errorf("the object has no Apply entry in managedFields — that means it was created or updated, not applied; managers seen: %v", managers(cm.ManagedFields))
	}
	return nil
}

// stageApplyConflict — someone else owns the field, and taking it is a
// decision the user makes.
func stageApplyConflict(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	// Another manager gets there first and owns data.greeting.
	if err := env.ApplyAs(ctx, "other-tool", "contested", "greeting", "theirs"); err != nil {
		return err
	}

	dir, err := os.MkdirTemp("", "byok8s-manifest-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	path, err := writeFile(dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: contested
data:
  greeting: ours
`)
	if err != nil {
		return err
	}

	clash, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "apply", "-f", path)
	if err != nil {
		return err
	}
	if clash.ExitCode == 0 {
		return fmt.Errorf("applying over another manager's field should conflict, got exit 0 — the server reports this, so the program must not swallow it")
	}

	forced, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "apply", "-f", path, "--force-conflicts")
	if err != nil {
		return err
	}
	if forced.ExitCode != 0 {
		return fmt.Errorf("--force-conflicts should succeed, got %d\nstderr:\n%s", forced.ExitCode, tail(forced.Stderr))
	}
	cm, err := env.ConfigMap(ctx, "contested")
	if err != nil {
		return err
	}
	if cm.Data["greeting"] != "ours" {
		return fmt.Errorf("after forcing, the value should be ours, got %q", cm.Data["greeting"])
	}
	return nil
}

func managers(fields []metav1.ManagedFieldsEntry) []string {
	var out []string
	for _, f := range fields {
		out = append(out, fmt.Sprintf("%s/%s", f.Manager, f.Operation))
	}
	return out
}

// stagePatch — change one field without sending the whole object.
func stagePatch(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := env.SeedConfigMap(ctx, "settings"); err != nil {
		return err
	}
	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout,
		"patch", "configmap", "settings", "-p", `{"data":{"colour":"blue"}}`)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	cm, err := env.ConfigMap(ctx, "settings")
	if err != nil {
		return err
	}
	if cm.Data["colour"] != "blue" {
		return fmt.Errorf("expected the patched key, got %v", cm.Data)
	}
	// The seeded key must survive: a merge patch changes what it names and
	// leaves the rest, which is the whole difference from an update.
	if cm.Data["greeting"] != "hello" {
		return fmt.Errorf("the patch replaced the object instead of merging into it — greeting is gone: %v", cm.Data)
	}
	return nil
}

// stageScale — the /scale subresource, not the whole Deployment.
func stageScale(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := env.SeedDeployment(ctx, "web", 1); err != nil {
		return err
	}
	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "scale", "deployment", "web", "--replicas", "3")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	dep, err := env.Deployment(ctx, "web")
	if err != nil {
		return err
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 3 {
		return fmt.Errorf("expected spec.replicas 3, got %v", dep.Spec.Replicas)
	}
	return nil
}

// stageDescribe — the block layout, not a table.
func stageDescribe(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "upsilon")
	if err != nil {
		return err
	}
	defer cleanup()

	if err := env.WaitForScheduled(ctx, "upsilon"); err != nil {
		return err
	}

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "describe", "pod", "upsilon")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	// Label: value lines, not columns — describe answers "tell me about this
	// one object" where get answers "show me these objects".
	for _, want := range []string{"Name:", "Namespace:", "Node:", "Status:"} {
		if !strings.Contains(res.Stdout, want) {
			return fmt.Errorf("expected a %q line in the description, got:\n%s", want, tail(res.Stdout))
		}
	}
	if !strings.Contains(res.Stdout, "upsilon") {
		return fmt.Errorf("expected the pod's name in the output, got:\n%s", tail(res.Stdout))
	}
	if strings.Contains(res.Stdout, "NAME   AGE") {
		return fmt.Errorf("that is the table printer — describe is a different shape:\n%s", tail(res.Stdout))
	}
	return nil
}

// stageDescribeEvents — the events belonging to this object, and no others.
func stageDescribeEvents(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env, "phi", "quebec")
	if err != nil {
		return err
	}
	defer cleanup()

	if err := env.WaitForScheduled(ctx, "phi"); err != nil {
		return err
	}
	if err := env.WaitForScheduled(ctx, "quebec"); err != nil {
		return err
	}

	res, err := runner.InvokeEnv(ctx, bin, kc, StageTimeout, "describe", "pod", "phi")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if !strings.Contains(res.Stdout, "Events:") {
		return fmt.Errorf("expected an Events section, got:\n%s", tail(res.Stdout))
	}
	if !strings.Contains(res.Stdout, "Scheduled") {
		return fmt.Errorf("expected the Scheduled event the scheduler emits, got:\n%s", tail(res.Stdout))
	}
	// Events live in their own collection keyed by involvedObject; fetching
	// them all and printing them would show the other pod's too.
	//
	// Matched as a whole word: the scheduler's own messages contain words like
	// "machine", and a substring check on a short name passes or fails on the
	// wrong thing.
	if regexp.MustCompile(`\bquebec\b`).MatchString(res.Stdout) {
		return fmt.Errorf("the events are not filtered to this pod — quebec's events appeared:\n%s", tail(res.Stdout))
	}
	return nil
}

// stageLogs — a container's output, streamed.
//
// The first stage that needs a pod actually running: logs come from the
// kubelet, not from the API object.
func stageLogs(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	const marker = "byok8s-was-here"
	if err := env.SeedRunningPod(ctx, "talker", marker); err != nil {
		return err
	}

	res, err := runner.InvokeEnv(ctx, bin, kc, 60*time.Second, "logs", "talker")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if !strings.Contains(res.Stdout, marker) {
		return fmt.Errorf("expected the container's output %q, got:\n%s", marker, tail(res.Stdout))
	}
	return nil
}

// stageExec — run something inside the container and read what it said.
func stageExec(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := env.SeedRunningPod(ctx, "shell", "ready"); err != nil {
		return err
	}

	res, err := runner.InvokeEnv(ctx, bin, kc, 60*time.Second, "exec", "shell", "--", "echo", "from-inside")
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("expected exit 0, got %d\nstderr:\n%s", res.ExitCode, tail(res.Stderr))
	}
	if !strings.Contains(res.Stdout, "from-inside") {
		return fmt.Errorf("expected the command's output from inside the container, got:\n%s", tail(res.Stdout))
	}
	return nil
}

// stagePortForward — a local port that reaches into the cluster.
//
// The program never exits, so the harness bounds it and connects from outside
// while it runs. What is asserted is that something answered on the local
// port, which can only happen if the tunnel was actually built.
func stagePortForward(ctx context.Context, env *kube.Env, bin string) error {
	kc, cleanup, err := scoped(ctx, env)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := env.SeedServingPod(ctx, "server"); err != nil {
		return err
	}

	const local = "18080"
	fetched := make(chan string, 1)
	go func() {
		// Give the forwarder a moment to bind before knocking.
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			fetched <- ""
			return
		}
		client := &http.Client{Timeout: 5 * time.Second}
		for i := 0; i < 5; i++ {
			resp, err := client.Get("http://127.0.0.1:" + local + "/")
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				fetched <- string(body)
				return
			}
			time.Sleep(2 * time.Second)
		}
		fetched <- ""
	}()

	res, err := runner.InvokeEnv(ctx, bin, kc, 30*time.Second,
		"port-forward", "server", local+":80")
	if err != nil {
		return err
	}
	body := <-fetched
	if body == "" {
		return fmt.Errorf("nothing answered on localhost:%s — the tunnel was never established\nprogram output:\n%s", local, tail(res.Stdout+res.Stderr))
	}
	if !strings.Contains(body, "byok8s") {
		return fmt.Errorf("the local port answered, but not from the pod: %q", body)
	}
	return nil
}
