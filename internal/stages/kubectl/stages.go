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
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
)

// StageTimeout bounds one stage. Generous, because a cold cluster answering
// its first request is slower than any later one.
const StageTimeout = 90 * time.Second

// Stage is one gradable step.
type Stage struct {
	Slug string
	Run  func(ctx context.Context, env *kube.Env, bin string) error
}

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
