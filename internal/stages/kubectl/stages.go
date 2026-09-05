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
	"fmt"
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
