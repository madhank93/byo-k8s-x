// Package scheduler holds the assertions for the "Build your own scheduler"
// course — one function per stage, registered by slug.
//
// The tester creates pods that name this scheduler, which the default
// scheduler ignores, so the learner's program is the only thing that can place
// them. The verdict is what the program reports and which node each pod lands
// on; nothing inspects the learner's source.
package scheduler

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

// The name pods use to ask for the learner's scheduler. The course specifies
// it; the default scheduler leaves any pod naming it alone.
const schedulerName = "byok8s"

var registry = map[string]Stage{}

func register(s Stage) { registry[s.Slug] = s }

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := registry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "watch-unscheduled", Run: stageWatchUnscheduled})
}

// stageWatchUnscheduled checks the program finds the pods it is responsible
// for: those naming this scheduler and not yet on a node.
//
// A list alone misses what is created afterwards and a watch alone misses what
// already existed, so one pod is created before the program starts and one
// while it runs. A pod naming another scheduler is not this program's.
func stageWatchUnscheduled(ctx context.Context, env *kube.Env, bin string) error {
	if err := seedPod(ctx, env, "before", schedulerName); err != nil {
		return err
	}
	if err := seedPod(ctx, env, "elsewhere", "default-scheduler"); err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := awaitLine(ctx, p, "unscheduled "+env.Namespace+"/before", 60*time.Second); err != nil {
		return fmt.Errorf("pod before names %s and existed when the program started, and it was never reported: a watch alone sees only what changes after it starts: %w", schedulerName, err)
	}

	if err := seedPod(ctx, env, "after", schedulerName); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, "unscheduled "+env.Namespace+"/after", 30*time.Second); err != nil {
		return fmt.Errorf("pod after names %s and was created while the program ran, and it was never reported: a list alone sees only what already existed: %w", schedulerName, err)
	}

	// The pod naming the default scheduler existed before the program started,
	// so if it was going to be reported, it would be before "after" was.
	if strings.Contains(p.Stdout(), env.Namespace+"/elsewhere") {
		return fmt.Errorf("pod elsewhere names the default scheduler and was reported anyway: only pods naming %s are this program's\nthe program said:\n%s", schedulerName, tail(p.Stdout()))
	}
	return nil
}

// seedPod creates a pod that names a scheduler. It asks for nothing and
// tolerates nothing, so any worker fits it.
func seedPod(ctx context.Context, env *kube.Env, name, scheduler string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: scheduler,
			Containers:    []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// launch starts the learner's scheduler with a kubeconfig whose context
// selects this stage's namespace, and returns a cleanup that stops it.
func launch(ctx context.Context, env *kube.Env, bin string, args ...string) (*runner.Process, func(), error) {
	kc, cleanupEnv, err := scoped(env)
	if err != nil {
		return nil, nil, err
	}
	p, err := runner.Start(ctx, bin, kc, args...)
	if err != nil {
		cleanupEnv()
		return nil, nil, err
	}
	return p, func() {
		p.Stop(5 * time.Second)
		cleanupEnv()
	}, nil
}

// scoped writes a kubeconfig for this stage and returns it as an environment
// entry, so the program finds the cluster the way any client would.
func scoped(env *kube.Env) ([]string, func(), error) {
	dir, err := os.MkdirTemp("", "byok8s-stage-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	path, err := env.KubeconfigScoped(dir)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return []string{"KUBECONFIG=" + path}, cleanup, nil
}

// awaitLine waits for the program to print a line containing want.
func awaitLine(ctx context.Context, p *runner.Process, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		out := p.Stdout()
		if strings.Contains(out, want) {
			return nil
		}
		if done, res := p.Exited(); done {
			return fmt.Errorf("the program exited (%d) without printing %q\nstdout:\n%s\nstderr:\n%s",
				res.ExitCode, want, tail(res.Stdout), tail(res.Stderr))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gave up waiting for %q after %s\nstdout:\n%s\nstderr:\n%s",
				want, timeout, tail(out), tail(p.Stderr()))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// waitFor polls cond until it holds or the timeout passes.
func waitFor(ctx context.Context, what string, timeout time.Duration, cond wait.ConditionWithContextFunc) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return kube.WaitFor(ctx, what, cond)
}

// tail keeps an error message readable when the program has been talkative.
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
