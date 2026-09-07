// Package runner builds and invokes the learner's program.
//
// Two pieces of hardening here are not premature. A Go program that talks to a
// cluster starts goroutines that hold watches open, and `go run` supervises a
// child rather than becoming it — so killing the process we spawned can leave
// the real program running against the cluster. We therefore build once to a
// real binary and invoke that, and we put every invocation in its own process
// group so a cancel reaches the whole tree.
package runner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// MaxOutput caps each captured stream. A learner's first watch loop often
// prints forever; without a cap the harness holds it all in memory.
const MaxOutput = 1 << 20 // 1 MiB

// Build compiles the learner's program to a temporary binary and returns its
// path. It is called once per run, not once per assertion: compiling is the
// slowest thing in the loop and the program does not change mid-run.
func Build(ctx context.Context, appDir string) (string, error) {
	bin, err := os.MkdirTemp("", "byok8s-app-")
	if err != nil {
		return "", fmt.Errorf("temp dir: %w", err)
	}
	out := filepath.Join(bin, "app")

	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Dir = appDir
	if combined, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build %s:\n%s", appDir, combined)
	}
	return out, nil
}

// Result is one invocation of the learner's program.
type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// Invoke runs the built binary with args and captures its output.
//
// The verdict a stage draws from this is the exit code and stdout — never
// stderr on its own. client-go logs warnings to stderr on entirely successful
// runs, so a harness that treats stderr as failure fails everything.
func Invoke(ctx context.Context, bin string, timeout time.Duration, args ...string) (*Result, error) {
	return InvokeEnv(ctx, bin, nil, timeout, args...)
}

// InvokeEnv is Invoke with extra environment entries, which is how a stage
// points the program at a scoped kubeconfig without the program knowing it is
// being tested.
func InvokeEnv(ctx context.Context, bin string, env []string, timeout time.Duration, args ...string) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, args...)
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	var stdout, stderr capped
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	// Its own process group, so cancelling reaches children the program spawned.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	// A grandchild can inherit our pipes and outlive the program; without a
	// bound, Wait blocks forever on EOF that never comes.
	cmd.WaitDelay = 3 * time.Second

	err := cmd.Run()
	res := &Result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		var exit *exec.ExitError
		if ok := asExitError(err, &exit); ok {
			res.ExitCode = exit.ExitCode()
			return res, nil
		}
		return res, fmt.Errorf("running the program: %w", err)
	}
	return res, nil
}

func asExitError(err error, target **exec.ExitError) bool {
	if e, ok := err.(*exec.ExitError); ok {
		*target = e
		return true
	}
	return false
}

// capped is an io.Writer that stops storing after MaxOutput but keeps
// reporting full writes, so the program never sees a short write or a closed
// pipe because the harness stopped listening.
type capped struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capped) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if room := MaxOutput - c.buf.Len(); room > 0 {
		if len(p) <= room {
			c.buf.Write(p)
		} else {
			c.buf.Write(p[:room])
		}
	}
	return len(p), nil
}

func (c *capped) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// Process is the learner's program left running while a stage changes the
// cluster underneath it. A controller, a webhook and a scheduler are all
// long-lived, so their stages start the program, act, wait for the cluster to
// converge, and only then stop it — which is a different shape from Invoke,
// where the program's exit is the whole answer.
type Process struct {
	cmd    *exec.Cmd
	stdout *capped
	stderr *capped
	done   chan error
	cancel context.CancelFunc

	mu     sync.Mutex
	result *Result
}

// Start launches the built binary and returns once it is running. The caller
// must Stop it; a stage that returns early without stopping leaves a program
// holding watches against the cluster.
func Start(ctx context.Context, bin string, env []string, args ...string) (*Process, error) {
	ctx, cancel := context.WithCancel(ctx)

	cmd := exec.CommandContext(ctx, bin, args...)
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	p := &Process{cmd: cmd, stdout: &capped{}, stderr: &capped{}, done: make(chan error, 1), cancel: cancel}
	cmd.Stdout, cmd.Stderr = p.stdout, p.stderr

	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 3 * time.Second

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("starting the program: %w", err)
	}
	go func() { p.done <- cmd.Wait() }()
	return p, nil
}

// Stdout and Stderr are what the program has printed so far. They are safe to
// read while it is still running.
func (p *Process) Stdout() string { return p.stdout.String() }

// Stderr is the program's error stream so far. A stage judges on stdout and
// the state of the cluster; stderr is for the failure message, because
// client-go logs warnings there on entirely successful runs.
func (p *Process) Stderr() string { return p.stderr.String() }

// Exited reports whether the program has already stopped on its own, which for
// a long-running program is a failure the stage should name rather than wait
// out.
func (p *Process) Exited() (bool, *Result) {
	select {
	case err := <-p.done:
		return true, p.finish(err)
	default:
		return false, nil
	}
}

// Stop asks the program to shut down the way Kubernetes would: SIGTERM to the
// whole process group, then SIGKILL if it outstays the grace period. The
// Result carries everything it printed, so a stage can assert on what it said
// on the way out.
func (p *Process) Stop(grace time.Duration) *Result {
	defer p.cancel()

	// It may have exited already — on its own, or through an earlier Exited
	// call that drained the channel.
	p.mu.Lock()
	cached := p.result
	p.mu.Unlock()
	if cached != nil {
		return cached
	}

	if p.cmd.Process != nil {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	}
	select {
	case err := <-p.done:
		return p.finish(err)
	case <-time.After(grace):
	}
	if p.cmd.Process != nil {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}
	select {
	case err := <-p.done:
		return p.finish(err)
	case <-time.After(3 * time.Second):
		return &Result{Stdout: p.Stdout(), Stderr: p.Stderr(), ExitCode: -1}
	}
}

// finish records the exit once, so Exited followed by Stop returns the same
// answer rather than blocking on a channel that has already been drained.
func (p *Process) finish(err error) *Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.result != nil {
		return p.result
	}
	res := &Result{Stdout: p.Stdout(), Stderr: p.Stderr()}
	var exit *exec.ExitError
	if err != nil && asExitError(err, &exit) {
		res.ExitCode = exit.ExitCode()
	} else if err != nil {
		res.ExitCode = -1
	}
	p.result = res
	return res
}
