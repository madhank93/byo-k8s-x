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
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, args...)
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
