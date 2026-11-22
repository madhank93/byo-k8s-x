package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// Node is a kind node container, named as Docker names it. Node-plane courses
// need namespaces, cgroups and mounts, so their program is built for Linux and
// run inside the node rather than on the host.
//
// Every run goes into its own transient systemd scope. A process started by
// `docker exec` outlives the docker client that started it, so killing the
// client stops nothing; a scope is a cgroup, and signalling it reaches every
// process the program forked, however it detached.
type Node string

// NodeBin is where Install puts the learner's program inside a node.
const NodeBin = "/usr/local/bin/byok8s-app"

// scopePrefix names every scope this package creates, so Sweep can find runs
// a killed harness left behind.
const scopePrefix = "byok8s-app-"

var scopeSeq atomic.Int64

// NodeArch is the GOARCH kind nodes run as: the Docker server's architecture,
// which is the host's on a Linux runner and the VM's under Docker Desktop or
// OrbStack.
func NodeArch(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Arch}}").Output()
	if err != nil {
		return "", fmt.Errorf("ask docker for the node architecture: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// Install stops any run still left in the node and copies a binary built for
// it (BuildFor "linux", NodeArch) to NodeBin.
func (n Node) Install(ctx context.Context, bin string) error {
	if err := n.Sweep(ctx); err != nil {
		return err
	}
	if out, err := exec.CommandContext(ctx, "docker", "cp", bin, string(n)+":"+NodeBin).CombinedOutput(); err != nil {
		return fmt.Errorf("copy the program into node %s: %w\n%s", n, err, out)
	}
	return nil
}

// Sweep kills every scope a previous run left in the node.
func (n Node) Sweep(ctx context.Context) error {
	res, err := n.Exec(ctx, 30*time.Second, "systemctl", "kill", "--signal=SIGKILL", scopePrefix+"*.scope")
	if err != nil {
		return err
	}
	// systemctl kill with a pattern that matches nothing exits non-zero and
	// says nothing; anything it does say is a real failure.
	if res.ExitCode != 0 && strings.TrimSpace(res.Stderr) != "" {
		return fmt.Errorf("sweep node %s: %s", n, res.Stderr)
	}
	return nil
}

// Invoke runs the installed program in the node with args and extra
// environment entries, and captures its output, like InvokeEnv on the host.
func (n Node) Invoke(ctx context.Context, env []string, timeout time.Duration, args ...string) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd, signal := n.scoped(ctx, env, args)
	return capture(cmd, func() error { return signal(syscall.SIGKILL) })
}

// Start runs the installed program in the node and leaves it running, like
// Start on the host. Stop and Kill signal its whole scope.
func (n Node) Start(ctx context.Context, env []string, args ...string) (*Process, error) {
	ctx, cancel := context.WithCancel(ctx)
	cmd, signal := n.scoped(ctx, env, args)
	return start(cmd, cancel, signal)
}

func (n Node) scoped(ctx context.Context, env, args []string) (*exec.Cmd, func(syscall.Signal) error) {
	unit := fmt.Sprintf("%s%d-%d", scopePrefix, os.Getpid(), scopeSeq.Add(1))
	argv := []string{"exec", "-i"}
	for _, e := range env {
		argv = append(argv, "-e", e)
	}
	argv = append(argv, string(n), "systemd-run", "--quiet", "--collect", "--scope", "--unit="+unit, NodeBin)
	cmd := exec.CommandContext(ctx, "docker", append(argv, args...)...)
	signal := func(sig syscall.Signal) error {
		// The context that ran the program may be the one that just ended.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return exec.CommandContext(ctx, "docker", "exec", string(n),
			"systemctl", "kill", "--signal="+strconv.Itoa(int(sig)), unit+".scope").Run()
	}
	return cmd, signal
}

// Exec runs a command in the node as the harness, not the learner: crictl,
// nsenter, findmnt, cat. Its exit code is in the Result, not the error.
func (n Node) Exec(ctx context.Context, timeout time.Duration, argv ...string) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"exec", string(n)}, argv...)...)
	return capture(cmd, func() error { return cmd.Process.Kill() })
}

// ReadFile returns a file's contents from the node's own filesystem.
func (n Node) ReadFile(ctx context.Context, path string) (string, error) {
	res, err := n.Exec(ctx, 30*time.Second, "cat", path)
	if err != nil {
		return "", err
	}
	if res.ExitCode != 0 {
		return "", fmt.Errorf("read %s in node %s: %s", path, n, strings.TrimSpace(res.Stderr))
	}
	return res.Stdout, nil
}

// WriteFile writes data to path in the node, creating its directory.
func (n Node) WriteFile(ctx context.Context, path string, data []byte, mode os.FileMode) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	script := fmt.Sprintf(`mkdir -p "$(dirname %[1]q)" && cat > %[1]q && chmod %[2]o %[1]q`, path, mode.Perm())
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", string(n), "sh", "-c", script)
	cmd.Stdin = strings.NewReader(string(data))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("write %s in node %s: %w\n%s", path, n, err, out)
	}
	return nil
}

// Nsenter runs argv in the node inside the namespaces of process pid, as the
// node sees pids; flags pick which (-m, -p, -n, …).
func (n Node) Nsenter(ctx context.Context, pid int, flags []string, argv ...string) (*Result, error) {
	full := append([]string{"nsenter", "-t", strconv.Itoa(pid)}, flags...)
	return n.Exec(ctx, 30*time.Second, append(append(full, "--"), argv...)...)
}
