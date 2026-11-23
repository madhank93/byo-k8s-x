// Command nodeprobe reports which Linux primitives a kind node offers the
// node-plane courses. It cross-compiles ./probe for the node, runs it there
// through runner.Node, and checks that a supervised run is really stopped.
//
//	go run ./hack/nodeprobe [-node byok8s-worker]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/runner"
)

func main() {
	node := flag.String("node", "byok8s-worker", "the kind node container to probe")
	flag.Parse()
	if err := run(runner.Node(*node)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(n runner.Node) error {
	ctx := context.Background()
	arch, err := runner.NodeArch(ctx)
	if err != nil {
		return err
	}
	bin, err := runner.BuildFor(ctx, "hack/nodeprobe/probe", "linux", arch)
	if err != nil {
		return err
	}
	defer os.RemoveAll(filepath.Dir(bin))
	if err := n.Install(ctx, bin); err != nil {
		return err
	}
	fmt.Printf("node %s, linux/%s\n", n, arch)

	if err := supervised(ctx, n); err != nil {
		fmt.Printf("%-20s FAIL %v\n", "supervised-run", err)
	} else {
		fmt.Printf("%-20s ok   Stop reached the program through its scope\n", "supervised-run")
	}

	res, err := n.Invoke(ctx, nil, 5*time.Minute)
	if err != nil {
		return err
	}
	fmt.Print(res.Stdout, res.Stderr)
	if res.ExitCode != 0 {
		return fmt.Errorf("probe exited %d", res.ExitCode)
	}
	return nil
}

// supervised starts the probe sleeping, stops it, and requires its pid gone.
func supervised(ctx context.Context, n runner.Node) error {
	p, err := n.Start(ctx, nil, "sleep")
	if err != nil {
		return err
	}
	var pid string
	for i := 0; i < 50 && pid == ""; i++ {
		time.Sleep(100 * time.Millisecond)
		if f := strings.Fields(p.Stdout()); len(f) == 2 {
			pid = f[1]
		}
	}
	res := p.Stop(5 * time.Second)
	if pid == "" {
		return fmt.Errorf("never started: %q %q", res.Stdout, res.Stderr)
	}
	if res.ExitCode == 0 {
		return fmt.Errorf("exited 0 after SIGTERM")
	}
	check, err := n.Exec(ctx, 10*time.Second, "test", "-d", "/proc/"+pid)
	if err != nil {
		return err
	}
	if check.ExitCode == 0 {
		return fmt.Errorf("pid %s is still running after Stop", pid)
	}
	return nil
}
