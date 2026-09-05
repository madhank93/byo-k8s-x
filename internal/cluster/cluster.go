// Package cluster manages the local kind cluster every course runs against.
// One cluster is shared by every course and every stage; isolation between
// stages is per-namespace, not per-cluster, because creating a cluster costs
// about thirty seconds and creating a namespace costs milliseconds.
package cluster

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

const (
	// Name is the kind cluster name, and Context the kubeconfig context kind
	// derives from it. Both are fixed: the guard in internal/kube refuses any
	// context that is not a kind one, and a fixed name makes that checkable.
	Name    = "byok8s"
	Context = "kind-byok8s"
)

// Exists reports whether the cluster is already created.
func Exists(ctx context.Context) (bool, error) {
	out, err := exec.CommandContext(ctx, "kind", "get", "clusters").Output()
	if err != nil {
		return false, fmt.Errorf("kind get clusters: %w", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == Name {
			return true, nil
		}
	}
	return false, nil
}

// Up creates the cluster if it is missing and waits for its node to be ready.
// It is idempotent, so `byok8s up` is safe to run at the start of any session.
func Up(ctx context.Context, progress func(string)) error {
	exists, err := Exists(ctx)
	if err != nil {
		return err
	}
	if exists {
		progress("cluster " + Name + " already exists")
		return nil
	}

	progress("creating cluster " + Name + " (this takes about half a minute)")
	create := exec.CommandContext(ctx, "kind", "create", "cluster", "--name", Name, "--wait", "60s")
	if out, err := create.CombinedOutput(); err != nil {
		return fmt.Errorf("kind create cluster: %w\n%s", err, out)
	}
	progress("cluster ready")
	return nil
}

// Down deletes the cluster. Courses keep no state in it, so this is always safe.
func Down(ctx context.Context, progress func(string)) error {
	exists, err := Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		progress("cluster " + Name + " does not exist")
		return nil
	}
	del := exec.CommandContext(ctx, "kind", "delete", "cluster", "--name", Name)
	if out, err := del.CombinedOutput(); err != nil {
		return fmt.Errorf("kind delete cluster: %w\n%s", err, out)
	}
	progress("cluster deleted")
	return nil
}
