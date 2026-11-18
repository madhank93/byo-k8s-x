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

// Up creates the cluster if it is missing and waits for its nodes to be ready.
// It is idempotent, so `byok8s up` is safe to run at the start of any session.
//
// workers is how many worker nodes the courses need beside the control plane.
// An existing cluster with fewer is reported rather than replaced: deleting it
// is the learner's call, not a side effect of up.
func Up(ctx context.Context, workers int, progress func(string)) error {
	exists, err := Exists(ctx)
	if err != nil {
		return err
	}
	if exists {
		have, err := Workers(ctx)
		if err != nil {
			return err
		}
		if have < workers {
			return fmt.Errorf("cluster %s has %d worker node(s) and the courses here need %d\n  fix: byok8s down && byok8s up", Name, have, workers)
		}
		progress("cluster " + Name + " already exists")
		return nil
	}

	progress(fmt.Sprintf("creating cluster %s with %d worker node(s) (this takes about half a minute)", Name, workers))
	create := exec.CommandContext(ctx, "kind", "create", "cluster", "--name", Name, "--wait", "60s", "--config", "-")
	create.Stdin = strings.NewReader(config(workers))
	if out, err := create.CombinedOutput(); err != nil {
		return fmt.Errorf("kind create cluster: %w\n%s", err, out)
	}
	progress("cluster ready")
	return nil
}

// config is the kind cluster config: one control plane and the given number of
// workers. With workers present, kind leaves the control plane tainted
// NoSchedule, so pods land only on the workers.
func config(workers int) string {
	var b strings.Builder
	b.WriteString("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n- role: control-plane\n")
	for range workers {
		b.WriteString("- role: worker\n")
	}
	return b.String()
}

// Workers counts the cluster's nodes that are not control planes.
func Workers(ctx context.Context) (int, error) {
	out, err := exec.CommandContext(ctx, "kubectl", "--context", Context, "get", "nodes",
		"--selector", "!node-role.kubernetes.io/control-plane", "--no-headers", "--output", "name").Output()
	if err != nil {
		return 0, fmt.Errorf("count worker nodes: %w", err)
	}
	return len(strings.Fields(string(out))), nil
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
