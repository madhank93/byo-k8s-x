// Package hpa holds the assertions for the "Build your own metrics server and
// HPA" course — one function per stage, registered by slug.
//
// The first seven stages grade a metrics pipeline: the program reads the
// kubelets, and from stage 4 serves metrics.k8s.io through the aggregator,
// where the real kubectl top can reach it. The rest grade an autoscaler that
// reads metrics the grader serves, so every number it must reach is known.
package hpa

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

var registry = map[string]Stage{}

// register bounds every stage: the autoscaling stages wait out windows and
// periods on purpose, so the bound is wider than a single wait.
func register(slug string, run func(ctx context.Context, env *kube.Env, bin string) error) {
	registry[slug] = Stage{Slug: slug, Run: func(ctx context.Context, env *kube.Env, bin string) error {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		defer cancel()
		defer clearPods(context.WithoutCancel(ctx), env)
		return run(ctx, env, bin)
	}}
}

// Lookup resolves a stage slug within this course.
func Lookup(slug string) (Stage, bool) {
	s, ok := registry[slug]
	return s, ok
}

func init() {
	register("summary-api", stageSummaryAPI)
	register("resource-metrics", stageResourceMetrics)
	register("pod-aggregation", stagePodAggregation)
	register("apiservice", stageAPIService)
	register("pod-metrics", stagePodMetrics)
	register("node-metrics", stageNodeMetrics)
	register("kubectl-top", stageKubectlTop)
	register("hpa-watch", stageHPAWatch)
	register("scaling-formula", stageScalingFormula)
	register("tolerance", stageTolerance)
	register("readiness-window", stageReadinessWindow)
	register("missing-metrics", stageMissingMetrics)
	register("scale-subresource", stageScaleSubresource)
	register("stabilization", stageStabilization)
	register("behavior-policies", stageBehaviorPolicies)
	register("custom-metrics", stageCustomMetrics)
	register("external-metrics", stageExternalMetrics)
	register("multi-metric", stageMultiMetric)
}

const mi = 1 << 20

// ctr is one container of a fixture pod: it holds fillMiB of memory in
// /dev/shm, which is charged to its own cgroup, and spins under a CPU limit of
// burnMilli when that is set, so both numbers the metrics report are known.
type ctr struct {
	name      string
	fillMiB   int
	burnMilli int
}

func fixturePod(env *kube.Env, name, node string, labels map[string]string, ctrs ...ctr) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace, Labels: labels},
		Spec: corev1.PodSpec{
			NodeName:                      node,
			TerminationGracePeriodSeconds: ptr(int64(0)),
		},
	}
	for _, c := range ctrs {
		script := "true"
		if c.fillMiB > 0 {
			script = fmt.Sprintf("dd if=/dev/zero of=/dev/shm/%s bs=1048576 count=%d 2>/dev/null", c.name, c.fillMiB)
		}
		if c.burnMilli > 0 {
			script += "; while :; do :; done"
		} else {
			script += "; exec sleep 86400"
		}
		k := corev1.Container{Name: c.name, Image: "busybox:1.36", Command: []string{"sh", "-c", script}}
		if c.burnMilli > 0 {
			q := resource.MustParse(fmt.Sprintf("%dm", c.burnMilli))
			k.Resources = corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: q}}
		}
		pod.Spec.Containers = append(pod.Spec.Containers, k)
	}
	return pod
}

// seedRunning creates pods and waits until every one is Running.
func seedRunning(ctx context.Context, env *kube.Env, pods ...*corev1.Pod) error {
	for _, p := range pods {
		if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, p, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("seed pod %s: %w", p.Name, err)
		}
	}
	for _, p := range pods {
		name := p.Name
		if err := kube.WaitFor(ctx, "pod "+name+" to be Running", func(ctx context.Context) (bool, error) {
			got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
			return err == nil && got.Status.Phase == corev1.PodRunning, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// clearPods deletes every pod in the stage's namespace at once. A spinning
// fixture would otherwise keep burning CPU until the stage next runs.
func clearPods(ctx context.Context, env *kube.Env) {
	if env == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	env.Client.CoreV1().Pods(env.Namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0))}, metav1.ListOptions{})
}

// kindNodes lists the nodes a kind container backs. Other courses register
// nodes no kubelet answers for, and a pod pinned to one never runs.
func kindNodes(ctx context.Context, env *kube.Env, selector string) ([]string, error) {
	list, err := env.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, n := range list.Items {
		if strings.HasPrefix(n.Spec.ProviderID, "kind://") {
			names = append(names, n.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// workers are the nodes fixture pods are pinned to, in a stable order.
func workers(ctx context.Context, env *kube.Env) ([]string, error) {
	names, err := kindNodes(ctx, env, "!node-role.kubernetes.io/control-plane")
	if err != nil {
		return nil, err
	}
	if len(names) < 2 {
		return nil, fmt.Errorf("this course needs two worker nodes and the cluster has %d (byok8s down && byok8s up)", len(names))
	}
	return names, nil
}

// summary is the part of the kubelet's Summary API the grader reads itself.
type summary struct {
	Node struct {
		Memory struct {
			WorkingSetBytes uint64 `json:"workingSetBytes"`
		} `json:"memory"`
	} `json:"node"`
	Pods []struct {
		PodRef struct{ Name, Namespace string } `json:"podRef"`
		Memory *struct {
			WorkingSetBytes uint64 `json:"workingSetBytes"`
		} `json:"memory"`
	} `json:"pods"`
}

func readSummary(ctx context.Context, env *kube.Env, node string) (*summary, error) {
	raw, err := env.Client.CoreV1().RESTClient().Get().AbsPath("/api/v1/nodes", node, "proxy/stats/summary").DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	var s summary
	return &s, json.Unmarshal(raw, &s)
}

// awaitKubeletMemory waits until the kubelet on node reports pod holding at
// least minBytes. The kubelet samples on its own schedule, so a fresh pod is
// invisible for a few seconds, and grading before then grades the kubelet.
func awaitKubeletMemory(ctx context.Context, env *kube.Env, node, pod string, minBytes uint64) error {
	return kube.WaitFor(ctx, "the kubelet on "+node+" to report pod "+pod, func(ctx context.Context) (bool, error) {
		s, err := readSummary(ctx, env, node)
		if err != nil {
			return false, nil
		}
		for _, p := range s.Pods {
			if p.PodRef.Namespace == env.Namespace && p.PodRef.Name == pod && p.Memory != nil {
				return p.Memory.WorkingSetBytes >= minBytes, nil
			}
		}
		return false, nil
	})
}

// usage is one line the program printed: a key, millicores, and bytes.
type usage struct{ cpu, mem int64 }

// parseUsage reads lines of the form "<key> <cpu>m <bytes>".
func parseUsage(out string) (map[string]usage, error) {
	got := map[string]usage{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 3 || !strings.HasSuffix(f[1], "m") {
			return nil, fmt.Errorf("cannot read the line %q — each line is \"<name> <cpu>m <memory-bytes>\"", line)
		}
		cpu, err1 := strconv.ParseInt(strings.TrimSuffix(f[1], "m"), 10, 64)
		mem, err2 := strconv.ParseInt(f[2], 10, 64)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("cannot read the numbers on %q — cpu is whole millicores, memory whole bytes", line)
		}
		got[f[0]] = usage{cpu, mem}
	}
	return got, nil
}

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

// invoke runs the program once against this stage's namespace.
func invoke(ctx context.Context, env *kube.Env, bin string, timeout time.Duration, args ...string) (*runner.Result, error) {
	kc, cleanup, err := scoped(env)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	res, err := runner.InvokeEnv(ctx, bin, kc, timeout, args...)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("`app %s` exited %d\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return res, nil
}

// stageSummaryAPI — `app summary <node>` prints every pod on that node, with
// what the kubelet's Summary API says it is using.
func stageSummaryAPI(ctx context.Context, env *kube.Env, bin string) error {
	nodes, err := workers(ctx, env)
	if err != nil {
		return err
	}
	if err := seedRunning(ctx, env,
		fixturePod(env, "fill", nodes[0], nil, ctr{name: "app", fillMiB: 40}),
		fixturePod(env, "elsewhere", nodes[1], nil, ctr{name: "app"})); err != nil {
		return err
	}
	if err := awaitKubeletMemory(ctx, env, nodes[0], "fill", 40*mi); err != nil {
		return err
	}

	res, err := invoke(ctx, env, bin, 30*time.Second, "summary", nodes[0])
	if err != nil {
		return err
	}
	got, err := parseUsage(res.Stdout)
	if err != nil {
		return err
	}
	fill, ok := got[env.Namespace+"/fill"]
	if !ok {
		return fmt.Errorf("pod %s/fill runs on %s and is not in the output\nthe program said:\n%s", env.Namespace, nodes[0], tail(res.Stdout))
	}
	if fill.mem < 40*mi || fill.mem > 400*mi {
		return fmt.Errorf("pod fill holds 40Mi and the program says it uses %d bytes — memory is the pod's workingSetBytes", fill.mem)
	}
	if _, ok := got[env.Namespace+"/elsewhere"]; ok {
		return fmt.Errorf("pod elsewhere runs on %s, not %s, and the program listed it — the summary is one kubelet's view of its own node", nodes[1], nodes[0])
	}
	system := false
	for k := range got {
		system = system || strings.HasPrefix(k, "kube-system/")
	}
	if !system {
		return fmt.Errorf("no kube-system pod in the output, though every node runs some — print every pod the kubelet reports, not only this namespace's")
	}
	return nil
}

// stageResourceMetrics — `app scrape <node>` reads the kubelet's Prometheus
// endpoint and prints a CPU rate per container, worked out from two samples.
func stageResourceMetrics(ctx context.Context, env *kube.Env, bin string) error {
	nodes, err := workers(ctx, env)
	if err != nil {
		return err
	}
	if err := seedRunning(ctx, env,
		fixturePod(env, "burner", nodes[0], nil, ctr{name: "app", burnMilli: 200}),
		fixturePod(env, "fill", nodes[0], nil, ctr{name: "app", fillMiB: 40})); err != nil {
		return err
	}
	if err := awaitKubeletMemory(ctx, env, nodes[0], "fill", 40*mi); err != nil {
		return err
	}

	res, err := invoke(ctx, env, bin, 75*time.Second, "scrape", nodes[0])
	if err != nil {
		return err
	}
	got, err := parseUsage(res.Stdout)
	if err != nil {
		return err
	}
	burner, ok := got[env.Namespace+"/burner/app"]
	if !ok {
		return fmt.Errorf("container %s/burner/app is not in the output\nthe program said:\n%s", env.Namespace, tail(res.Stdout))
	}
	if burner.cpu < 100 || burner.cpu > 300 {
		return fmt.Errorf("container burner/app spins under a 200m limit and the program says %dm — container_cpu_usage_seconds_total is a counter: the rate is its change divided by the change in its sample timestamp", burner.cpu)
	}
	fill, ok := got[env.Namespace+"/fill/app"]
	if !ok {
		return fmt.Errorf("container %s/fill/app is not in the output\nthe program said:\n%s", env.Namespace, tail(res.Stdout))
	}
	if fill.mem < 40*mi || fill.mem > 400*mi {
		return fmt.Errorf("container fill/app holds 40Mi and the program says %d bytes", fill.mem)
	}
	if fill.cpu > 50 {
		return fmt.Errorf("container fill/app is asleep and the program says it uses %dm", fill.cpu)
	}
	return nil
}

// stagePodAggregation — `app pods` prints one line per pod in the namespace,
// its containers summed, from whichever node each pod runs on.
func stagePodAggregation(ctx context.Context, env *kube.Env, bin string) error {
	nodes, err := workers(ctx, env)
	if err != nil {
		return err
	}
	if err := seedRunning(ctx, env,
		fixturePod(env, "pair", nodes[0], nil, ctr{name: "a", fillMiB: 25, burnMilli: 100}, ctr{name: "b", fillMiB: 25, burnMilli: 100}),
		fixturePod(env, "solo", nodes[1], nil, ctr{name: "app", fillMiB: 40})); err != nil {
		return err
	}
	if err := awaitKubeletMemory(ctx, env, nodes[0], "pair", 50*mi); err != nil {
		return err
	}
	if err := awaitKubeletMemory(ctx, env, nodes[1], "solo", 40*mi); err != nil {
		return err
	}

	res, err := invoke(ctx, env, bin, 75*time.Second, "pods")
	if err != nil {
		return err
	}
	got, err := parseUsage(res.Stdout)
	if err != nil {
		return err
	}
	for k := range got {
		if k != "pair" && k != "solo" {
			return fmt.Errorf("the output lists %q, and the namespace holds only pair and solo — print pod names from the kubeconfig's namespace", k)
		}
	}
	pair, ok := got["pair"]
	if !ok {
		return fmt.Errorf("pod pair is not in the output\nthe program said:\n%s", tail(res.Stdout))
	}
	if pair.mem < 50*mi {
		return fmt.Errorf("pod pair has two containers holding 25Mi each and the program says %d bytes — a pod's usage is the sum of its containers'", pair.mem)
	}
	if pair.cpu < 120 || pair.cpu > 300 {
		return fmt.Errorf("pod pair has two containers spinning under 100m limits and the program says %dm", pair.cpu)
	}
	solo, ok := got["solo"]
	if !ok {
		return fmt.Errorf("pod solo runs on %s and is not in the output — scrape every node, not one", nodes[1])
	}
	if solo.mem < 40*mi {
		return fmt.Errorf("pod solo holds 40Mi and the program says %d bytes", solo.mem)
	}
	return nil
}

// kubectlTop runs the real kubectl against this stage's namespace.
func kubectlTop(ctx context.Context, kc []string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", append([]string{"top"}, args...)...)
	cmd.Env = append(os.Environ(), kc...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("kubectl top %s: %v\n%s", strings.Join(args, " "), err, tail(string(out)))
	}
	return string(out), nil
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "  (nothing)"
	}
	lines := strings.Split(s, "\n")
	if len(lines) > 12 {
		lines = append([]string{"…"}, lines[len(lines)-12:]...)
	}
	return "  " + strings.Join(lines, "\n  ")
}

func ptr[T any](v T) *T { return &v }

func notFound(err error) bool { return apierrors.IsNotFound(err) }
