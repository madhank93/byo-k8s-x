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
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	register(Stage{Slug: "bind", Run: stageBind})
	register(Stage{Slug: "node-list", Run: stageNodeList})
	register(Stage{Slug: "fit-resources", Run: stageFitResources})
	register(Stage{Slug: "fit-ports", Run: stageFitPorts})
	register(Stage{Slug: "node-selector", Run: stageNodeSelector})
	register(Stage{Slug: "node-affinity", Run: stageNodeAffinity})
	register(Stage{Slug: "taints", Run: stageTaints})
	register(Stage{Slug: "unschedulable", Run: stageUnschedulable})
	register(Stage{Slug: "events", Run: stageEvents})
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

// stageEvents checks the program says what it did, where a person will look.
//
// kubectl describe pod is the first thing anyone runs when a pod is not where
// they expected, so both answers belong there: a Normal event naming the node
// when the pod is placed, and a Warning when nothing fits.
func stageEvents(ctx context.Context, env *kube.Env, bin string) error {
	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := seedPod(ctx, env, "placed", schedulerName); err != nil {
		return err
	}
	node, err := boundNode(ctx, env, "placed", 60*time.Second)
	if err != nil {
		return fmt.Errorf("pod placed was never bound: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	var scheduled eventsv1.Event
	if err := waitFor(ctx, "an event about pod placed", 30*time.Second, func(ctx context.Context) (bool, error) {
		events, err := podEvents(ctx, env, "placed")
		if err != nil {
			return false, err
		}
		for _, e := range events {
			if e.Type == corev1.EventTypeNormal {
				scheduled = e
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("pod placed was bound to %s and no Normal event says so: kubectl describe pod is where that answer belongs: %w\nthe program said:\n%s", node, err, tail(p.Stdout()))
	}
	if scheduled.Reason == "" {
		return fmt.Errorf("the event about pod placed carries no reason: reason is the word people grep for, like Scheduled")
	}
	if !strings.Contains(scheduled.Note, node) {
		return fmt.Errorf("the event about pod placed says %q, which does not name %s, the node it went to", scheduled.Note, node)
	}
	if scheduled.ReportingController == "" {
		return fmt.Errorf("the event about pod placed names no reportingController, so kubectl describe cannot say who decided")
	}

	// A pod nothing can place: the reason is owed just as much.
	if err := seedSelectingPod(ctx, env, "nowhere", map[string]string{"byok8s.dev/stage-nowhere": "yes"}); err != nil {
		return err
	}
	if err := waitFor(ctx, "a warning event about pod nowhere", 60*time.Second, func(ctx context.Context) (bool, error) {
		events, err := podEvents(ctx, env, "nowhere")
		if err != nil {
			return false, err
		}
		for _, e := range events {
			if e.Type == corev1.EventTypeWarning && e.Reason != "" {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("pod nowhere selects a label no node carries and no Warning event explains why it is still waiting: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	return nil
}

// stageBind checks the program records a decision the way a scheduler must:
// with a Binding, after which the API server sets the pod's node.
//
// The node is given on the command line, so this stage is about binding alone;
// choosing a node comes later. A pod bound to a real node is then run by that
// node's kubelet, which is the proof the binding named one.
func stageBind(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	target := workers[len(workers)-1]

	if err := seedPod(ctx, env, "before", schedulerName); err != nil {
		return err
	}
	p, cleanup, err := launch(ctx, env, bin, "--node", target)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := seedPod(ctx, env, "after", schedulerName); err != nil {
		return err
	}

	for _, name := range []string{"before", "after"} {
		var pod *corev1.Pod
		if err := waitFor(ctx, "pod "+name+" to be bound", 60*time.Second, func(ctx context.Context) (bool, error) {
			got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			pod = got
			return got.Spec.NodeName != "", nil
		}); err != nil {
			return fmt.Errorf("pod %s names %s and was never bound to a node: %w\nthe program said:\n%s", name, schedulerName, err, tail(p.Stdout()))
		}
		if pod.Spec.NodeName != target {
			return fmt.Errorf("pod %s was bound to %s, but --node named %s", name, pod.Spec.NodeName, target)
		}
	}

	for _, name := range []string{"before", "after"} {
		if err := waitFor(ctx, "pod "+name+" to run", 60*time.Second, func(ctx context.Context) (bool, error) {
			got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return got.Status.Phase == corev1.PodRunning, nil
		}); err != nil {
			return fmt.Errorf("pod %s was bound to %s but never started running: %w", name, target, err)
		}
	}
	return nil
}

// workerNodes lists the nodes a pod may be placed on, sorted by name: every
// node that is not a control plane.
func workerNodes(ctx context.Context, env *kube.Env) ([]string, error) {
	list, err := env.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "!node-role.kubernetes.io/control-plane"})
	if err != nil {
		return nil, fmt.Errorf("list worker nodes: %w", err)
	}
	var names []string
	for _, n := range list.Items {
		names = append(names, n.Name)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("the cluster has no worker nodes\n  fix: byok8s down && byok8s up")
	}
	slices.Sort(names)
	return names, nil
}

// stageNodeList checks the program chooses its own node, and only one that can
// run the pod.
//
// A fake node is added that the API server lists but no kubelet backs, marked
// not Ready. Its name sorts first, so a scheduler that takes the first node, or
// takes every node in turn, lands pods there; ten pods leave a random choice
// little room to miss it.
func stageNodeList(ctx context.Context, env *kube.Env, bin string) error {
	const ghost = "byok8s-a-ghost"
	// A run that died mid-stage can leave the node behind: start from none, and
	// remove it however this run ends, including a failed add.
	dropFakeNode(ctx, env, ghost)
	defer dropFakeNode(context.WithoutCancel(ctx), env, ghost)
	if err := addFakeNode(ctx, env, ghost, false); err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	var names []string
	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("pod-%02d", i)
		names = append(names, name)
		if err := seedPod(ctx, env, name, schedulerName); err != nil {
			return err
		}
	}

	for _, name := range names {
		var node string
		if err := waitFor(ctx, "pod "+name+" to be bound", 60*time.Second, func(ctx context.Context) (bool, error) {
			got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			node = got.Spec.NodeName
			return node != "", nil
		}); err != nil {
			return fmt.Errorf("pod %s was never bound: with no --node the program has to choose a node itself: %w\nthe program said:\n%s", name, err, tail(p.Stdout()))
		}
		if node == ghost {
			return fmt.Errorf("pod %s was bound to %s, whose Ready condition is False: the node is listed, but no kubelet will ever run the pod", name, ghost)
		}
	}
	return nil
}

// addFakeNode creates a node the API server lists but no kubelet backs, with
// its Ready condition set as given. Nothing ever runs a pod bound to it.
func addFakeNode(ctx context.Context, env *kube.Env, name string, ready bool) error {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"byok8s.dev/fake": "true"}}}
	if _, err := env.Client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create fake node %s (left by an earlier run? kubectl delete node %s): %w", name, name, err)
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	// A patch, not an update: the node controller edits a new node straight
	// away, so an update built from the created object races it and conflicts.
	now := time.Now().UTC().Format(time.RFC3339)
	patch := fmt.Sprintf(`{"status":{"conditions":[{"type":"Ready","status":%q,"reason":"FakeNode","lastHeartbeatTime":%q,"lastTransitionTime":%q}]}}`, status, now, now)
	if _, err := env.Client.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}, "status"); err != nil {
		return fmt.Errorf("set the status of fake node %s: %w", name, err)
	}
	return nil
}

// dropFakeNode removes a fake node and every pod bound to it, in any
// namespace. Such a pod never finishes a graceful delete — no kubelet confirms
// it — so it is forced, or its namespace would hang in Terminating.
func dropFakeNode(ctx context.Context, env *kube.Env, name string) {
	pods, err := env.Client.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + name})
	if err == nil {
		zero := int64(0)
		for _, p := range pods.Items {
			_ = env.Client.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}
	_ = env.Client.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
}

// stageFitResources checks the program places a pod only where its requests
// fit beside everything already on the node.
//
// Each pod asks for more than half of any node, so a node takes one and no
// more. Pods are created one at a time, each placed before the next exists,
// which keeps the program's view of the cluster current; racing it is a later
// stage. A pod bound where it does not fit is failed by the kubelet, which is
// how an overcommit shows.
func stageFitResources(ctx context.Context, env *kube.Env, bin string) error {
	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	for _, ask := range []struct {
		resource corev1.ResourceName
		amount   string
	}{{corev1.ResourceCPU, "6"}, {corev1.ResourceMemory, "5Gi"}} {
		done, err := fillNodes(ctx, env, p, string(ask.resource), ask.amount+" of "+string(ask.resource), func(name string) error {
			return seedRequestingPod(ctx, env, name, ask.resource, ask.amount)
		})
		done()
		if err != nil {
			return err
		}
	}
	return nil
}

// fillNodes creates pods with seed, one at a time, until one is left waiting,
// and returns a cleanup that deletes them. Each pod must be one no node can
// take twice; prefix names the pods and what describes what each asks for.
func fillNodes(ctx context.Context, env *kube.Env, p *runner.Process, prefix, what string, seed func(name string) error) (func(), error) {
	var created []string
	cleanup := func() {
		zero := int64(0)
		for _, name := range created {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}

	holder := map[string]string{} // node -> the pod this batch put there
	for i := 1; i <= 6; i++ {
		name := fmt.Sprintf("%s-%d", prefix, i)
		if err := seed(name); err != nil {
			return cleanup, err
		}
		created = append(created, name)

		var node string
		if err := waitFor(ctx, "pod "+name+" to be bound", 15*time.Second, func(ctx context.Context) (bool, error) {
			got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			node = got.Spec.NodeName
			return node != "", nil
		}); err != nil {
			// Left waiting is right once every worker holds one.
			if len(holder) < 3 {
				return cleanup, fmt.Errorf("pod %s asks for %s and was left waiting while only %d node(s) held such a pod: a worker still had room\nthe program said:\n%s", name, what, len(holder), tail(p.Stdout()))
			}
			return cleanup, nil
		}

		if other, ok := holder[node]; ok {
			return cleanup, fmt.Errorf("pods %s and %s each ask for %s, which a node can give only once, and both were bound to %s", other, name, what, node)
		}
		holder[node] = name

		if err := waitFor(ctx, "pod "+name+" to run", 60*time.Second, func(ctx context.Context) (bool, error) {
			got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if got.Status.Phase == corev1.PodFailed {
				return false, fmt.Errorf("the kubelet on %s refused it: %s", node, got.Status.Message)
			}
			return got.Status.Phase == corev1.PodRunning, nil
		}); err != nil {
			return cleanup, fmt.Errorf("pod %s was bound to %s but did not run: %w", name, node, err)
		}
	}
	return cleanup, fmt.Errorf("six pods each asking for %s were all bound: a node can give it only once", what)
}

// stageFitPorts checks the program keeps a host port to one pod per node.
//
// Pods asking for host port 8080 are created one at a time until one has
// nowhere to go. A pod asking for 8081 must then still find a node: a port is
// held by its number and protocol, not by the node.
func stageFitPorts(ctx context.Context, env *kube.Env, bin string) error {
	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	done, err := fillNodes(ctx, env, p, "port", "host port 8080", func(name string) error {
		return seedPortPod(ctx, env, name, 8080)
	})
	defer done()
	if err != nil {
		return err
	}

	if err := seedPortPod(ctx, env, "other-port", 8081); err != nil {
		return err
	}
	if err := waitFor(ctx, "pod other-port to be bound", 30*time.Second, func(ctx context.Context) (bool, error) {
		got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, "other-port", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return got.Spec.NodeName != "", nil
	}); err != nil {
		return fmt.Errorf("pod other-port asks for host port 8081, which no node holds, and was left waiting: a port is held by its number and protocol, not by the node\nthe program said:\n%s", tail(p.Stdout()))
	}
	return nil
}

// seedPortPod creates a pod naming this scheduler that claims a TCP port on
// its node's own network.
func seedPortPod(ctx context.Context, env *kube.Env, name string, port int32) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "registry.k8s.io/pause:3.9",
				Ports: []corev1.ContainerPort{{ContainerPort: 80, HostPort: port, Protocol: corev1.ProtocolTCP}},
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// stageTaints checks the program keeps pods off nodes that say keep off.
//
// A taint is the node's own refusal, and nothing downstream enforces the
// NoSchedule kind: a pod bound to a tainted node it does not tolerate is run by
// the kubelet as if nothing were wrong. The control plane has carried such a
// taint all course, so this stage also expects pods to stop landing there.
func stageTaints(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	tainted := workers[0]
	const key = "byok8s.dev/stage-dedicated"
	if err := taintNode(ctx, env, tainted, key, "yes", corev1.TaintEffectNoSchedule); err != nil {
		return err
	}
	defer taintNode(context.WithoutCancel(ctx), env, tainted, key, "", "")

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// Pods that tolerate nothing: none may land on the tainted worker, and none
	// on the control plane, which carries a NoSchedule taint of its own.
	for i := 1; i <= 4; i++ {
		name := fmt.Sprintf("plain-%d", i)
		if err := seedPod(ctx, env, name, schedulerName); err != nil {
			return err
		}
		node, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s tolerates nothing and was never bound, though two workers carry no taint: %w\nthe program said:\n%s", name, err, tail(p.Stdout()))
		}
		if node == tainted {
			return fmt.Errorf("pod %s tolerates nothing and was bound to %s, which is tainted %s=yes:NoSchedule: nothing downstream refuses it, so the scheduler must", name, node, key)
		}
		if strings.HasSuffix(node, "control-plane") {
			return fmt.Errorf("pod %s tolerates nothing and was bound to %s, which carries node-role.kubernetes.io/control-plane:NoSchedule", name, node)
		}
	}

	// A pod that tolerates the taint may go there — and with every other node
	// already holding one of the pods above, it is the only place left.
	tolerations := []corev1.Toleration{{
		Key:      key,
		Operator: corev1.TolerationOpEqual,
		Value:    "yes",
		Effect:   corev1.TaintEffectNoSchedule,
	}}
	if err := seedToleratingPod(ctx, env, "tolerating", tolerations); err != nil {
		return err
	}
	if _, err := boundNode(ctx, env, "tolerating", 60*time.Second); err != nil {
		return fmt.Errorf("pod tolerating carries a toleration for %s=yes:NoSchedule and was never bound: a taint keeps out only the pods that do not answer it: %w\nthe program said:\n%s", key, err, tail(p.Stdout()))
	}
	return nil
}

// stageUnschedulable checks the program respects a node closed to new work,
// and the one thing that reopens it for a pod.
//
// Cordoning sets spec.unschedulable and the node controller adds
// node.kubernetes.io/unschedulable:NoSchedule to match. Plain pods must go
// elsewhere; a pod tolerating that taint may still be placed there, which is
// what keeps a DaemonSet running on a node being drained. The tolerating pod
// also carries a selector for the cordoned node, so that node is the only
// candidate it has and being placed anywhere else is a failure.
func stageUnschedulable(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	closed := workers[0]
	const key = "byok8s.dev/stage-cordoned"
	if err := labelNode(ctx, env, closed, key, "yes"); err != nil {
		return err
	}
	defer labelNode(context.WithoutCancel(ctx), env, closed, key, "")
	if err := cordonNode(ctx, env, closed, true); err != nil {
		return err
	}
	defer cordonNode(context.WithoutCancel(ctx), env, closed, false)

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("plain-%d", i)
		if err := seedPod(ctx, env, name, schedulerName); err != nil {
			return err
		}
		node, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s tolerates nothing and was never bound, though two workers are open: %w\nthe program said:\n%s", name, err, tail(p.Stdout()))
		}
		if node == closed {
			return fmt.Errorf("pod %s was bound to %s, which is cordoned: spec.unschedulable is the operator saying take nothing new, and nothing downstream refuses it", name, node)
		}
	}

	// A pod that answers the cordon taint, and can go nowhere else.
	tolerations := []corev1.Toleration{{
		Key:      "node.kubernetes.io/unschedulable",
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	}}
	if err := seedPodWith(ctx, env, "tolerating", map[string]string{key: "yes"}, tolerations); err != nil {
		return err
	}
	node, err := boundNode(ctx, env, "tolerating", 60*time.Second)
	if err != nil {
		return fmt.Errorf("pod tolerating carries a toleration for node.kubernetes.io/unschedulable and a selector only %s satisfies, and was never bound: a cordoned node is still a candidate for a pod that tolerates being there: %w\nthe program said:\n%s", closed, err, tail(p.Stdout()))
	}
	if node != closed {
		return fmt.Errorf("pod tolerating selects %s=yes, which only %s carries, and was bound to %s", key, closed, node)
	}
	return nil
}

// stageNodeAffinity checks the program honours required node affinity, whose
// rules are not the node selector's.
//
// Two workers are labelled for the length of the stage: one zone=west and
// disk=ssd, the other disk=nvme. Four pods then ask four different questions —
// terms are ORed, the expressions inside a term are ANDed, NotIn is satisfied
// by a node that lacks the key entirely, and a pod nothing matches waits.
func stageNodeAffinity(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 2 {
		return fmt.Errorf("this stage needs two worker nodes, and the cluster has %d\n  fix: byok8s down && byok8s up", len(workers))
	}
	west, other := workers[0], workers[1]
	const zone, disk = "byok8s.dev/stage-zone", "byok8s.dev/stage-disk"
	for _, l := range []struct{ node, key, value string }{
		{west, zone, "west"}, {west, disk, "ssd"}, {other, disk, "nvme"},
	} {
		if err := labelNode(ctx, env, l.node, l.key, l.value); err != nil {
			return err
		}
		defer labelNode(context.WithoutCancel(ctx), env, l.node, l.key, "")
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	in := func(key string, values ...string) corev1.NodeSelectorRequirement {
		return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpIn, Values: values}
	}
	notIn := func(key string, values ...string) corev1.NodeSelectorRequirement {
		return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpNotIn, Values: values}
	}
	exists := func(key string) corev1.NodeSelectorRequirement {
		return corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpExists}
	}
	term := func(reqs ...corev1.NodeSelectorRequirement) corev1.NodeSelectorTerm {
		return corev1.NodeSelectorTerm{MatchExpressions: reqs}
	}

	// Nothing matches this one: created first, so the others being placed
	// proves the program had its chance to decide about it.
	if err := seedAffinityPod(ctx, env, "nowhere", []corev1.NodeSelectorTerm{term(in(zone, "south"))}); err != nil {
		return err
	}

	cases := []struct {
		name  string
		terms []corev1.NodeSelectorTerm
		want  string
		why   string
	}{
		{"or-terms", []corev1.NodeSelectorTerm{term(in(zone, "south")), term(in(disk, "nvme"))}, other,
			"terms are ORed, and only the second one matches any node"},
		{"and-expressions", []corev1.NodeSelectorTerm{term(exists(disk), in(zone, "west"))}, west,
			"the expressions in one term are ANDed, and only one node carries both labels"},
	}
	for _, c := range cases {
		if err := seedAffinityPod(ctx, env, c.name, c.terms); err != nil {
			return err
		}
		node, err := boundNode(ctx, env, c.name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s was never bound, though %s: %w\nthe program said:\n%s", c.name, c.why, err, tail(p.Stdout()))
		}
		if node != c.want {
			return fmt.Errorf("pod %s was bound to %s, but %s, so it belongs on %s", c.name, node, c.why, c.want)
		}
	}

	// NotIn is satisfied by a node that does not carry the key at all, so this
	// pod may go anywhere except the one node labelled zone=west.
	if err := seedAffinityPod(ctx, env, "not-in-west", []corev1.NodeSelectorTerm{term(notIn(zone, "west"))}); err != nil {
		return err
	}
	node, err := boundNode(ctx, env, "not-in-west", 60*time.Second)
	if err != nil {
		return fmt.Errorf("pod not-in-west asks for a node whose zone is not west, which every node but %s satisfies — a node lacking the label entirely satisfies NotIn: %w\nthe program said:\n%s", west, err, tail(p.Stdout()))
	}
	if node == west {
		return fmt.Errorf("pod not-in-west was bound to %s, which is labelled zone=west", west)
	}

	got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, "nowhere", metav1.GetOptions{})
	if err != nil {
		return err
	}
	if got.Spec.NodeName != "" {
		return fmt.Errorf("pod nowhere asks for zone=south, which no node carries, and was bound to %s", got.Spec.NodeName)
	}
	return nil
}

// stageNodeSelector checks the program keeps a pod to the nodes its selector
// names.
//
// One worker is labelled for the length of the stage. Pods selecting that
// label must land there, and a pod selecting a label no node carries must be
// left waiting. That pod is created first, so by the time the others are
// placed the program has already decided about it.
func stageNodeSelector(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	chosen := workers[0]
	const key = "byok8s.dev/stage-selected"
	if err := labelNode(ctx, env, chosen, key, "yes"); err != nil {
		return err
	}
	defer labelNode(context.WithoutCancel(ctx), env, chosen, key, "")

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := seedSelectingPod(ctx, env, "nowhere", map[string]string{key: "no-such-node"}); err != nil {
		return err
	}
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("selecting-%d", i)
		if err := seedSelectingPod(ctx, env, name, map[string]string{key: "yes"}); err != nil {
			return err
		}
		node, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s selects %s=yes, which %s carries, and was never bound: a node needs at least the labels a selector names, not only them: %w\nthe program said:\n%s", name, key, chosen, err, tail(p.Stdout()))
		}
		if node != chosen {
			return fmt.Errorf("pod %s selects %s=yes, which only %s carries, and was bound to %s", name, key, chosen, node)
		}
	}

	got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, "nowhere", metav1.GetOptions{})
	if err != nil {
		return err
	}
	if got.Spec.NodeName != "" {
		return fmt.Errorf("pod nowhere selects %s=no-such-node, which no node carries, and was bound to %s: when the selector leaves no candidate, the pod waits", key, got.Spec.NodeName)
	}
	return nil
}

// seedAffinityPod creates a pod naming this scheduler whose required node
// affinity is the given terms.
func seedAffinityPod(ctx context.Context, env *kube.Env, name string, terms []corev1.NodeSelectorTerm) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms},
				},
			},
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// podEvents returns the events a scheduler recorded about one pod in this
// stage's namespace, in the current API — the one kubectl describe reads.
//
// Events are matched on the pod's UID, not its name. The namespace is per
// stage and outlives a single run, and events stay for an hour, so a run that
// matched on the name alone would read the previous run's answers about a pod
// with the same name. The kubelet's own Normal events about a running pod
// (Pulled, Created, Started) are left out too: they carry the right UID but
// say nothing about scheduling, and they arrive in no fixed order relative to
// the scheduler's.
func podEvents(ctx context.Context, env *kube.Env, name string) ([]eventsv1.Event, error) {
	pod, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get pod %s: %w", name, err)
	}
	list, err := env.Client.EventsV1().Events(env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	var out []eventsv1.Event
	for _, e := range list.Items {
		if e.Regarding.UID == pod.UID && e.ReportingController != "kubelet" {
			out = append(out, e)
		}
	}
	return out, nil
}

// cordonNode closes a node to new work, or opens it again. It is what kubectl
// cordon does: set the field, and let the node controller add the taint.
func cordonNode(ctx context.Context, env *kube.Env, node string, closed bool) error {
	patch := fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, closed)
	if _, err := env.Client.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("cordon node %s: %w", node, err)
	}
	return nil
}

// seedPodWith creates a pod naming this scheduler that carries both a node
// selector and tolerations, for the stages where one without the other would
// not pin the pod to a single node.
func seedPodWith(ctx context.Context, env *kube.Env, name string, selector map[string]string, tolerations []corev1.Toleration) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			NodeSelector:  selector,
			Tolerations:   tolerations,
			Containers:    []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// taintNode adds a taint to a node, or removes the taint with that key when
// effect is empty. The whole list is sent back, since a taint is an item in a
// slice rather than a field of its own.
func taintNode(ctx context.Context, env *kube.Env, node, key, value string, effect corev1.TaintEffect) error {
	got, err := env.Client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read node %s: %w", node, err)
	}
	var keep []corev1.Taint
	for _, t := range got.Spec.Taints {
		if t.Key != key {
			keep = append(keep, t)
		}
	}
	if effect != "" {
		keep = append(keep, corev1.Taint{Key: key, Value: value, Effect: effect})
	}
	got.Spec.Taints = keep
	if _, err := env.Client.CoreV1().Nodes().Update(ctx, got, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("taint node %s: %w", node, err)
	}
	return nil
}

// seedToleratingPod creates a pod naming this scheduler that carries the given
// tolerations.
func seedToleratingPod(ctx context.Context, env *kube.Env, name string, tolerations []corev1.Toleration) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Tolerations:   tolerations,
			Containers:    []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// labelNode sets a label on a node, or removes it when value is empty.
func labelNode(ctx context.Context, env *kube.Env, node, key, value string) error {
	v := "null"
	if value != "" {
		v = fmt.Sprintf("%q", value)
	}
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%s}}}`, key, v)
	if _, err := env.Client.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("label node %s: %w", node, err)
	}
	return nil
}

// seedSelectingPod creates a pod naming this scheduler with a node selector.
func seedSelectingPod(ctx context.Context, env *kube.Env, name string, selector map[string]string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			NodeSelector:  selector,
			Containers:    []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// boundNode waits for a pod to be bound and returns the node it went to.
func boundNode(ctx context.Context, env *kube.Env, name string, within time.Duration) (string, error) {
	var node string
	err := waitFor(ctx, "pod "+name+" to be bound", within, func(ctx context.Context) (bool, error) {
		got, err := env.Client.CoreV1().Pods(env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		node = got.Spec.NodeName
		return node != "", nil
	})
	return node, err
}

// seedRequestingPod creates a pod naming this scheduler that asks for amount of
// one resource.
func seedRequestingPod(ctx context.Context, env *kube.Env, name string, resource corev1.ResourceName, amount string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "registry.k8s.io/pause:3.9",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{resource: apiresource.MustParse(amount)},
				},
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
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
