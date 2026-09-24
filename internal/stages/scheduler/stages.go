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
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
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

// The second profile the program serves from stage 22 on: the same filters,
// with the scores weighed to pack pods onto nodes already in use.
const packingProfile = schedulerName + "-packing"

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
	register(Stage{Slug: "requeue", Run: stageRequeue})
	register(Stage{Slug: "score-least-allocated", Run: stageScoreLeastAllocated})
	register(Stage{Slug: "score-balanced", Run: stageScoreBalanced})
	register(Stage{Slug: "score-image-locality", Run: stageScoreImageLocality})
	register(Stage{Slug: "spread-by-owner", Run: stageSpreadByOwner})
	register(Stage{Slug: "topology-spread", Run: stageTopologySpread})
	register(Stage{Slug: "pod-affinity", Run: stagePodAffinity})
	register(Stage{Slug: "volume-binding", Run: stageVolumeBinding})
	register(Stage{Slug: "priority", Run: stagePriority})
	register(Stage{Slug: "preemption", Run: stagePreemption})
	register(Stage{Slug: "framework-plugins", Run: stageFrameworkPlugins})
	register(Stage{Slug: "multi-profile", Run: stageMultiProfile})
	register(Stage{Slug: "percentage-of-nodes", Run: stagePercentageOfNodes})
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

// stageScoreLeastAllocated checks the program prefers the node with room over
// one that merely fits.
//
// One worker is loaded to roughly three quarters of its CPU by a pod pinned
// with spec.nodeName, which no scheduler is involved in. Small pods then have
// somewhere better to be, and a program that takes nodes in turn will put one
// on the loaded node anyway.
func stageScoreLeastAllocated(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 2 {
		return fmt.Errorf("this stage needs at least two workers, found %d", len(workers))
	}
	loaded := workers[0]

	node, err := env.Client.CoreV1().Nodes().Get(ctx, loaded, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", loaded, err)
	}
	cpu := node.Status.Allocatable[corev1.ResourceCPU]
	ballast := cpu.MilliValue() * 75 / 100
	if err := seedBallast(ctx, env, "ballast", loaded, milliCPU(ballast)); err != nil {
		return err
	}
	// The namespace outlives this run, and the ballast holds three quarters of
	// a worker: left behind, it shrinks the cluster for every stage graded
	// after this one.
	zero := int64(0)
	defer func() {
		_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), "ballast",
			metav1.DeleteOptions{GracePeriodSeconds: &zero})
	}()

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// One pod per worker and one more: a program that ignores the scores and
	// takes nodes in turn has to land on the loaded one within this many.
	for _, name := range []string{"small-1", "small-2", "small-3", "small-4"} {
		if err := seedPod(ctx, env, name, schedulerName); err != nil {
			return err
		}
		where, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s was never placed: %w\nthe program said:\n%s", name, err, tail(p.Stdout()))
		}
		if where == loaded {
			return fmt.Errorf("pod %s went to %s, which is already %dm of %dm spoken for, while another worker sits empty: a node that fits is not the same as the best node for the pod\nthe program said:\n%s", name, loaded, ballast, cpu.MilliValue(), tail(p.Stdout()))
		}
	}
	return nil
}

// stageTopologySpread checks a pod's own spread constraint is obeyed, and
// obeyed as a filter: DoNotSchedule means a node that would break it is not a
// node, however good it looks otherwise.
//
// Two workers are put in one zone and one in another, and the zone with two
// workers is given a matching pod on each. A pod spreading by zone with a max
// skew of one then has only the far zone open to it — which is the zone with
// the ballast on it, so room alone says the opposite.
func stageTopologySpread(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 3 {
		return fmt.Errorf("this stage needs three workers, found %d", len(workers))
	}
	near, far := workers[:2], workers[2]

	const zoneKey = "topology.kubernetes.io/zone"
	zones := map[string]string{near[0]: "near", near[1]: "near", far: "far"}
	for node, zone := range zones {
		if err := labelNode(ctx, env, node, zoneKey, zone); err != nil {
			return err
		}
		defer func() { _ = labelNode(context.WithoutCancel(ctx), env, node, zoneKey, "") }()
	}

	node, err := env.Client.CoreV1().Nodes().Get(ctx, far, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", far, err)
	}
	cpu := node.Status.Allocatable[corev1.ResourceCPU]

	zero := int64(0)
	seeded := []string{"ballast-far", "held-1", "held-2"}
	defer func() {
		for _, name := range seeded {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	// The far zone is the one the constraint leaves open, so it is the one
	// given ballast: a program that reads the constraint goes there anyway.
	if err := seedBallast(ctx, env, "ballast-far", far, milliCPU(cpu.MilliValue()*10/100)); err != nil {
		return err
	}
	// One matching pod on each near worker, pinned, so the near zone holds two
	// and the far zone none before the program starts.
	spread := map[string]string{"app": "spread"}
	for i, n := range near {
		if err := seedLabelledPod(ctx, env, fmt.Sprintf("held-%d", i+1), n, spread); err != nil {
			return err
		}
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// near holds 2 and far holds 0, so a third in near would skew by 3, and a
	// fourth by 2 once far holds one. Both belong in the far zone; a fifth
	// would be free to go either way, so the run stops at two.
	constraint := corev1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       zoneKey,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: spread},
	}
	for _, name := range []string{"even-1", "even-2"} {
		seeded = append(seeded, name)
		if err := seedSpreadingPod(ctx, env, name, spread, constraint); err != nil {
			return err
		}
		where, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s spreads by %s with a max skew of 1 and %s is the only zone that keeps it: %w\nthe program said:\n%s", name, zoneKey, far, err, tail(p.Stdout()))
		}
		if where != far {
			return fmt.Errorf("pod %s spreads by %s with a max skew of 1 and went to %s, in a zone already holding two matching pods against the other zone's none: a constraint that says DoNotSchedule takes the node away, however much room it has\nthe program said:\n%s",
				name, zoneKey, where, tail(p.Stdout()))
		}
	}

	// A key no node carries puts every node outside every domain, so there is
	// nowhere the pod can go — not a node with a bad score, no node at all.
	nowhere := constraint
	nowhere.TopologyKey = "topology.kubernetes.io/rack"
	seeded = append(seeded, "no-rack")
	if err := seedSpreadingPod(ctx, env, "no-rack", spread, nowhere); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, env.Namespace+"/no-rack", 30*time.Second); err != nil {
		return fmt.Errorf("pod no-rack was never reported: %w", err)
	}
	if where, err := boundNode(ctx, env, "no-rack", 20*time.Second); err == nil {
		return fmt.Errorf("pod no-rack spreads by a topology key no node carries and was placed on %s anyway: a node with no value for the key is in no domain, so it cannot take a pod spreading across them\nthe program said:\n%s", where, tail(p.Stdout()))
	}
	return nil
}

// stagePodAffinity checks required inter-pod affinity and anti-affinity are
// obeyed, and obeyed as filters: where a pod goes can depend on the pods
// already there, and no score gets a say in it.
//
// One worker is given ballast and the pod others want to sit beside, so
// affinity has to argue against room. The two emptiest workers are given the
// pods an anti-affinity term refuses to share a node with, so anti-affinity
// argues against room too. The last pod is then refused everywhere, by a pod
// this scheduler placed itself.
func stagePodAffinity(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 3 {
		return fmt.Errorf("this stage needs three workers, found %d", len(workers))
	}
	empty, crowded := workers[:2], workers[2]

	node, err := env.Client.CoreV1().Nodes().Get(ctx, crowded, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", crowded, err)
	}
	cpu := node.Status.Allocatable[corev1.ResourceCPU]

	zero := int64(0)
	seeded := []string{"ballast-crowded", "cache", "web-1", "web-2"}
	defer func() {
		for _, name := range seeded {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	// The node the terms point at is the one room argues against.
	if err := seedBallast(ctx, env, "ballast-crowded", crowded, milliCPU(cpu.MilliValue()*20/100)); err != nil {
		return err
	}
	if err := seedLabelledPod(ctx, env, "cache", crowded, map[string]string{"app": "cache"}); err != nil {
		return err
	}
	web := map[string]string{"app": "web"}
	for i, n := range empty {
		if err := seedLabelledPod(ctx, env, fmt.Sprintf("web-%d", i+1), n, web); err != nil {
			return err
		}
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	const hostKey = "kubernetes.io/hostname"
	beside := &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey:   hostKey,
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cache"}},
		}},
	}}
	seeded = append(seeded, "near-cache")
	if err := seedCompanyPod(ctx, env, "near-cache", nil, beside); err != nil {
		return err
	}
	where, err := boundNode(ctx, env, "near-cache", 60*time.Second)
	if err != nil {
		return fmt.Errorf("pod near-cache requires a pod labelled app=cache on its node, and only %s has one: %w\nthe program said:\n%s", crowded, err, tail(p.Stdout()))
	}
	if where != crowded {
		return fmt.Errorf("pod near-cache requires a pod labelled app=cache on its node and went to %s, which has none: required pod affinity takes every other node away, including the ones with more room\nthe program said:\n%s",
			where, tail(p.Stdout()))
	}

	away := &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey:   hostKey,
			LabelSelector: &metav1.LabelSelector{MatchLabels: web},
		}},
	}}
	seeded = append(seeded, "apart")
	if err := seedCompanyPod(ctx, env, "apart", web, away); err != nil {
		return err
	}
	where, err = boundNode(ctx, env, "apart", 60*time.Second)
	if err != nil {
		return fmt.Errorf("pod apart refuses a node holding a pod labelled app=web, which leaves only %s: %w\nthe program said:\n%s", crowded, err, tail(p.Stdout()))
	}
	if where != crowded {
		return fmt.Errorf("pod apart refuses a node holding a pod labelled app=web and went to %s, which holds one: anti-affinity removes the node however empty it is\nthe program said:\n%s",
			where, tail(p.Stdout()))
	}

	// apart carries app=web itself, so the placement just made leaves every
	// worker holding one, and the next pod refusing their company has nowhere
	// left — counted from the pods, not from a snapshot taken at startup.
	seeded = append(seeded, "crowded-out")
	if err := seedCompanyPod(ctx, env, "crowded-out", web, away); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, env.Namespace+"/crowded-out", 30*time.Second); err != nil {
		return fmt.Errorf("pod crowded-out was never reported: %w", err)
	}
	if where, err := boundNode(ctx, env, "crowded-out", 20*time.Second); err == nil {
		return fmt.Errorf("pod crowded-out refuses a node holding a pod labelled app=web and every worker now holds one, but it was placed on %s anyway: a pod with no node left waits\nthe program said:\n%s",
			where, tail(p.Stdout()))
	}
	return nil
}

// seedCompanyPod creates a pod naming this scheduler that carries inter-pod
// affinity terms, and the labels another pod's terms select it by.
func seedCompanyPod(ctx context.Context, env *kube.Env, name string, labels map[string]string, affinity *corev1.Affinity) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace, Labels: labels},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Affinity:      affinity,
			Containers:    []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// seedLabelledPod pins a labelled pod to a node with spec.nodeName, so it
// counts towards a spread without any scheduler having placed it.
func seedLabelledPod(ctx context.Context, env *kube.Env, name, node string, labels map[string]string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace, Labels: labels},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// seedSpreadingPod creates a pod naming this scheduler that carries a topology
// spread constraint, and the labels its own selector matches.
func seedSpreadingPod(ctx context.Context, env *kube.Env, name string, labels map[string]string, constraint corev1.TopologySpreadConstraint) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace, Labels: labels},
		Spec: corev1.PodSpec{
			SchedulerName:             schedulerName,
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{constraint},
			Containers:                []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// stageSpreadByOwner checks replicas of one thing are kept apart, even when
// the node they are already on is the one with the most room.
//
// A ReplicaSet wants five pods. Two are put on one worker by hand, pinned with
// spec.nodeName so no scheduler chose it, and the other two workers are given
// a little ballast so that crowded worker is also the emptiest. Room says put
// the remaining three there with their siblings; a node failing then takes the
// whole ReplicaSet with it.
func stageSpreadByOwner(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 3 {
		return fmt.Errorf("this stage needs three workers, found %d", len(workers))
	}
	crowded, rest := workers[0], workers[1:]

	node, err := env.Client.CoreV1().Nodes().Get(ctx, crowded, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", crowded, err)
	}
	cpu := node.Status.Allocatable[corev1.ResourceCPU]

	// Enough to make the crowded node the best answer on room, and not enough
	// to stop anything fitting anywhere.
	zero := int64(0)
	defer func() {
		for _, n := range rest {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), "ballast-"+n,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	for _, n := range rest {
		if err := seedBallast(ctx, env, "ballast-"+n, n, milliCPU(cpu.MilliValue()*10/100)); err != nil {
			return err
		}
	}

	// A real owner, because a pod whose owner does not exist is garbage
	// collected out from under the test. The ReplicaSet is what makes these
	// pods replicas of one thing rather than five unrelated pods.
	rs, err := seedReplicaSet(ctx, env, "web", 5)
	if err != nil {
		return err
	}
	defer func() {
		policy := metav1.DeletePropagationBackground
		_ = env.Client.AppsV1().ReplicaSets(env.Namespace).Delete(context.WithoutCancel(ctx), "web",
			metav1.DeleteOptions{PropagationPolicy: &policy, GracePeriodSeconds: &zero})
	}()

	// Two of the five, placed by hand where no scheduler would be blamed for
	// them. The ReplicaSet counts them as its own, so it asks for three more.
	for _, name := range []string{"sibling-1", "sibling-2"} {
		if err := seedOwnedPod(ctx, env, name, crowded, rs); err != nil {
			return err
		}
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// The three the controller creates are the ones under test, and they are
	// named by the controller, so they are found by what they are rather than
	// by name.
	var placed map[string]string
	if err := waitFor(ctx, "the ReplicaSet's remaining pods to be placed", 90*time.Second, func(ctx context.Context) (bool, error) {
		placed, err = ownedPlacements(ctx, env, rs.UID)
		if err != nil {
			return false, err
		}
		return len(placed) == 5, nil
	}); err != nil {
		return fmt.Errorf("%w: %d of the ReplicaSet's 5 pods have a node\nthe program said:\n%s", err, len(placed), tail(p.Stdout()))
	}

	var stacked []string
	for name, where := range placed {
		if where == crowded && name != "sibling-1" && name != "sibling-2" {
			stacked = append(stacked, name)
		}
	}
	if len(stacked) > 0 {
		slices.Sort(stacked)
		return fmt.Errorf("%s joined two pods of the same ReplicaSet on %s, which has the most room of the three workers: replicas on one node fail together, and room is not the only thing worth scoring\nthe program said:\n%s",
			strings.Join(stacked, ", "), crowded, tail(p.Stdout()))
	}
	return nil
}

// seedReplicaSet creates a ReplicaSet whose pods name this scheduler. It is
// the owner the spread is judged by; its pods ask for nothing, so room alone
// would put them all in the same place.
func seedReplicaSet(ctx context.Context, env *kube.Env, name string, replicas int32) (*appsv1.ReplicaSet, error) {
	labels := map[string]string{"app": name}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					SchedulerName: schedulerName,
					Containers:    []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
				},
			},
		},
	}
	got, err := env.Client.AppsV1().ReplicaSets(env.Namespace).Create(ctx, rs, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create replicaset %s: %w", name, err)
	}
	return got, nil
}

// seedOwnedPod pins a pod to a node and gives it the ReplicaSet as its
// controller, so the ReplicaSet counts it among its replicas and the learner's
// program sees a sibling already placed.
func seedOwnedPod(ctx context.Context, env *kube.Env, name, node string, rs *appsv1.ReplicaSet) error {
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: env.Namespace,
			Labels:    rs.Spec.Selector.MatchLabels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       rs.Name,
				UID:        rs.UID,
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.9"}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed owned pod %s: %w", name, err)
	}
	return nil
}

// ownedPlacements maps the name of every bound pod this owner controls to the
// node it is on.
func ownedPlacements(ctx context.Context, env *kube.Env, owner types.UID) (map[string]string, error) {
	pods, err := env.Client.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	placed := map[string]string{}
	for _, pod := range pods.Items {
		for _, ref := range pod.OwnerReferences {
			if ref.UID == owner && pod.Spec.NodeName != "" {
				placed[pod.Name] = pod.Spec.NodeName
			}
		}
	}
	return placed, nil
}

// localityImage is the image this stage warms one node with: large enough to
// be worth preferring a node for, and not one kind puts on every node.
const localityImage = "registry.k8s.io/e2e-test-images/agnhost:2.53"

// stageScoreImageLocality checks the program prefers a node that can start the
// pod now over one that has to download it first.
//
// One worker is made to pull the image by a pod pinned to it, which no
// scheduler is involved in; the pod is then deleted and the image stays in
// that node's cache. The pods that follow ask for nothing, so every worker is
// equal on room and on evenness, and the image is the only thing to tell them
// apart.
func stageScoreImageLocality(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 2 {
		return fmt.Errorf("this stage needs at least two workers, found %d", len(workers))
	}
	warm := workers[len(workers)-1]

	// The image is meant to be the only thing between the workers, so whatever
	// earlier stages left on them is levelled out first.
	unlevel, err := levelWorkers(ctx, env)
	defer unlevel()
	if err != nil {
		return err
	}

	if err := warmImage(ctx, env, warm); err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// One per worker and one more: a program that cannot tell the nodes apart
	// takes them in turn, so it has to miss within this many.
	names := []string{"local-1", "local-2", "local-3", "local-4"}
	zero := int64(0)
	defer func() {
		for _, name := range names {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	for _, name := range names {
		if err := seedImagePod(ctx, env, name, localityImage, corev1.PullNever); err != nil {
			return err
		}
		where, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s was never placed: %w\nthe program said:\n%s", name, err, tail(p.Stdout()))
		}
		if where != warm {
			return fmt.Errorf("pod %s runs %s, which %s already holds, and it went to %s, which has to download it first: every worker has the same room here, so the image is the only thing between them\nthe program said:\n%s",
				name, localityImage, warm, where, tail(p.Stdout()))
		}
	}
	return nil
}

// warmImage leaves the stage's image in one node's cache, by running a pod
// there that pulls it. The pod is pinned with spec.nodeName, so no scheduler
// decided this, and it is deleted again: the image is what stays behind.
//
// The node reports its images on its own schedule, so having run the pod is
// not the same as the program being able to see the image, and the wait is for
// the node to say so.
// levelWorkers pins ballast until every worker has the same cpu and memory
// spoken for, and returns a cleanup that takes it away again.
//
// A stage that asserts on any score other than room needs room not to be the
// thing that differs. Earlier stages leave their pods behind until their own
// namespace is next reset, and on a four-cpu runner one leftover request is
// worth more than the score under test — which passes on a big machine and
// fails on a small one.
func levelWorkers(ctx context.Context, env *kube.Env) (func(), error) {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return func() {}, err
	}
	pods, err := env.Client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return func() {}, fmt.Errorf("list pods: %w", err)
	}
	cpu, mem := map[string]int64{}, map[string]int64{}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, c := range pod.Spec.Containers {
			cpu[pod.Spec.NodeName] += c.Resources.Requests.Cpu().MilliValue()
			mem[pod.Spec.NodeName] += c.Resources.Requests.Memory().Value()
		}
	}
	var mostCPU, mostMem int64
	for _, w := range workers {
		mostCPU, mostMem = max(mostCPU, cpu[w]), max(mostMem, mem[w])
	}

	var created []string
	cleanup := func() {
		zero := int64(0)
		for _, name := range created {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}
	for _, w := range workers {
		if cpu[w] == mostCPU && mem[w] == mostMem {
			continue
		}
		name := "level-" + w
		created = append(created, name)
		if err := seedBallast(ctx, env, name, w, corev1.ResourceList{
			corev1.ResourceCPU:    *apiresource.NewMilliQuantity(mostCPU-cpu[w], apiresource.DecimalSI),
			corev1.ResourceMemory: *apiresource.NewQuantity(mostMem-mem[w], apiresource.BinarySI),
		}); err != nil {
			return cleanup, err
		}
	}
	return cleanup, nil
}

func warmImage(ctx context.Context, env *kube.Env, node string) error {
	zero := int64(0)
	defer func() {
		_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), "warm",
			metav1.DeleteOptions{GracePeriodSeconds: &zero})
	}()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			NodeName:      node,
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "app",
				Image:   localityImage,
				Command: []string{"/agnhost", "pause"},
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("pin a pod to %s to pull %s: %w", node, localityImage, err)
	}

	return waitFor(ctx, "node "+node+" to report "+localityImage, 5*time.Minute, func(ctx context.Context) (bool, error) {
		got, err := env.Client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, img := range got.Status.Images {
			if slices.Contains(img.Names, localityImage) {
				return true, nil
			}
		}
		return false, nil
	})
}

// seedImagePod creates a pod naming this scheduler that runs one image and
// asks for nothing. Never pulling is what keeps a misplaced pod from warming
// the cache of the node it should not have gone to, which would leave the
// cluster with two warm nodes for the next run.
func seedImagePod(ctx context.Context, env *kube.Env, name, image string, pull corev1.PullPolicy) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Containers: []corev1.Container{{
				Name:            "app",
				Image:           image,
				Command:         []string{"/agnhost", "pause"},
				ImagePullPolicy: pull,
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// stageScoreBalanced checks the program weighs how evenly a node is used, not
// only how much of it is left.
//
// Each worker is loaded differently, so neither half of the score is enough on
// its own. One is lopsided — 60% of its CPU spoken for and none of its memory
// — and room alone prefers it, because the untouched memory carries the
// average. One is evenly 70% through both, and evenness alone is happy with
// it. Only the node that is even *and* has room, at 35% of each, is right by
// both.
func stageScoreBalanced(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 3 {
		return fmt.Errorf("this stage needs three workers, found %d", len(workers))
	}
	lopsided, level, full := workers[0], workers[1], workers[2]

	node, err := env.Client.CoreV1().Nodes().Get(ctx, lopsided, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", lopsided, err)
	}
	cpu := node.Status.Allocatable[corev1.ResourceCPU]
	mem := node.Status.Allocatable[corev1.ResourceMemory]

	// A pod asking for the same share of each: it leaves the level node level,
	// so every pod in the run faces the same choice as the first.
	ask := corev1.ResourceList{
		corev1.ResourceCPU:    *apiresource.NewMilliQuantity(cpu.MilliValue()*5/100, apiresource.DecimalSI),
		corev1.ResourceMemory: *apiresource.NewQuantity(mem.Value()*5/100, apiresource.BinarySI),
	}

	// Only these two nodes are in the running, so the choice is between them
	// rather than between them and an empty third worker. The label is the
	// cluster's, not this namespace's, so it goes whatever happens here.
	const pickKey = "byok8s.io/score-balanced"
	for _, n := range []string{lopsided, level, full} {
		if err := labelNode(ctx, env, n, pickKey, "candidate"); err != nil {
			return err
		}
		defer func() { _ = labelNode(context.WithoutCancel(ctx), env, n, pickKey, "") }()
	}

	ballasts := []struct {
		name string
		node string
		asks corev1.ResourceList
	}{
		{"lopsided", lopsided, milliCPU(cpu.MilliValue() * 60 / 100)},
		{"level", level, evenLoad(cpu, mem, 35)},
		{"full", full, evenLoad(cpu, mem, 70)},
	}
	// Ballast left behind holds most of two workers: every stage graded after
	// this one would be scheduling on a cluster this one shrank.
	zero := int64(0)
	defer func() {
		for _, b := range ballasts {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), b.name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	for _, b := range ballasts {
		if err := seedBallast(ctx, env, b.name, b.node, b.asks); err != nil {
			return err
		}
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// These ask for real cpu and memory, so one left behind on a failed run
	// shrinks a worker for every stage graded after this one.
	placed := []string{"even-1", "even-2", "even-3"}
	defer func() {
		for _, name := range placed {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	for _, name := range placed {
		if err := seedAskingPod(ctx, env, name, map[string]string{pickKey: "candidate"}, ask); err != nil {
			return err
		}
		where, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s was never placed: %w\nthe program said:\n%s", name, err, tail(p.Stdout()))
		}
		if where != level {
			why := "which is evenly 70% through both and has least room of the three"
			if where == lopsided {
				why = "which has 60% of its cpu spoken for and none of its memory, so free memory flatters its average while the cpu is what will run out"
			}
			return fmt.Errorf("pod %s asks for cpu and memory together and went to %s, %s, while %s is evenly 35%% through both\na node has to have room and be even in it, which is why the two scores are added\nthe program said:\n%s",
				name, where, why, level, tail(p.Stdout()))
		}
	}
	return nil
}

// evenLoad is a request list holding the same percentage of a node's cpu and
// of its memory.
func evenLoad(cpu, mem apiresource.Quantity, percent int64) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    *apiresource.NewMilliQuantity(cpu.MilliValue()*percent/100, apiresource.DecimalSI),
		corev1.ResourceMemory: *apiresource.NewQuantity(mem.Value()*percent/100, apiresource.BinarySI),
	}
}

// milliCPU is a request list for CPU alone.
func milliCPU(milli int64) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: *apiresource.NewMilliQuantity(milli, apiresource.DecimalSI)}
}

// seedAskingPod creates a pod naming this scheduler with both a node selector
// and resource requests.
func seedAskingPod(ctx context.Context, env *kube.Env, name string, selector map[string]string, asks corev1.ResourceList) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			NodeSelector:  selector,
			Containers: []corev1.Container{{
				Name:      "app",
				Image:     "registry.k8s.io/pause:3.9",
				Resources: corev1.ResourceRequirements{Requests: asks},
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// seedBallast pins a pod to one node with spec.nodeName and the given
// requests, so that node is spoken for without any scheduler having chosen it.
func seedBallast(ctx context.Context, env *kube.Env, name, node string, asks corev1.ResourceList) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{{
				Name:      "app",
				Image:     "registry.k8s.io/pause:3.9",
				Resources: corev1.ResourceRequirements{Requests: asks},
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed ballast pod %s: %w", name, err)
	}
	return nil
}

// stageRequeue checks a pod that found no node is tried again when the cluster
// changes, which is the difference between deciding once and scheduling.
//
// Every worker is closed before the program starts, so the pod it is given has
// nowhere to go. Opening the workers again is the only thing that happens
// next: nothing touches the pod, and no new pod arrives.
func stageRequeue(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	for _, n := range workers {
		if err := cordonNode(ctx, env, n, true); err != nil {
			return err
		}
	}
	// The nodes are shared with every later run, so they are opened again
	// whatever happens here.
	defer func() {
		for _, n := range workers {
			_ = cordonNode(context.WithoutCancel(ctx), env, n, false)
		}
	}()

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := seedPod(ctx, env, "later", schedulerName); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, "waiting", 60*time.Second); err != nil {
		return fmt.Errorf("every node is closed to new work and the program never reported pod later as waiting: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}

	for _, n := range workers {
		if err := cordonNode(ctx, env, n, false); err != nil {
			return err
		}
	}
	if _, err := boundNode(ctx, env, "later", 90*time.Second); err != nil {
		return fmt.Errorf("the nodes were opened again and pod later was never placed: a pod that could not be scheduled has to be tried again when the cluster changes, not decided once: %w\nthe program said:\n%s", err, tail(p.Stdout()))
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
// workerAllocatable is the smallest worker's allocatable resources, which is
// what a stage sizes its pods against. A request in whole cpus means one thing
// on a laptop with sixteen of them and another on a CI runner with four, and
// these stages assert on how many such pods a node can hold.
func workerAllocatable(ctx context.Context, env *kube.Env) (corev1.ResourceList, error) {
	list, err := env.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "!node-role.kubernetes.io/control-plane"})
	if err != nil {
		return nil, fmt.Errorf("list worker nodes: %w", err)
	}
	smallest := corev1.ResourceList{}
	for _, n := range list.Items {
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			have := n.Status.Allocatable[name]
			if held, ok := smallest[name]; !ok || have.Cmp(held) < 0 {
				smallest[name] = have
			}
		}
	}
	if len(smallest) == 0 {
		return nil, fmt.Errorf("the cluster has no worker nodes\n  fix: byok8s down && byok8s up")
	}
	return smallest, nil
}

// shareOfCPU is percent of a quantity of cpu, as a request to put on a pod.
func shareOfCPU(cpu apiresource.Quantity, percent int64) string {
	return fmt.Sprintf("%dm", cpu.MilliValue()*percent/100)
}

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

	alloc, err := workerAllocatable(ctx, env)
	if err != nil {
		return err
	}
	cpu, mem := alloc[corev1.ResourceCPU], alloc[corev1.ResourceMemory]
	// More than half a worker: one such pod fits on a node and two never do,
	// whatever size the workers happen to be.
	for _, ask := range []struct {
		resource corev1.ResourceName
		amount   string
	}{
		{corev1.ResourceCPU, shareOfCPU(cpu, 60)},
		{corev1.ResourceMemory, apiresource.NewQuantity(mem.Value()*60/100, apiresource.BinarySI).String()},
	} {
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

// stageVolumeBinding checks a pod goes where its volume already is.
//
// Two local volumes are pinned to different workers, each with a claim bound to
// it, and the worker holding the first is loaded so that room argues for
// somewhere else. A pod whose claim resolves to a volume on one node has one
// candidate, whatever the scores say; a pod whose claim does not exist has
// none, and waits.
func stageVolumeBinding(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 2 {
		return fmt.Errorf("this stage needs at least two workers, found %d", len(workers))
	}
	spare, crowded := workers[0], workers[len(workers)-1]

	node, err := env.Client.CoreV1().Nodes().Get(ctx, crowded, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", crowded, err)
	}
	cpu := node.Status.Allocatable[corev1.ResourceCPU]

	// PersistentVolumes are cluster scoped, so deleting the namespace does not
	// take them with it and a name reused across runs would meet the leftover.
	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	pvCrowded, pvSpare := "bko-sched-vol-"+run+"-a", "bko-sched-vol-"+run+"-b"

	zero := int64(0)
	seeded := []string{"ballast-crowded"}
	defer func() {
		back := context.WithoutCancel(ctx)
		for _, name := range seeded {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(back, name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
		for _, name := range []string{"data-crowded", "data-spare", "data-pending"} {
			_ = env.Client.CoreV1().PersistentVolumeClaims(env.Namespace).Delete(back, name, metav1.DeleteOptions{})
		}
		for _, name := range []string{pvCrowded, pvSpare} {
			_ = env.Client.CoreV1().PersistentVolumes().Delete(back, name, metav1.DeleteOptions{})
		}
	}()

	// The node the volume is on is the one room argues against.
	if err := seedBallast(ctx, env, "ballast-crowded", crowded, milliCPU(cpu.MilliValue()*60/100)); err != nil {
		return err
	}
	for _, v := range []struct{ pv, claim, node string }{
		{pvCrowded, "data-crowded", crowded},
		{pvSpare, "data-spare", spare},
	} {
		if err := seedLocalVolume(ctx, env, v.pv, v.node); err != nil {
			return err
		}
		if err := seedBoundClaim(ctx, env, v.claim, v.pv); err != nil {
			return err
		}
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	for _, want := range []struct{ pod, claim, node, why string }{
		{"on-disk", "data-crowded", crowded, "and " + crowded + " is loaded, so every score points elsewhere"},
		{"on-spare-disk", "data-spare", spare, "so a node read from the volume, not a fixed one, is the only answer that works twice"},
	} {
		seeded = append(seeded, want.pod)
		if err := seedClaimingPod(ctx, env, want.pod, want.claim); err != nil {
			return err
		}
		where, err := boundNode(ctx, env, want.pod, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s mounts claim %s, whose volume only %s can reach: %w\nthe program said:\n%s",
				want.pod, want.claim, want.node, err, tail(p.Stdout()))
		}
		if where != want.node {
			return fmt.Errorf("pod %s mounts claim %s, whose volume only %s can reach, and went to %s: a volume's node affinity takes every other node away, %s\nthe program said:\n%s",
				want.pod, want.claim, want.node, where, want.why, tail(p.Stdout()))
		}
	}

	// A claim that resolves to no volume names no node either, whether it is
	// missing or merely still waiting for the binder. Either way there is no
	// node this scheduler may choose, so the pod waits.
	if err := seedPendingClaim(ctx, env, "data-pending"); err != nil {
		return err
	}
	for _, want := range []struct{ pod, claim, why string }{
		{"no-disk", "data-missing", "mounts a claim that does not exist"},
		{"pending-disk", "data-pending", "mounts a claim that is bound to no volume yet"},
	} {
		seeded = append(seeded, want.pod)
		if err := seedClaimingPod(ctx, env, want.pod, want.claim); err != nil {
			return err
		}
		if err := awaitLine(ctx, p, env.Namespace+"/"+want.pod, 30*time.Second); err != nil {
			return fmt.Errorf("pod %s was never reported: %w", want.pod, err)
		}
		if where, err := boundNode(ctx, env, want.pod, 20*time.Second); err == nil {
			return fmt.Errorf("pod %s %s and was placed on %s anyway: a pod whose volume cannot be resolved waits\nthe program said:\n%s",
				want.pod, want.why, where, tail(p.Stdout()))
		}
	}
	return nil
}

// seedPendingClaim creates a claim that stays unbound: an empty storage class
// asks for no provisioner, and no volume names it back.
func seedPendingClaim(ctx context.Context, env *kube.Env, name string) error {
	empty := ""
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &empty,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: apiresource.MustParse("64Mi")},
			},
		},
	}
	if _, err := env.Client.CoreV1().PersistentVolumeClaims(env.Namespace).Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed claim %s: %w", name, err)
	}
	return nil
}

// seedLocalVolume creates a PersistentVolume only one node can reach, which is
// what node affinity on a volume means. The path need not exist: the assertion
// is where the pod is bound, not whether the kubelet could mount it.
func seedLocalVolume(ctx context.Context, env *kube.Env, name, node string) error {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:    corev1.ResourceList{corev1.ResourceStorage: apiresource.MustParse("64Mi")},
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			// An empty class, rather than none at all, keeps the cluster's
			// default provisioner out of a claim that already has its volume.
			StorageClassName:              "",
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/var/local-path-provisioner"},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{node},
					}},
				}},
			}},
		},
	}
	if _, err := env.Client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed volume %s: %w", name, err)
	}
	return nil
}

// seedBoundClaim creates a claim on one named volume and waits for the binder,
// so the pod that follows meets a claim the scheduler can resolve to a node.
func seedBoundClaim(ctx context.Context, env *kube.Env, name, volume string) error {
	empty := ""
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:       volume,
			StorageClassName: &empty,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: apiresource.MustParse("64Mi")},
			},
		},
	}
	if _, err := env.Client.CoreV1().PersistentVolumeClaims(env.Namespace).Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed claim %s: %w", name, err)
	}
	return waitFor(ctx, "claim "+name+" to bind to "+volume, 60*time.Second, func(ctx context.Context) (bool, error) {
		got, err := env.Client.CoreV1().PersistentVolumeClaims(env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return got.Status.Phase == corev1.ClaimBound, nil
	})
}

// seedClaimingPod creates a pod naming this scheduler that mounts one claim.
func seedClaimingPod(ctx context.Context, env *kube.Env, name, claim string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
				},
			}},
			Containers: []corev1.Container{{
				Name:         "app",
				Image:        "registry.k8s.io/pause:3.9",
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// stagePriority checks the pods waiting for room are tried in the order of
// their priority, not the order they arrived in.
//
// Every worker is cordoned before the pods exist, so all of them are waiting
// on the same thing and none has been placed by arriving first. Each pod asks
// for most of a node, so uncordoning one worker frees exactly one seat: who
// takes it is the whole assertion. The low-priority pods are created first, so
// a program that retries in arrival order — or in whatever order its map hands
// back — gives the seat away.
func stagePriority(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 3 {
		return fmt.Errorf("this stage needs three workers, found %d", len(workers))
	}

	// PriorityClasses are cluster scoped, so deleting the namespace does not
	// take them with it and a name reused across runs would meet the leftover.
	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	low, high := "bko-sched-low-"+run, "bko-sched-high-"+run
	defer func() {
		back := context.WithoutCancel(ctx)
		for _, name := range []string{low, high} {
			_ = env.Client.SchedulingV1().PriorityClasses().Delete(back, name, metav1.DeleteOptions{})
		}
	}()
	if err := seedPriorityClass(ctx, env, low, 100); err != nil {
		return err
	}
	if err := seedPriorityClass(ctx, env, high, 1000); err != nil {
		return err
	}

	for _, w := range workers {
		if err := cordonNode(ctx, env, w, true); err != nil {
			return err
		}
		defer cordonNode(context.WithoutCancel(ctx), env, w, false)
	}

	alloc, err := workerAllocatable(ctx, env)
	if err != nil {
		return err
	}
	// More than half a worker each, so a worker that opens has room for one.
	seat := shareOfCPU(alloc[corev1.ResourceCPU], 60)

	// Six against three: a program that picks without looking at priority is
	// unlikely to be lucky three times over.
	zero := int64(0)
	var seeded []string
	defer func() {
		for _, name := range seeded {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	// The unimportant pods are created first, and so have waited longest.
	for _, batch := range []struct {
		prefix string
		class  string
		count  int
	}{{"low", low, 6}, {"high", high, 3}} {
		for i := 1; i <= batch.count; i++ {
			name := fmt.Sprintf("%s-%d", batch.prefix, i)
			if err := seedPriorityPod(ctx, env, name, batch.class, seat); err != nil {
				return err
			}
			seeded = append(seeded, name)
		}
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// Every worker is closed, so the program has nowhere to put anything. No
	// room opens until it has told each pod so, or a pod still being tried
	// would meet an open node and be placed on the way past, rather than by
	// the comparison this stage is about.
	for _, name := range seeded {
		if err := awaitLine(ctx, p, "unscheduled "+env.Namespace+"/"+name, 60*time.Second); err != nil {
			return fmt.Errorf("pod %s was never reported: %w", name, err)
		}
		if err := awaitFailedScheduling(ctx, env, name, 60*time.Second); err != nil {
			return fmt.Errorf("every worker is cordoned, so pod %s can go nowhere, and no FailedScheduling event says so: %w\nthe program said:\n%s", name, err, tail(p.Stdout()))
		}
	}
	placed, err := placements(ctx, env)
	if err != nil {
		return err
	}
	if len(placed) > 0 {
		return fmt.Errorf("every worker is cordoned and %d pod(s) were placed anyway: %v", len(placed), placed)
	}

	// One worker opens at a time, and a worker holds one of these pods. Who
	// takes the seat is the whole assertion, and it is asked three times so
	// that picking without comparing is unlikely to be lucky throughout.
	for i, w := range workers {
		if err := cordonNode(ctx, env, w, false); err != nil {
			return err
		}
		placed, err = settledPlacements(ctx, env, i+1, 60*time.Second)
		if err != nil {
			return fmt.Errorf("worker %s was uncordoned, leaving room for one more pod, and %d of the nine waiting pods are placed: %w\nthe program said:\n%s",
				w, len(placed), err, tail(p.Stdout()))
		}
		for name, node := range placed {
			if !strings.HasPrefix(name, "high-") {
				return fmt.Errorf("pod %s was placed on %s while a pod of class %s (value 1000) was still waiting: the pods waiting for room are compared by priority, not taken in the order they arrived\nthe program said:\n%s",
					name, node, high, tail(p.Stdout()))
			}
		}
		if len(placed) > i+1 {
			return fmt.Errorf("%d workers are open and %d pods are placed: each of these pods asks for %s, three fifths of a worker, so a worker can hold one\nthe program said:\n%s",
				i+1, len(placed), seat, tail(p.Stdout()))
		}
	}
	return nil
}

// settledPlacements waits for want pods to be placed and then for the program
// to stop placing them. An attempt already under way when the room appeared
// lands a moment later, so an answer read the instant the count is reached is
// read too early.
func settledPlacements(ctx context.Context, env *kube.Env, want int, within time.Duration) (map[string]string, error) {
	var placed map[string]string
	err := waitFor(ctx, fmt.Sprintf("%d pod(s) to be placed", want), within, func(ctx context.Context) (bool, error) {
		var err error
		placed, err = placements(ctx, env)
		return err == nil && len(placed) >= want, err
	})
	if err != nil {
		return placed, err
	}
	const quiet = 3 * time.Second
	for deadline := time.Now().Add(quiet); time.Now().Before(deadline); {
		select {
		case <-ctx.Done():
			return placed, ctx.Err()
		case <-time.After(time.Second):
		}
		next, err := placements(ctx, env)
		if err != nil {
			return placed, err
		}
		if len(next) != len(placed) {
			placed, deadline = next, time.Now().Add(quiet)
		}
	}
	return placed, nil
}

// awaitFailedScheduling waits for the program to say, where kubectl describe
// shows it, that this pod has nowhere to go. It is how the stage knows an
// attempt has finished rather than merely started.
func awaitFailedScheduling(ctx context.Context, env *kube.Env, name string, within time.Duration) error {
	return waitFor(ctx, "a FailedScheduling event on pod "+name, within, func(ctx context.Context) (bool, error) {
		events, err := podEvents(ctx, env, name)
		if err != nil {
			return false, err
		}
		return slices.ContainsFunc(events, func(e eventsv1.Event) bool { return e.Reason == "FailedScheduling" }), nil
	})
}

// seedPriorityClass creates a PriorityClass. Admission copies its value into
// the spec.priority of every pod that names it; the pod never carries the
// number itself, and the API server refuses one that tries.
func seedPriorityClass(ctx context.Context, env *kube.Env, name string, value int32) error {
	pc := &schedulingv1.PriorityClass{
		ObjectMeta:  metav1.ObjectMeta{Name: name},
		Value:       value,
		Description: "byok8s course fixture",
	}
	if _, err := env.Client.SchedulingV1().PriorityClasses().Create(ctx, pc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed priority class %s: %w", name, err)
	}
	return nil
}

// seedPriorityPod creates a pod naming this scheduler and one priority class,
// asking for cpu of a worker's eleven.
func seedPriorityPod(ctx context.Context, env *kube.Env, name, class, cpu string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName:     schedulerName,
			PriorityClassName: class,
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "registry.k8s.io/pause:3.9",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: apiresource.MustParse(cpu)},
				},
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// placements is every pod in the stage's namespace that has a node, by name.
func placements(ctx context.Context, env *kube.Env) (map[string]string, error) {
	list, err := env.Client.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	placed := map[string]string{}
	for _, pod := range list.Items {
		if pod.Spec.NodeName != "" {
			placed[pod.Name] = pod.Spec.NodeName
		}
	}
	return placed, nil
}

// stagePreemption checks a pod that fits nowhere takes room from pods that
// matter less, rather than waiting for room nobody is going to free.
//
// Three low-priority pods fill the workers, one each. The high-priority pod
// that follows fits nowhere, and the only way it runs is if one of them stops:
// one, not three, and the pod has to land where the room was made. A fourth
// low-priority pod then asks for the same thing and must wait, because equals
// are not victims.
func stagePreemption(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 3 {
		return fmt.Errorf("this stage needs three workers, found %d", len(workers))
	}

	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	low, high := "bko-sched-low-"+run, "bko-sched-high-"+run
	defer func() {
		back := context.WithoutCancel(ctx)
		for _, name := range []string{low, high} {
			_ = env.Client.SchedulingV1().PriorityClasses().Delete(back, name, metav1.DeleteOptions{})
		}
	}()
	if err := seedPriorityClass(ctx, env, low, 100); err != nil {
		return err
	}
	if err := seedPriorityClass(ctx, env, high, 1000); err != nil {
		return err
	}

	zero := int64(0)
	var seeded []string
	defer func() {
		for _, name := range seeded {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	alloc, err := workerAllocatable(ctx, env)
	if err != nil {
		return err
	}
	// Three of these fill a worker, and what the important pod asks for is
	// more than one of them leaves behind and less than two do: the answer is
	// two victims, whatever size the workers are.
	small := shareOfCPU(alloc[corev1.ResourceCPU], 30)
	large := shareOfCPU(alloc[corev1.ResourceCPU], 55)

	// Three pods on each of three workers, which leaves no worker with the
	// room the important pod wants: two of the three have to go, and only two,
	// for it to have it.
	const perNode = 3
	held := map[string][]string{} // node -> the pods this stage put there
	for i := 1; i <= len(workers)*perNode; i++ {
		name := fmt.Sprintf("low-%d", i)
		if err := seedPriorityPod(ctx, env, name, low, small); err != nil {
			return err
		}
		seeded = append(seeded, name)
		node, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s asks for %s, and a worker holding fewer than %d such pods is free: %w\nthe program said:\n%s", name, small, perNode, err, tail(p.Stdout()))
		}
		held[node] = append(held[node], name)
	}
	for node, pods := range held {
		if len(pods) != perNode {
			return fmt.Errorf("worker %s holds %d of the nine pods rather than %d, so no worker is full in the way this stage needs: %v", node, len(pods), perNode, held)
		}
	}

	seeded = append(seeded, "important")
	if err := seedPriorityPod(ctx, env, "important", high, large); err != nil {
		return err
	}
	where, err := boundNode(ctx, env, "important", 75*time.Second)
	if err != nil {
		return fmt.Errorf("pod important is of class %s (value 1000) and every worker is full of pods of class %s (value 100): the pods in its way matter less than it does, so room is made by evicting them rather than waited for: %w\nthe program said:\n%s",
			high, low, err, tail(p.Stdout()))
	}

	left, err := survivors(ctx, env, "low-")
	if err != nil {
		return err
	}
	// Two of the three on one worker is what six cpus costs: evicting one
	// leaves too little, and evicting three empties a worker for no reason.
	const want = 2
	if gone := len(workers)*perNode - len(left); gone != want {
		return fmt.Errorf("pod important asks for %s and %d pods of %s were evicted for it: two of a worker's three leave it that room, and preemption takes the least it can\nthe program said:\n%s",
			large, gone, small, tail(p.Stdout()))
	}
	for node, pods := range held {
		var stayed int
		for _, name := range pods {
			if slices.Contains(left, name) {
				stayed++
			}
		}
		if node != where && stayed != perNode {
			return fmt.Errorf("%d pod(s) were evicted from %s while pod important was bound to %s: the victims and the pod that evicted them belong on one node, or a second worker is emptied for nothing\nthe program said:\n%s",
				perNode-stayed, node, where, tail(p.Stdout()))
		}
	}

	// Equal priority is not lower priority: this one has nowhere to go and
	// nobody it may take room from.
	seeded = append(seeded, "another")
	if err := seedPriorityPod(ctx, env, "another", low, small); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, "unscheduled "+env.Namespace+"/another", 30*time.Second); err != nil {
		return fmt.Errorf("pod another was never reported: %w", err)
	}
	if err := awaitFailedScheduling(ctx, env, "another", 60*time.Second); err != nil {
		return fmt.Errorf("pod another fits nowhere and may evict nobody, and no FailedScheduling event says so: %w\nthe program said:\n%s", err, tail(p.Stdout()))
	}
	if node, err := boundNode(ctx, env, "another", 15*time.Second); err == nil {
		return fmt.Errorf("pod another is of class %s (value 100), as are the pods already on every worker, and it was placed on %s: a pod may only take room from pods that matter less than it does\nthe program said:\n%s",
			low, node, tail(p.Stdout()))
	}
	if after, err := survivors(ctx, env, "low-"); err != nil {
		return err
	} else if len(after) != len(left) {
		return fmt.Errorf("pod another is of class %s, as are the pods on every worker, and %d of them were evicted for it: equals are never victims, or two replicas of one thing would take turns evicting each other\nthe program said:\n%s",
			low, len(left)-len(after), tail(p.Stdout()))
	}
	return nil
}

// survivors is the pods whose name starts with prefix that are still here and
// not on their way out: a pod being deleted keeps answering a Get until the
// kubelet is done with it.
func survivors(ctx context.Context, env *kube.Env, prefix string) ([]string, error) {
	list, err := env.Client.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	var names []string
	for _, pod := range list.Items {
		if strings.HasPrefix(pod.Name, prefix) && pod.DeletionTimestamp == nil {
			names = append(names, pod.Name)
		}
	}
	return names, nil
}

// stageFrameworkPlugins checks each filter is a named thing that reports what
// it turned down, and that a pod nobody could place says so in those names.
//
// Nothing here reads the learner's source. What makes the structure visible is
// the FailedScheduling note: counting nodes per reason is only possible once
// every check has a name and the nodes it rejected are counted against it.
func stageFrameworkPlugins(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 3 {
		return fmt.Errorf("this stage needs three workers, found %d", len(workers))
	}
	// The control plane is a node too, and it turns pods down like any other.
	all, err := env.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	total := len(all.Items)
	// Every node that is not a worker is tainted against ordinary pods, and is
	// turned down by that plugin before any other looks at it.
	tainted := fmt.Sprintf("%d TaintToleration", total-len(workers))

	zero := int64(0)
	var seeded []string
	defer func() {
		for _, name := range seeded {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// One plugin turning down every node: the simplest note there is.
	seeded = append(seeded, "unlabelled")
	if err := seedSelectingPod(ctx, env, "unlabelled", map[string]string{"byok8s.dev/stage-plugins": "yes"}); err != nil {
		return err
	}
	if err := awaitNote(ctx, env, p, "unlabelled",
		[]string{fmt.Sprintf("0/%d nodes are available", total), fmt.Sprintf("%d NodeAffinity", len(workers)), tainted},
		"selects a label no node carries"); err != nil {
		return err
	}

	// Asking for more than any node has is a different plugin, and it must be
	// named as that one rather than as whatever refused the last pod.
	seeded = append(seeded, "enormous")
	if err := seedRequestingPod(ctx, env, "enormous", corev1.ResourceCPU, "500"); err != nil {
		return err
	}
	if err := awaitNote(ctx, env, p, "enormous",
		[]string{fmt.Sprintf("0/%d nodes are available", total), fmt.Sprintf("%d NodeResourcesFit", len(workers)), tainted},
		"asks for 500 cpus"); err != nil {
		return err
	}

	// Two plugins at once, each counting only the nodes it turned down. The
	// control plane is tainted, the cordoned worker is closed, and the two
	// workers left have no room.
	alloc, err := workerAllocatable(ctx, env)
	if err != nil {
		return err
	}
	closed := workers[0]
	if err := cordonNode(ctx, env, closed, true); err != nil {
		return err
	}
	defer cordonNode(context.WithoutCancel(ctx), env, closed, false)
	for _, w := range workers[1:] {
		name := "ballast-" + w
		seeded = append(seeded, name)
		node, err := env.Client.CoreV1().Nodes().Get(ctx, w, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get node %s: %w", w, err)
		}
		cpu := node.Status.Allocatable[corev1.ResourceCPU]
		if err := seedBallast(ctx, env, name, w, milliCPU(cpu.MilliValue()*95/100)); err != nil {
			return err
		}
	}
	seeded = append(seeded, "mixed")
	// A fifth of a worker: more than the twentieth the ballast leaves behind.
	mixed := shareOfCPU(alloc[corev1.ResourceCPU], 20)
	if err := seedRequestingPod(ctx, env, "mixed", corev1.ResourceCPU, mixed); err != nil {
		return err
	}
	if err := awaitNote(ctx, env, p, "mixed",
		[]string{
			fmt.Sprintf("0/%d nodes are available", total),
			"1 NodeUnschedulable",
			fmt.Sprintf("%d NodeResourcesFit", len(workers)-1),
			tainted,
		},
		"asks for "+mixed+" with one worker cordoned and the rest full"); err != nil {
		return err
	}
	return nil
}

// awaitNote waits for a FailedScheduling event on the pod whose note contains
// every one of want. The whole note is quoted on failure, since the answer is
// usually one count away from right.
func awaitNote(ctx context.Context, env *kube.Env, p *runner.Process, name string, want []string, why string) error {
	var note string
	err := waitFor(ctx, "a FailedScheduling note about pod "+name, 60*time.Second, func(ctx context.Context) (bool, error) {
		events, err := podEvents(ctx, env, name)
		if err != nil {
			return false, err
		}
		for _, e := range events {
			if e.Reason != "FailedScheduling" {
				continue
			}
			note = e.Note
			if !slices.ContainsFunc(want, func(s string) bool { return !strings.Contains(note, s) }) {
				return true, nil
			}
		}
		return false, nil
	})
	if err == nil {
		return nil
	}
	if note == "" {
		return fmt.Errorf("pod %s %s and no FailedScheduling event says why: %w\nthe program said:\n%s", name, why, err, tail(p.Stdout()))
	}
	return fmt.Errorf("pod %s %s, and its FailedScheduling note says %q, which does not carry every part of %q: a node is counted against the first plugin to turn it down, by that plugin's name\nthe program said:\n%s",
		name, why, note, strings.Join(want, ", "), tail(p.Stdout()))
}

// stageMultiProfile checks one program serves two scheduler names, and weighs
// the same nodes differently under each.
//
// A worker is loaded to roughly half its cpu. A pod naming the ordinary
// profile wants the room and must go elsewhere; a pod naming the packing
// profile wants the node already in use, so that the empty ones stay empty.
// Same cluster, same moment, same filters: only the weights differ.
func stageMultiProfile(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 2 {
		return fmt.Errorf("this stage needs at least two workers, found %d", len(workers))
	}
	loaded := workers[0]

	node, err := env.Client.CoreV1().Nodes().Get(ctx, loaded, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get node %s: %w", loaded, err)
	}
	cpu := node.Status.Allocatable[corev1.ResourceCPU]

	zero := int64(0)
	seeded := []string{"ballast-loaded", "spread-me", "pack-me", "not-ours"}
	defer func() {
		for _, name := range seeded {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	if err := seedBallast(ctx, env, "ballast-loaded", loaded, milliCPU(cpu.MilliValue()*50/100)); err != nil {
		return err
	}

	p, cleanup, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer cleanup()

	// A tenth of a worker, which the half-loaded node has room for.
	ask := shareOfCPU(cpu, 10)
	if err := seedProfilePod(ctx, env, "spread-me", schedulerName, ask); err != nil {
		return err
	}
	where, err := boundNode(ctx, env, "spread-me", 60*time.Second)
	if err != nil {
		return fmt.Errorf("pod spread-me names %s and was never bound: %w\nthe program said:\n%s", schedulerName, err, tail(p.Stdout()))
	}
	if where == loaded {
		return fmt.Errorf("pod spread-me names %s, whose scores prefer the node with the most room, and went to %s, which is half spoken for while other workers are empty\nthe program said:\n%s",
			schedulerName, loaded, tail(p.Stdout()))
	}

	if err := seedProfilePod(ctx, env, "pack-me", packingProfile, ask); err != nil {
		return err
	}
	where, err = boundNode(ctx, env, "pack-me", 60*time.Second)
	if err != nil {
		return fmt.Errorf("pod pack-me names %s, which this program serves as well as %s, and was never bound: a second profile is a second name on the same queue, not a second program: %w\nthe program said:\n%s",
			packingProfile, schedulerName, err, tail(p.Stdout()))
	}
	if where != loaded {
		return fmt.Errorf("pod pack-me names %s, whose scores prefer the fullest node that still fits, and went to %s rather than %s, which is half full and has room for it: a profile is the same filters with the scores weighed differently\nthe program said:\n%s",
			packingProfile, where, loaded, tail(p.Stdout()))
	}

	// The field selector that used to keep other schedulers' pods out of this
	// program is gone, since it cannot ask for either of two names. What takes
	// its place has to be just as strict.
	if err := seedProfilePod(ctx, env, "not-ours", schedulerName+"-nope", ask); err != nil {
		return err
	}
	if node, err := boundNode(ctx, env, "not-ours", 20*time.Second); err == nil {
		return fmt.Errorf("pod not-ours names a scheduler this program does not serve and was bound to %s anyway: a pod belongs to the program whose profile it names, and no other\nthe program said:\n%s",
			node, tail(p.Stdout()))
	}
	if strings.Contains(p.Stdout(), env.Namespace+"/not-ours") {
		return fmt.Errorf("pod not-ours names a scheduler this program does not serve and was reported anyway: watching every unbound pod means deciding for yourself which are yours\nthe program said:\n%s",
			tail(p.Stdout()))
	}
	return nil
}

// seedProfilePod creates a pod naming one scheduler profile and asking for cpu.
func seedProfilePod(ctx context.Context, env *kube.Env, name, profile, cpu string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
		Spec: corev1.PodSpec{
			SchedulerName: profile,
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "registry.k8s.io/pause:3.9",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: apiresource.MustParse(cpu)},
				},
			}},
		},
	}
	if _, err := env.Client.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed pod %s: %w", name, err)
	}
	return nil
}

// stagePercentageOfNodes checks the program will settle for a good enough node
// when told to, and that the part of the cluster it looks at moves.
//
// Two workers are loaded and one is left empty, so scoring every node has one
// answer and gives it every time. Asked to look at a third of the cluster, the
// program has to take what it finds — and because the scan resumes where the
// last one stopped, six pods reach all three workers rather than one.
//
// The pod nothing can take is the other half: a sample is enough to place a
// pod, never enough to declare there is nowhere to put it.
func stagePercentageOfNodes(ctx context.Context, env *kube.Env, bin string) error {
	workers, err := workerNodes(ctx, env)
	if err != nil {
		return err
	}
	if len(workers) < 3 {
		return fmt.Errorf("this stage needs three workers, found %d", len(workers))
	}
	all, err := env.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	total := len(all.Items)

	zero := int64(0)
	var seeded []string
	defer func() {
		for _, name := range seeded {
			_ = env.Client.CoreV1().Pods(env.Namespace).Delete(context.WithoutCancel(ctx), name,
				metav1.DeleteOptions{GracePeriodSeconds: &zero})
		}
	}()
	for _, w := range workers[1:] {
		name := "ballast-" + w
		seeded = append(seeded, name)
		node, err := env.Client.CoreV1().Nodes().Get(ctx, w, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get node %s: %w", w, err)
		}
		cpu := node.Status.Allocatable[corev1.ResourceCPU]
		if err := seedBallast(ctx, env, name, w, milliCPU(cpu.MilliValue()*50/100)); err != nil {
			return err
		}
	}

	// A third of four nodes is one: the first feasible node each scan meets.
	p, cleanup, err := launch(ctx, env, bin, "-percentage-of-nodes=34")
	if err != nil {
		return err
	}
	defer cleanup()

	used := map[string]int{}
	for i := 1; i <= 2*len(workers); i++ {
		name := fmt.Sprintf("sampled-%d", i)
		seeded = append(seeded, name)
		if err := seedPod(ctx, env, name, schedulerName); err != nil {
			return err
		}
		node, err := boundNode(ctx, env, name, 60*time.Second)
		if err != nil {
			return fmt.Errorf("pod %s asks for nothing and every worker can take it: %w\nthe program said:\n%s", name, err, tail(p.Stdout()))
		}
		if !slices.Contains(workers, node) {
			return fmt.Errorf("pod %s was bound to %s, which is not a worker: a node the filters turn down is skipped over, not counted towards the sample\nthe program said:\n%s",
				name, node, tail(p.Stdout()))
		}
		used[node]++
	}
	if len(used) != len(workers) {
		return fmt.Errorf("%d pods that ask for nothing reached %d of the %d workers (%v), with -percentage-of-nodes=34: one node is scored per pod, and the scan resumes where the last one stopped, or every pod in the cluster is weighed against the same few nodes\nthe program said:\n%s",
			2*len(workers), len(used), len(workers), used, tail(p.Stdout()))
	}

	// Stopping early is for finding somewhere good enough. Finding nowhere has
	// to mean nowhere at all.
	seeded = append(seeded, "nowhere")
	if err := seedSelectingPod(ctx, env, "nowhere", map[string]string{"byok8s.dev/stage-sampled": "yes"}); err != nil {
		return err
	}
	if err := awaitNote(ctx, env, p, "nowhere",
		[]string{fmt.Sprintf("0/%d nodes are available", total), fmt.Sprintf("%d NodeAffinity", len(workers))},
		"selects a label no node carries"); err != nil {
		return err
	}
	return nil
}
