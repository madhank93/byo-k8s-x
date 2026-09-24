// Your scheduler.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// The name pods use to ask for this scheduler. The default scheduler ignores
// any pod that names another, so these are this program's alone to place.
const schedulerName = "byok8s"

// The second profile this program serves. One binary, two schedulerNames: the
// same filters and the same queue, with the scores weighed differently.
const packingProfile = schedulerName + "-packing"

// profiles is what a scheduler's configuration comes down to. The filters are
// the rules and do not vary; the scores are opinions, and a profile is how
// much each one counts for.
var profiles = map[string][]scorePlugin{
	schedulerName: scores,
	// Packing wants the fullest node that still fits, so that the empty ones
	// stay empty and can be given back. It is the same arithmetic as
	// LeastAllocated, read the other way round, and nothing else gets a say:
	// spreading replicas would pull straight against it.
	packingProfile: {
		{"NodeResourcesMostAllocated", 1, func(pod *corev1.Pod, n *corev1.Node, s snapshot) int64 {
			return 100 - leastAllocated(pod, n, s.placed)
		}},
	},
}

// The taint the node controller adds to a node whose spec.unschedulable is
// set. A pod tolerating it is saying a closed node suits it anyway.
const unschedulableTaint = "node.kubernetes.io/unschedulable"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// leastAllocated scores a node out of 100 by how much of it is still free once
// this pod is on it, averaged over cpu and memory. It is the shape of the
// default scheduler's LeastAllocated strategy.
//
// The arithmetic is on requests, not usage: a node holding an idle pod that
// reserved eight CPUs is eight CPUs full, because that is the promise the
// cluster made on its behalf.
func leastAllocated(pod *corev1.Pod, node *corev1.Node, placed []*corev1.Pod) int64 {
	want := requests(pod)
	used := usedOn(node, placed)

	var total, counted int64
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		alloc := node.Status.Allocatable[name]
		if alloc.IsZero() {
			continue
		}
		held := used[name]
		asked := want[name]
		free := alloc.MilliValue() - held.MilliValue() - asked.MilliValue()
		if free < 0 {
			free = 0
		}
		total += free * 100 / alloc.MilliValue()
		counted++
	}
	if counted == 0 {
		return 0
	}
	return total / counted
}

// usedOn is what the pods already on a node have asked of it.
func usedOn(node *corev1.Node, placed []*corev1.Pod) corev1.ResourceList {
	used := corev1.ResourceList{}
	for _, p := range placed {
		if occupies(p) != node.Name {
			continue
		}
		for name, q := range requests(p) {
			held := used[name]
			held.Add(q)
			used[name] = held
		}
	}
	return used
}

// balancedAllocation scores a node out of 100 by how alike its cpu and memory
// would be used once this pod is on it. A node 70% through its cpu and barely
// into its memory scores 30; a node evenly 40% through both scores 100.
//
// It is the counterweight to leastAllocated, which sees only how much room is
// left and will happily put a cpu-hungry pod on the node whose cpu is nearly
// gone, because that node's untouched memory carries the average.
func balancedAllocation(pod *corev1.Pod, node *corev1.Node, placed []*corev1.Pod) int64 {
	want := requests(pod)
	used := usedOn(node, placed)

	var fractions []int64
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		alloc := node.Status.Allocatable[name]
		if alloc.IsZero() {
			// A node that does not say how much of something it has cannot be
			// called balanced in it.
			return 0
		}
		held := used[name]
		asked := want[name]
		fractions = append(fractions, (held.MilliValue()+asked.MilliValue())*100/alloc.MilliValue())
	}
	spread := fractions[0] - fractions[1]
	if spread < 0 {
		spread = -spread
	}
	if spread > 100 {
		spread = 100
	}
	return 100 - spread
}

// An image smaller than this is not worth preferring a node for: the pull is
// quicker than the imbalance the preference would cause. An image larger than
// the upper bound cannot earn more than the full score. Both are the
// thresholds the default scheduler uses.
const (
	minImageBytes = 23 * 1024 * 1024
	maxImageBytes = 1000 * 1024 * 1024
)

// imageLocality scores a node out of 100 by how much of what this pod has to
// download is already on it. A pod starts sooner where its image is, and a
// large image is the difference between seconds and minutes.
//
// The score is the size already there, not the number of images: ten small
// images are not worth what one large one is.
func imageLocality(pod *corev1.Pod, node *corev1.Node) int64 {
	have := map[string]int64{}
	for _, img := range node.Status.Images {
		for _, name := range img.Names {
			have[name] = img.SizeBytes
		}
	}
	var bytes int64
	for _, c := range pod.Spec.Containers {
		bytes += have[c.Image]
	}
	switch {
	case bytes <= minImageBytes:
		return 0
	case bytes >= maxImageBytes:
		return 100
	}
	return (bytes - minImageBytes) * 100 / (maxImageBytes - minImageBytes)
}

// topologySatisfied reports whether placing this pod on a node keeps every
// required topology spread constraint. Unlike the scores, this is a filter: a
// constraint whose whenUnsatisfiable is DoNotSchedule removes the node, and a
// pod with nowhere left waits.
//
// The skew a placement would cause is counted in domains, not nodes: how many
// matching pods the candidate's domain would then hold, against the fewest any
// domain holds now.
func topologySatisfied(pod *corev1.Pod, node *corev1.Node, all []*corev1.Node, placed []*corev1.Pod) bool {
	for _, c := range pod.Spec.TopologySpreadConstraints {
		// ScheduleAnyway is a preference. It belongs in the scores, and a node
		// that fails it is still a node the pod can go to.
		if c.WhenUnsatisfiable != corev1.DoNotSchedule {
			continue
		}
		// A node with no value for the key is in no domain at all, so it
		// cannot be part of a spread across them.
		domain, ok := node.Labels[c.TopologyKey]
		if !ok {
			return false
		}

		selector := labels.Nothing()
		if c.LabelSelector != nil {
			s, err := metav1.LabelSelectorAsSelector(c.LabelSelector)
			if err != nil {
				continue
			}
			selector = s
		}

		// Every domain the cluster has, including the ones holding nothing:
		// an empty domain is the whole reason a placement can be refused.
		counts := map[string]int{}
		domainOf := map[string]string{}
		for _, n := range all {
			if d, ok := n.Labels[c.TopologyKey]; ok {
				counts[d] += 0
				domainOf[n.Name] = d
			}
		}
		for _, p := range placed {
			if p.UID == pod.UID || p.Namespace != pod.Namespace {
				continue
			}
			d, ok := domainOf[p.Spec.NodeName]
			if !ok || !selector.Matches(labels.Set(p.Labels)) {
				continue
			}
			counts[d]++
		}

		fewest := -1
		for _, n := range counts {
			if fewest < 0 || n < fewest {
				fewest = n
			}
		}
		if fewest < 0 {
			return false
		}
		if int32(counts[domain]+1-fewest) > c.MaxSkew {
			return false
		}
	}
	return true
}

// podAffinitySatisfied reports whether a node satisfies the pod's required
// inter-pod affinity and anti-affinity. Node affinity reads the node's own
// labels; these terms are about the company a pod keeps: affinity wants
// matching pods already in the same topology domain, anti-affinity wants none.
func podAffinitySatisfied(pod *corev1.Pod, node *corev1.Node, all []*corev1.Node, placed []*corev1.Pod) bool {
	affinity := pod.Spec.Affinity
	if affinity == nil {
		return true
	}
	if affinity.PodAffinity != nil {
		for _, term := range affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if matchingPodsInDomain(pod, node, all, placed, term) == 0 {
				return false
			}
		}
	}
	if affinity.PodAntiAffinity != nil {
		for _, term := range affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if matchingPodsInDomain(pod, node, all, placed, term) > 0 {
				return false
			}
		}
	}
	return true
}

// matchingPodsInDomain counts the pods a term selects that are already in the
// same topology domain as this node — every node sharing its value for the
// term's key, which for kubernetes.io/hostname is the node alone.
//
// A node with no value for the key is in no domain, so it keeps company with
// nobody: affinity there has no one to join, and anti-affinity no one to avoid.
func matchingPodsInDomain(pod *corev1.Pod, node *corev1.Node, all []*corev1.Node, placed []*corev1.Pod, term corev1.PodAffinityTerm) int {
	domain, ok := node.Labels[term.TopologyKey]
	if !ok {
		return 0
	}
	// A nil selector matches nothing, which is what labels.Nothing() gives
	// back here; an empty one matches every pod.
	selector, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
	if err != nil {
		return 0
	}
	// A term names the namespaces it looks in, and saying nothing means the
	// pod's own — not every namespace in the cluster.
	namespaces := term.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{pod.Namespace}
	}

	inDomain := map[string]bool{}
	for _, n := range all {
		if d, ok := n.Labels[term.TopologyKey]; ok && d == domain {
			inDomain[n.Name] = true
		}
	}
	var count int
	for _, p := range placed {
		if p.UID == pod.UID || !inDomain[p.Spec.NodeName] || !slices.Contains(namespaces, p.Namespace) {
			continue
		}
		if selector.Matches(labels.Set(p.Labels)) {
			count++
		}
	}
	return count
}

// volumesReachable reports whether this node can reach every volume the pod
// mounts. A local disk lives on one machine, and the PersistentVolume records
// that as node affinity: a pod bound to it has one node, however full, and the
// emptier ones are not candidates at all.
//
// A claim that is missing, or bound to no volume yet, leaves the pod waiting.
// Binding it is the volume binder's job, not this scheduler's, and a pod placed
// before the claim is resolved can land where the volume never reaches.
func volumesReachable(pod *corev1.Pod, node *corev1.Node, claims corelisters.PersistentVolumeClaimLister, pvs corelisters.PersistentVolumeLister) bool {
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		claim, err := claims.PersistentVolumeClaims(pod.Namespace).Get(v.PersistentVolumeClaim.ClaimName)
		if err != nil || claim.Spec.VolumeName == "" {
			return false
		}
		pv, err := pvs.Get(claim.Spec.VolumeName)
		if err != nil {
			return false
		}
		// A volume every node can reach — a network disk — constrains nothing.
		if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
			continue
		}
		// The terms are ORed, as they are in a pod's node affinity.
		if !slices.ContainsFunc(pv.Spec.NodeAffinity.Required.NodeSelectorTerms, func(t corev1.NodeSelectorTerm) bool {
			return termMatches(t, node.Labels)
		}) {
			return false
		}
	}
	return true
}

// beforeInQueue reports whether a should be tried before b: the more important
// pod first, and the one that has waited longer when they are equally
// important, so a pod of middling priority is not starved by a stream of its
// equals arriving behind it.
func beforeInQueue(a, b *corev1.Pod) bool {
	if pa, pb := priorityOf(a), priorityOf(b); pa != pb {
		return pa > pb
	}
	return a.CreationTimestamp.Time.Before(b.CreationTimestamp.Time)
}

// others is every pod but this one. A pod nominated for a node is counted as
// being on it, and a pod that counted itself would find its own request in the
// way of itself.
func others(placed []*corev1.Pod, pod *corev1.Pod) []*corev1.Pod {
	return slices.DeleteFunc(slices.Clone(placed), func(p *corev1.Pod) bool { return p.UID == pod.UID })
}

// evictionUnderway reports whether the room this pod was promised is still
// being cleared. A pod is deleted long before it is gone, and every attempt in
// between would find the node just as full as the first one did.
func evictionUnderway(pod *corev1.Pod, placed []*corev1.Pod) bool {
	if pod.Status.NominatedNodeName == "" {
		return false
	}
	return slices.ContainsFunc(placed, func(p *corev1.Pod) bool {
		return p.Spec.NodeName == pod.Status.NominatedNodeName && p.DeletionTimestamp != nil
	})
}

// snapshot is the cluster as one scheduling cycle sees it: the plugins all read
// the same nodes and pods, so two of them cannot disagree about what is there.
type snapshot struct {
	nodes      []*corev1.Node
	placed     []*corev1.Pod
	candidates []*corev1.Node // the nodes still in the running, filled before scoring
	claims     corelisters.PersistentVolumeClaimLister
	pvs        corelisters.PersistentVolumeLister
}

// filterPlugin is one reason a node is not for this pod, under the name the
// pod's event will report it by.
type filterPlugin struct {
	name  string
	allow func(pod *corev1.Pod, node *corev1.Node, s snapshot) bool
}

// scorePlugin is one opinion about how good a node is, out of 100, and how
// much that opinion counts for.
type scorePlugin struct {
	name   string
	weight int64
	score  func(pod *corev1.Pod, node *corev1.Node, s snapshot) int64
}

// filters run in this order, and the first to reject a node is the reason that
// node is reported against — so the cheap, absolute checks come first and the
// ones that have to look at other pods come last. NodeUnschedulable before
// TaintToleration, as in the real scheduler: a cordoned node is reported as
// closed rather than as tainted, though the cordon puts a taint there too.
var filters = []filterPlugin{
	{"NodeReady", func(_ *corev1.Pod, n *corev1.Node, _ snapshot) bool { return isReady(n) }},
	{"NodeUnschedulable", func(pod *corev1.Pod, n *corev1.Node, _ snapshot) bool { return schedulable(pod, n) }},
	{"TaintToleration", func(pod *corev1.Pod, n *corev1.Node, _ snapshot) bool { return tolerates(pod, n) }},
	{"NodeAffinity", func(pod *corev1.Pod, n *corev1.Node, _ snapshot) bool {
		// A pod's node selector names labels a node must carry, every one of
		// them; an empty selector matches every node.
		return labels.SelectorFromSet(pod.Spec.NodeSelector).Matches(labels.Set(n.Labels)) && affinityMatches(pod, n)
	}},
	{"VolumeBinding", func(pod *corev1.Pod, n *corev1.Node, s snapshot) bool {
		return volumesReachable(pod, n, s.claims, s.pvs)
	}},
	{"PodTopologySpread", func(pod *corev1.Pod, n *corev1.Node, s snapshot) bool {
		return topologySatisfied(pod, n, s.nodes, s.placed)
	}},
	{"InterPodAffinity", func(pod *corev1.Pod, n *corev1.Node, s snapshot) bool {
		return podAffinitySatisfied(pod, n, s.nodes, s.placed)
	}},
	{"NodePorts", func(pod *corev1.Pod, n *corev1.Node, s snapshot) bool { return portsFree(pod, n, s.placed) }},
	{"NodeResourcesFit", func(pod *corev1.Pod, n *corev1.Node, s snapshot) bool { return roomFor(pod, n, s.placed) }},
}

// scores are summed with their weights, which is all a scheduling profile is:
// the same opinions, listened to in different proportions.
var scores = []scorePlugin{
	{"NodeResourcesLeastAllocated", 1, func(pod *corev1.Pod, n *corev1.Node, s snapshot) int64 {
		return leastAllocated(pod, n, s.placed)
	}},
	{"NodeResourcesBalancedAllocation", 1, func(pod *corev1.Pod, n *corev1.Node, s snapshot) int64 {
		return balancedAllocation(pod, n, s.placed)
	}},
	{"ImageLocality", 1, func(pod *corev1.Pod, n *corev1.Node, _ snapshot) int64 { return imageLocality(pod, n) }},
	{"SelectorSpread", 1, func(pod *corev1.Pod, n *corev1.Node, s snapshot) int64 { return spreadScore(pod, n, s) }},
}

// runFilters returns the name of the first plugin to refuse this node, or an
// empty string when every one of them allows it.
func runFilters(pod *corev1.Pod, node *corev1.Node, s snapshot) string {
	for _, f := range filters {
		if !f.allow(pod, node, s) {
			return f.name
		}
	}
	return ""
}

// feasible reports whether a node suits the pod once room is set aside: every
// filter but the two about space. Preemption asks the same question, since a
// node this pod may not run on is not made suitable by evicting anyone.
func feasible(pod *corev1.Pod, node *corev1.Node, s snapshot) bool {
	for _, f := range filters {
		switch f.name {
		case "NodePorts", "NodeResourcesFit":
			continue
		}
		if !f.allow(pod, node, s) {
			return false
		}
	}
	return true
}

// unavailable is what a pod that went nowhere is owed: how many nodes were
// looked at, and which plugin turned each of them down. It is the message the
// real scheduler puts on a FailedScheduling event, and the first thing anyone
// debugging a pending pod reads.
func unavailable(total int, rejected map[string]int) string {
	names := slices.Sorted(maps.Keys(rejected))
	slices.SortStableFunc(names, func(a, b string) int { return cmp.Compare(rejected[b], rejected[a]) })
	reasons := make([]string, 0, len(names))
	for _, name := range names {
		reasons = append(reasons, fmt.Sprintf("%d %s", rejected[name], name))
	}
	if len(reasons) == 0 {
		return fmt.Sprintf("0/%d nodes are available", total)
	}
	return fmt.Sprintf("0/%d nodes are available: %s", total, strings.Join(reasons, ", "))
}

// spreadScore is how well a node keeps this pod away from its own replicas.
//
// It is the one score that cannot be worked out from a node alone: how bad
// three siblings on a node is depends on whether the other candidates have
// none or have three of their own. So the worst candidate sets the scale, and
// with a handful of nodes recomputing that per node is cheaper than threading
// a pre-pass through the framework.
func spreadScore(pod *corev1.Pod, node *corev1.Node, s snapshot) int64 {
	var worst int64
	for _, n := range s.candidates {
		if c := siblingsOn(pod, n, s.placed); c > worst {
			worst = c
		}
	}
	if worst == 0 {
		return 100
	}
	return (worst - siblingsOn(pod, node, s.placed)) * 100 / worst
}

// preempt finds room for a pod that fits nowhere by taking it from pods that
// matter less, and returns the node and the pods that would have to go.
//
// The least important pods go first, and the node needing the fewest of them
// is the one to disturb: preemption is meant to be the smallest harm that
// answers the request. A pod of equal priority is never a victim — otherwise
// two pods of one deployment would take turns evicting each other forever.
func preempt(pod *corev1.Pod, s snapshot) (string, []*corev1.Pod) {
	var bestNode string
	var best []*corev1.Pod
	for _, n := range s.nodes {
		if !feasible(pod, n, s) {
			continue
		}
		var removable []*corev1.Pod
		for _, p := range s.placed {
			if p.Spec.NodeName == n.Name && priorityOf(p) < priorityOf(pod) {
				removable = append(removable, p)
			}
		}
		slices.SortFunc(removable, func(a, b *corev1.Pod) int { return cmp.Compare(priorityOf(a), priorityOf(b)) })

		remaining := s.placed
		var victims []*corev1.Pod
		for _, v := range removable {
			if fits(pod, n, remaining) {
				break
			}
			victims = append(victims, v)
			remaining = slices.DeleteFunc(slices.Clone(remaining), func(p *corev1.Pod) bool { return p.UID == v.UID })
		}
		if !fits(pod, n, remaining) {
			continue
		}
		if bestNode == "" || len(victims) < len(best) {
			bestNode, best = n.Name, victims
		}
	}
	return bestNode, best
}

// occupies is the node a pod's requests are counted against: the one it is
// bound to, or the one preemption has nominated it for. Room taken from other
// pods is already spoken for, and a second pod that treated it as free would
// evict someone for nothing.
func occupies(pod *corev1.Pod) string {
	if pod.Spec.NodeName != "" {
		return pod.Spec.NodeName
	}
	return pod.Status.NominatedNodeName
}

// priorityOf is how important a pod is, as an integer. A pod names a
// PriorityClass and admission writes its value into spec.priority, so the
// scheduler compares numbers and never reads the class. A pod that named no
// class is a zero.
func priorityOf(pod *corev1.Pod) int32 {
	if pod.Spec.Priority == nil {
		return 0
	}
	return *pod.Spec.Priority
}

// controllerOf is the uid of whatever manages this pod — a ReplicaSet, a Job,
// a StatefulSet — or empty for a pod created on its own. Pods sharing it are
// replicas of one thing, and are worth keeping apart: they fail together when
// their node does.
func controllerOf(pod *corev1.Pod) string {
	for _, ref := range pod.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			return string(ref.UID)
		}
	}
	return ""
}

// siblingsOn counts the pods on a node managed by the same controller as this
// pod, itself excluded. A pod with no controller has no siblings.
func siblingsOn(pod *corev1.Pod, node *corev1.Node, placed []*corev1.Pod) int64 {
	owner := controllerOf(pod)
	if owner == "" {
		return 0
	}
	var n int64
	for _, p := range placed {
		if p.Spec.NodeName == node.Name && p.UID != pod.UID && controllerOf(p) == owner {
			n++
		}
	}
	return n
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Choosing a node is a later stage; until then the node is given.
	node := flag.String("node", "", "bind every waiting pod to this node")
	percent := flag.Int("percentage-of-nodes", 100, "how much of the cluster to look at before binding, as a percentage")
	flag.Parse()

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("build clientset: %w", err)
	}

	// The API server narrows the stream to pods not yet on a node. It cannot
	// narrow it to either of two scheduler names — a field selector compares
	// one field to one value — so the profile a pod named is read here, and a
	// pod naming a scheduler this program does not serve is left alone.
	unbound := fields.OneTermEqualSelector("spec.nodeName", "").String()
	factory := informers.NewSharedInformerFactoryWithOptions(cs, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.FieldSelector = unbound }))
	pods := factory.Core().V1().Pods().Informer()

	// Nodes come from a factory of their own: the pod factory's field selector
	// is applied to every informer it makes, and a node list sent with it is
	// refused.
	nodeFactory := informers.NewSharedInformerFactory(cs, 0)
	nodes := nodeFactory.Core().V1().Nodes()
	nodeLister := nodes.Lister()
	nodeInformer := nodes.Informer()

	// Every pod, whoever placed it: what a node already carries decides
	// whether another pod fits. This factory has no field selector.
	allPods := nodeFactory.Core().V1().Pods()
	podLister := allPods.Lister()
	allPodsInformer := allPods.Informer()

	// The volumes a pod mounts, and where they can be reached from: a claim
	// names a volume, and the volume names the nodes that can see it.
	claims := nodeFactory.Core().V1().PersistentVolumeClaims()
	claimLister := claims.Lister()
	claimInformer := claims.Informer()
	pvs := nodeFactory.Core().V1().PersistentVolumes()
	pvLister := pvs.Lister()
	pvInformer := pvs.Informer()

	// Nodes are taken in turn, so pods spread without one node yet being
	// weighed against another. The add handler runs on one goroutine, so the
	// counter needs no lock.
	var turn int
	// Where the next scan starts. The add handler runs on one goroutine, so
	// this needs no lock, exactly as turn does not.
	var start int
	// cycle is one pass of the framework over one pod: the same snapshot
	// through the filters and then the scores. It returns the node, or an
	// empty name and the reason every node was turned down.
	cycle := func(pod *corev1.Pod) (string, snapshot, string) {
		all, err := nodeLister.List(labels.Everything())
		if err != nil {
			return "", snapshot{}, "the node cache could not be read"
		}
		placed, err := podLister.List(labels.Everything())
		if err != nil {
			return "", snapshot{}, "the pod cache could not be read"
		}
		// The rotation below only means anything over a stable order, and a
		// lister hands its objects back in whatever order it has them in.
		slices.SortFunc(all, func(a, b *corev1.Node) int { return cmp.Compare(a.Name, b.Name) })
		snap := snapshot{nodes: all, placed: others(placed, pod), claims: claimLister, pvs: pvLister}

		// Enough feasible nodes, not every feasible node. Looking at all of
		// them is the best answer and the slowest one, and past a few thousand
		// nodes the pod waiting is a worse outcome than the second-best node.
		want := len(all) * *percent / 100
		if want < 1 {
			want = 1
		}
		rejected := map[string]int{}
		examined := 0
		for i := 0; i < len(all) && len(snap.candidates) < want; i++ {
			n := all[(start+i)%len(all)]
			examined++
			if why := runFilters(pod, n, snap); why != "" {
				rejected[why]++
				continue
			}
			snap.candidates = append(snap.candidates, n)
		}
		// The next cycle carries on from here. Starting over each time would
		// score the same handful of nodes for every pod in the cluster and
		// leave the rest of it empty — the sample has to move.
		start = (start + examined) % len(all)
		if len(snap.candidates) == 0 {
			return "", snap, unavailable(len(all), rejected)
		}

		// Fitting is a yes or no; it does not say which yes is better. Each
		// score plugin answers out of 100 and the weighted sum decides, which
		// is all a scheduler does with its opinions.
		var best []string
		bestScore := int64(-1)
		for _, n := range snap.candidates {
			var total int64
			for _, sc := range profiles[pod.Spec.SchedulerName] {
				total += sc.weight * sc.score(pod, n, snap)
			}
			switch {
			case total > bestScore:
				bestScore, best = total, []string{n.Name}
			case total == bestScore:
				best = append(best, n.Name)
			}
		}
		// Pods that ask for nothing score every node the same, so the tie is
		// the common case on an empty cluster: keep taking those in turn, or
		// they all land on whichever name sorts first.
		slices.Sort(best)
		turn++
		return best[turn%len(best)], snap, ""
	}

	// The scheduling queue. queued is what to try; unschedulable is what has
	// been tried and found no node, held there until the cluster changes.
	// Informer handlers and the scheduling loop all touch these, so they take
	// a lock, and moves counts the times the cluster changed, so an attempt
	// that was in flight across a change is retried rather than parked.
	var queueMu sync.Mutex
	var moves int
	queued := make(map[string]*corev1.Pod)
	unschedulable := make(map[string]*corev1.Pod)
	wake := make(chan struct{}, 1)

	// A wake-up nobody is waiting for is dropped: the loop checks the queue
	// before it sleeps, so it cannot miss what was queued before the send.
	notify := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	push := func(pod *corev1.Pod) {
		key := pod.Namespace + "/" + pod.Name
		queueMu.Lock()
		delete(unschedulable, key)
		queued[key] = pod
		queueMu.Unlock()
		notify()
	}

	// pop takes the pod that should be tried next: the most important one, and
	// the one that has waited longest of those equally important. Taking the
	// best one now, rather than sorting a copy and working through it, is what
	// makes room that appears midway go to the pod that most deserves it.
	pop := func() (*corev1.Pod, int) {
		queueMu.Lock()
		defer queueMu.Unlock()
		var best *corev1.Pod
		for _, pod := range queued {
			if best == nil || beforeInQueue(pod, best) {
				best = pod
			}
		}
		if best == nil {
			return nil, moves
		}
		delete(queued, best.Namespace+"/"+best.Name)
		return best, moves
	}

	// requeueAll is what a cluster change means to a scheduler: every pod that
	// found no node is worth another look, and none of them is more worth it
	// than the others until they are compared again.
	requeueAll := func() {
		queueMu.Lock()
		moves++
		for key, pod := range unschedulable {
			queued[key] = pod
			delete(unschedulable, key)
		}
		queueMu.Unlock()
		notify()
	}

	schedule := func(pod *corev1.Pod, since int) {
		key := pod.Namespace + "/" + pod.Name
		target, snap, why := *node, snapshot{}, ""
		if target == "" {
			target, snap, why = cycle(pod)
		}
		// More work than room is not an error: the pod is left waiting,
		// and the reason belongs where whoever is waiting for it looks.
		if target == "" {
			// Room can still be made, by taking it from pods that matter less.
			// The pod is not bound here: the victims have to stop running
			// first, and their going brings it back to the queue.
			on, victims := "", []*corev1.Pod(nil)
			// Room already being made is room enough: victims take a moment to
			// go, and a pod that asks again while they do empties a second node
			// for nothing.
			if !evictionUnderway(pod, snap.placed) {
				on, victims = preempt(pod, snap)
			}
			if on != "" {
				nominate(ctx, cs, pod, on)
				for _, v := range victims {
					fmt.Printf("preempting %s/%s on %s for %s/%s\n", v.Namespace, v.Name, on, pod.Namespace, pod.Name)
					recordEvent(ctx, cs, v, corev1.EventTypeWarning, "Preempted",
						"evicted to make room for "+pod.Namespace+"/"+pod.Name)
					if err := cs.CoreV1().Pods(v.Namespace).Delete(ctx, v.Name, metav1.DeleteOptions{}); err != nil {
						fmt.Fprintf(os.Stderr, "preempt %s/%s: %v\n", v.Namespace, v.Name, err)
					}
				}
			}
			fmt.Printf("waiting %s/%s: %s\n", pod.Namespace, pod.Name, why)
			recordEvent(ctx, cs, pod, corev1.EventTypeWarning, "FailedScheduling", why)
			queueMu.Lock()
			// The cluster changed while this pod was being tried, so the
			// answer it just got is already out of date: queue it again
			// instead of parking it until the next change, which may not come.
			if moves != since {
				queued[key] = pod
			} else {
				unschedulable[key] = pod
			}
			queueMu.Unlock()
			notify()
			return
		}
		// One pod that cannot be placed must not stop the others.
		if err := bind(ctx, cs, pod, target); err != nil {
			fmt.Fprintf(os.Stderr, "bind %s/%s: %v\n", pod.Namespace, pod.Name, err)
			return
		}
		fmt.Printf("bound %s/%s to %s\n", pod.Namespace, pod.Name, target)
		recordEvent(ctx, cs, pod, corev1.EventTypeNormal, "Scheduled", "bound to "+target)
	}

	// An informer lists and then watches, so a pod that existed before this
	// program started is reported exactly like one created afterwards.
	if _, err := pods.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return
			}
			if _, ours := profiles[pod.Spec.SchedulerName]; !ours {
				return
			}
			fmt.Printf("unscheduled %s/%s\n", pod.Namespace, pod.Name)
			push(pod)
		},
	}); err != nil {
		return fmt.Errorf("watch pods: %w", err)
	}

	// A pod that found no node is not stuck, it is waiting for the cluster to
	// change. A node arriving or changing — an uncordon, a taint removed, a
	// label added — is the signal that the answer may be different now.
	//
	// One pod is scheduled at a time, from one goroutine, so the queue is
	// consulted again after every decision rather than once per change.
	go func() {
		for {
			pod, since := pop()
			if pod == nil {
				select {
				case <-wake:
				case <-ctx.Done():
					return
				}
				continue
			}
			// The pod may have been placed or deleted since it was queued, so
			// the current one is what gets scheduled, if it is still here.
			current, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil || current.Spec.NodeName != "" {
				continue
			}
			schedule(current, since)
		}
	}()

	// A pod leaving frees what it held, which is a change of the same kind as
	// a node arriving — and it is how a preempted pod's room reaches the pod
	// that preempted it.
	if _, err := allPodsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(any) { requeueAll() },
	}); err != nil {
		return fmt.Errorf("watch placed pods: %w", err)
	}

	if _, err := nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { requeueAll() },
		UpdateFunc: func(any, any) { requeueAll() },
	}); err != nil {
		return fmt.Errorf("watch nodes: %w", err)
	}

	// The nodes are known before the first pod is handled, or the pods that
	// were already waiting would find no node to go to.
	nodeFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced, allPodsInformer.HasSynced, claimInformer.HasSynced, pvInformer.HasSynced) {
		return fmt.Errorf("the node, pod and volume caches never synced")
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), pods.HasSynced) {
		return fmt.Errorf("the pod cache never synced")
	}
	fmt.Println("watching for pods naming", schedulerName)
	<-ctx.Done()
	return nil
}

// nominate records on the pod which node preemption is making room on, so the
// room is not handed to someone else in the moment between the victims being
// evicted and this pod being bound. kubectl shows it, and so does every other
// scheduler reading the same pods.
func nominate(ctx context.Context, cs kubernetes.Interface, pod *corev1.Pod, node string) {
	updated := pod.DeepCopy()
	updated.Status.NominatedNodeName = node
	if _, err := cs.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		fmt.Fprintf(os.Stderr, "nominate %s/%s: %v\n", pod.Namespace, pod.Name, err)
	}
}

// bind places a pod by creating a Binding, which is how a scheduler records a
// decision: the API server sets spec.nodeName from it, and refuses a second
// binding for a pod that already has a node.
func bind(ctx context.Context, cs kubernetes.Interface, pod *corev1.Pod, node string) error {
	return cs.CoreV1().Pods(pod.Namespace).Bind(ctx, &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
		Target:     corev1.ObjectReference{Kind: "Node", Name: node},
	}, metav1.CreateOptions{})
}

// isReady reports whether a node's kubelet says it can run pods now. A node the
// API server lists is not necessarily one that is there.
func isReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// tolerates reports whether a pod may be placed on a node despite its taints.
// Every taint that keeps pods off — NoSchedule and NoExecute — must be
// tolerated by one of the pod's tolerations; PreferNoSchedule is a preference,
// not a filter.
func tolerates(pod *corev1.Pod, node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		tolerated := false
		for _, toleration := range pod.Spec.Tolerations {
			if toleratesTaint(toleration, taint) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			return false
		}
	}
	return true
}

// toleratesTaint reports whether one toleration answers one taint. An empty
// effect answers every effect of that key, and an empty key with operator
// Exists answers every taint there is, which is how a DaemonSet runs anywhere.
func toleratesTaint(toleration corev1.Toleration, taint corev1.Taint) bool {
	if toleration.Effect != "" && toleration.Effect != taint.Effect {
		return false
	}
	if toleration.Key == "" {
		return toleration.Operator == corev1.TolerationOpExists
	}
	if toleration.Key != taint.Key {
		return false
	}
	if toleration.Operator == corev1.TolerationOpExists {
		return true
	}
	return toleration.Value == taint.Value
}

// schedulable reports whether a node takes new pods. Cordoning sets the field
// at once and the node controller adds the taint a moment later, so the field
// is what is true first — and the taint is how a pod says it does not mind,
// which is what keeps a DaemonSet placing pods on a node being drained.
func schedulable(pod *corev1.Pod, node *corev1.Node) bool {
	if !node.Spec.Unschedulable {
		return true
	}
	cordon := corev1.Taint{Key: unschedulableTaint, Effect: corev1.TaintEffectNoSchedule}
	for _, toleration := range pod.Spec.Tolerations {
		if toleratesTaint(toleration, cordon) {
			return true
		}
	}
	return false
}

// requests is what a pod asks of a node: the sum of its containers' requests.
// The full rule also counts init containers and the pod's overhead.
func requests(pod *corev1.Pod) corev1.ResourceList {
	total := corev1.ResourceList{}
	for _, c := range pod.Spec.Containers {
		for name, q := range c.Resources.Requests {
			sum := total[name]
			sum.Add(q)
			total[name] = sum
		}
	}
	return total
}

// fits reports whether a pod fits on a node beside every pod already bound
// there, whoever placed it: its CPU and memory requests within what the node
// allocates, and none of its host ports already held. A finished pod holds
// nothing.
func fits(pod *corev1.Pod, node *corev1.Node, placed []*corev1.Pod) bool {
	return portsFree(pod, node, placed) && roomFor(pod, node, placed)
}

// portsFree reports whether the host ports this pod wants are unspoken for on
// this node. A hostPort is a number and a protocol together, held by one pod.
func portsFree(pod *corev1.Pod, node *corev1.Node, placed []*corev1.Pod) bool {
	held := map[hostPort]bool{}
	for _, p := range placed {
		if occupies(p) != node.Name || done(p) {
			continue
		}
		for _, hp := range hostPorts(p) {
			held[hp] = true
		}
	}
	for _, hp := range hostPorts(pod) {
		if held[hp] {
			return false
		}
	}
	return true
}

// roomFor reports whether the node's allocatable cpu and memory cover what is
// already promised on it plus what this pod asks.
func roomFor(pod *corev1.Pod, node *corev1.Node, placed []*corev1.Pod) bool {
	used := corev1.ResourceList{}
	for _, p := range placed {
		if occupies(p) != node.Name || done(p) {
			continue
		}
		for name, q := range requests(p) {
			sum := used[name]
			sum.Add(q)
			used[name] = sum
		}
	}
	want := requests(pod)
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		total := used[name]
		total.Add(want[name])
		if total.Cmp(node.Status.Allocatable[name]) > 0 {
			return false
		}
	}
	return true
}

// done is a pod that has stopped: it ran and finished, and holds nothing.
func done(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// hostPort is one port a pod claims on its node's own network: the number and
// the protocol together, since TCP 8080 and UDP 8080 are different ports.
type hostPort struct {
	protocol corev1.Protocol
	port     int32
}

// hostPorts lists the ports a pod claims on its node. A container port with no
// hostPort claims nothing there, and an empty protocol means TCP.
func hostPorts(pod *corev1.Pod) []hostPort {
	var out []hostPort
	for _, c := range pod.Spec.Containers {
		for _, p := range c.Ports {
			if p.HostPort == 0 {
				continue
			}
			protocol := p.Protocol
			if protocol == "" {
				protocol = corev1.ProtocolTCP
			}
			out = append(out, hostPort{protocol: protocol, port: p.HostPort})
		}
	}
	return out
}

// affinityMatches reports whether a node satisfies a pod's required node
// affinity. Nil anywhere on the path means the pod asked for nothing, so every
// node matches; otherwise the terms are ORed.
func affinityMatches(pod *corev1.Pod, node *corev1.Node) bool {
	affinity := pod.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil || affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}
	for _, term := range affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if termMatches(term, node.Labels) {
			return true
		}
	}
	return false
}

// termMatches reports whether every expression in one term holds. A term with
// no expressions matches nothing, which is what the API server means by one.
func termMatches(term corev1.NodeSelectorTerm, nodeLabels map[string]string) bool {
	if len(term.MatchExpressions) == 0 {
		return false
	}
	for _, req := range term.MatchExpressions {
		if !expressionMatches(req, nodeLabels) {
			return false
		}
	}
	return true
}

// expressionMatches reports whether one requirement holds for a node's labels.
// A key the node does not carry fails In, Exists, Gt and Lt — and satisfies
// NotIn and DoesNotExist, which is the rule that surprises people.
func expressionMatches(req corev1.NodeSelectorRequirement, nodeLabels map[string]string) bool {
	value, present := nodeLabels[req.Key]
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		return present && slices.Contains(req.Values, value)
	case corev1.NodeSelectorOpNotIn:
		return !present || !slices.Contains(req.Values, value)
	case corev1.NodeSelectorOpExists:
		return present
	case corev1.NodeSelectorOpDoesNotExist:
		return !present
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		if !present || len(req.Values) != 1 {
			return false
		}
		have, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return false
		}
		want, err := strconv.ParseInt(req.Values[0], 10, 64)
		if err != nil {
			return false
		}
		if req.Operator == corev1.NodeSelectorOpGt {
			return have > want
		}
		return have < want
	}
	return false
}

// recordEvent writes a decision down where people look for it: an Event on the
// pod, which is what kubectl describe shows. eventTime is a MicroTime, and the
// API server refuses an Event with no action or reason.
//
// An event is a side effect, not the decision: a scheduler that stopped
// placing pods because it could not write a note would be worse than one that
// places them quietly.
func recordEvent(ctx context.Context, cs kubernetes.Interface, pod *corev1.Pod, eventType, reason, note string) {
	event := &eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.%x", pod.Name, time.Now().UnixNano()),
			Namespace: pod.Namespace,
		},
		EventTime:           metav1.NewMicroTime(time.Now()),
		ReportingController: pod.Spec.SchedulerName,
		ReportingInstance:   schedulerName,
		Action:              "Scheduling",
		Reason:              reason,
		Type:                eventType,
		Note:                note,
		Regarding: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        pod.UID,
		},
	}
	if _, err := cs.EventsV1().Events(pod.Namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil {
		fmt.Fprintf(os.Stderr, "record event for %s/%s: %v\n", pod.Namespace, pod.Name, err)
	}
}
