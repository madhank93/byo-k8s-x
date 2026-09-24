// Your scheduler.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strconv"
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
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// The name pods use to ask for this scheduler. The default scheduler ignores
// any pod that names another, so these are this program's alone to place.
const schedulerName = "byok8s"

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
		if p.Spec.NodeName != node.Name {
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

	// The API server narrows the stream to pods naming this scheduler and not
	// yet on a node, so nothing else is ever sent here to be thrown away.
	unbound := fields.AndSelectors(
		fields.OneTermEqualSelector("spec.schedulerName", schedulerName),
		fields.OneTermEqualSelector("spec.nodeName", ""),
	).String()
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

	// Nodes are taken in turn, so pods spread without one node yet being
	// weighed against another. The add handler runs on one goroutine, so the
	// counter needs no lock.
	var turn int
	pick := func(pod *corev1.Pod) string {
		all, err := nodeLister.List(labels.Everything())
		if err != nil {
			return ""
		}
		placed, err := podLister.List(labels.Everything())
		if err != nil {
			return ""
		}
		// A pod's node selector names labels a node must carry, every one of
		// them; an empty selector matches every node.
		selector := labels.SelectorFromSet(pod.Spec.NodeSelector)
		var candidates []*corev1.Node
		for _, n := range all {
			if isReady(n) && schedulable(pod, n) && tolerates(pod, n) && selector.Matches(labels.Set(n.Labels)) && affinityMatches(pod, n) && topologySatisfied(pod, n, all, placed) && fits(pod, n, placed) {
				candidates = append(candidates, n)
			}
		}
		if len(candidates) == 0 {
			return ""
		}
		// Spreading is the one score that cannot be worked out from a node
		// alone: how bad three siblings on a node is depends on whether the
		// other nodes have none or have three of their own. So the counts are
		// taken first and the worst becomes the scale the rest are judged on.
		siblings := make(map[string]int64, len(candidates))
		var worst int64
		for _, n := range candidates {
			siblings[n.Name] = siblingsOn(pod, n, placed)
			if siblings[n.Name] > worst {
				worst = siblings[n.Name]
			}
		}
		spread := func(node string) int64 {
			if worst == 0 {
				return 100
			}
			return (worst - siblings[node]) * 100 / worst
		}

		// Fitting is a yes or no; it does not say which yes is better. Room
		// left is one answer, evenness is another, how soon the pod can start
		// is a third, and keeping replicas apart is a fourth: the scores are
		// added, the way a real scheduler sums the plugins that scored a node.
		var best []string
		bestScore := int64(-1)
		for _, n := range candidates {
			switch s := leastAllocated(pod, n, placed) + balancedAllocation(pod, n, placed) + imageLocality(pod, n) + spread(n.Name); {
			case s > bestScore:
				bestScore, best = s, []string{n.Name}
			case s == bestScore:
				best = append(best, n.Name)
			}
		}
		// Pods that ask for nothing score every node the same, so the tie is
		// the common case on an empty cluster: keep taking those in turn, or
		// they all land on whichever name sorts first.
		slices.Sort(best)
		turn++
		return best[turn%len(best)]
	}

	// Pods that found no node, kept so the cluster can change their answer.
	// Node handlers and pod handlers both touch this, so it takes a lock.
	var waitingMu sync.Mutex
	waiting := make(map[string]*corev1.Pod)

	schedule := func(pod *corev1.Pod) {
		key := pod.Namespace + "/" + pod.Name
		target := *node
		if target == "" {
			target = pick(pod)
		}
		// More work than room is not an error: the pod is left waiting,
		// and the reason belongs where whoever is waiting for it looks.
		if target == "" {
			fmt.Printf("waiting %s/%s: no node fits\n", pod.Namespace, pod.Name)
			recordEvent(ctx, cs, pod, corev1.EventTypeWarning, "FailedScheduling", "no node fits this pod")
			waitingMu.Lock()
			waiting[key] = pod
			waitingMu.Unlock()
			return
		}
		// One pod that cannot be placed must not stop the others.
		if err := bind(ctx, cs, pod, target); err != nil {
			fmt.Fprintf(os.Stderr, "bind %s/%s: %v\n", pod.Namespace, pod.Name, err)
			return
		}
		// A pod with a node is nobody's problem any more.
		waitingMu.Lock()
		delete(waiting, key)
		waitingMu.Unlock()
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
			fmt.Printf("unscheduled %s/%s\n", pod.Namespace, pod.Name)
			schedule(pod)
		},
	}); err != nil {
		return fmt.Errorf("watch pods: %w", err)
	}

	// A pod that found no node is not stuck, it is waiting for the cluster to
	// change. A node arriving or changing — an uncordon, a taint removed, a
	// label added — is the signal that the answer may be different now.
	retryWaiting := func() {
		waitingMu.Lock()
		pods := make([]*corev1.Pod, 0, len(waiting))
		for _, pod := range waiting {
			pods = append(pods, pod)
		}
		waitingMu.Unlock()
		for _, pod := range pods {
			// The pod may have been placed or deleted since it was remembered,
			// so the current one is what gets scheduled, if it is still here.
			current, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil || current.Spec.NodeName != "" {
				waitingMu.Lock()
				delete(waiting, pod.Namespace+"/"+pod.Name)
				waitingMu.Unlock()
				continue
			}
			schedule(current)
		}
	}

	if _, err := nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { retryWaiting() },
		UpdateFunc: func(any, any) { retryWaiting() },
	}); err != nil {
		return fmt.Errorf("watch nodes: %w", err)
	}

	// The nodes are known before the first pod is handled, or the pods that
	// were already waiting would find no node to go to.
	nodeFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced, allPodsInformer.HasSynced) {
		return fmt.Errorf("the node and pod caches never synced")
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), pods.HasSynced) {
		return fmt.Errorf("the pod cache never synced")
	}
	fmt.Println("watching for pods naming", schedulerName)
	<-ctx.Done()
	return nil
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
	used := corev1.ResourceList{}
	held := map[hostPort]bool{}
	for _, p := range placed {
		if p.Spec.NodeName != node.Name || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		for name, q := range requests(p) {
			sum := used[name]
			sum.Add(q)
			used[name] = sum
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
		ReportingController: schedulerName,
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
