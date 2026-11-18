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
	"syscall"

	corev1 "k8s.io/api/core/v1"
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

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
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
	waiting := fields.AndSelectors(
		fields.OneTermEqualSelector("spec.schedulerName", schedulerName),
		fields.OneTermEqualSelector("spec.nodeName", ""),
	).String()
	factory := informers.NewSharedInformerFactoryWithOptions(cs, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.FieldSelector = waiting }))
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
		var candidates []string
		for _, n := range all {
			if isReady(n) && tolerates(pod, n) && selector.Matches(labels.Set(n.Labels)) && affinityMatches(pod, n) && fits(pod, n, placed) {
				candidates = append(candidates, n.Name)
			}
		}
		if len(candidates) == 0 {
			return ""
		}
		slices.Sort(candidates)
		turn++
		return candidates[turn%len(candidates)]
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
			target := *node
			if target == "" {
				target = pick(pod)
			}
			// More work than room is not an error: the pod is left waiting.
			if target == "" {
				fmt.Printf("waiting %s/%s: no node fits\n", pod.Namespace, pod.Name)
				return
			}
			// One pod that cannot be placed must not stop the others.
			if err := bind(ctx, cs, pod, target); err != nil {
				fmt.Fprintf(os.Stderr, "bind %s/%s: %v\n", pod.Namespace, pod.Name, err)
				return
			}
			fmt.Printf("bound %s/%s to %s\n", pod.Namespace, pod.Name, target)
		},
	}); err != nil {
		return fmt.Errorf("watch pods: %w", err)
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
