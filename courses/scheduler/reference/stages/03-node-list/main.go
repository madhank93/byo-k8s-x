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

	// Nodes are taken in turn, so pods spread without one node yet being
	// weighed against another. The add handler runs on one goroutine, so the
	// counter needs no lock.
	var turn int
	pick := func() string {
		all, err := nodeLister.List(labels.Everything())
		if err != nil {
			return ""
		}
		var ready []string
		for _, n := range all {
			if isReady(n) {
				ready = append(ready, n.Name)
			}
		}
		if len(ready) == 0 {
			return ""
		}
		slices.Sort(ready)
		turn++
		return ready[turn%len(ready)]
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
				target = pick()
			}
			if target == "" {
				fmt.Fprintf(os.Stderr, "no ready node for %s/%s\n", pod.Namespace, pod.Name)
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
	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced) {
		return fmt.Errorf("the node cache never synced")
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
