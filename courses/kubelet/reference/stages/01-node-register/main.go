// A kubelet: register a Node, keep it reported, and run the pods the
// scheduler assigns to it.
//
//	app --kubeconfig PATH --hostname-override NAME [--register-with-taints K=V:EFFECT,…]
//	    --root-dir DIR --containerd-namespace NS --pod-manifest-path DIR --port N
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type kubelet struct {
	cs     kubernetes.Interface
	name   string
	taints []corev1.Taint
}

func run() error {
	kubeconfig := flag.String("kubeconfig", "", "path to the kubeconfig")
	name := flag.String("hostname-override", "", "the Node's name")
	taints := flag.String("register-with-taints", "", "taints to register the Node with, K=V:EFFECT,…")
	// The pod half's flags, accepted now so the command line never changes.
	flag.String("root-dir", "", "where the kubelet keeps pod logs and volumes")
	flag.String("containerd-namespace", "", "the containerd namespace pods run in")
	flag.String("pod-manifest-path", "", "a directory of static pod manifests")
	flag.Int("port", 10250, "the port the kubelet serves its API on")
	flag.Parse()
	if *name == "" {
		return fmt.Errorf("--hostname-override is required")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	k := &kubelet{cs: cs, name: *name}
	if k.taints, err = parseTaints(*taints); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	if err := k.register(ctx); err != nil {
		return err
	}
	fmt.Println("registered", k.name)
	<-ctx.Done()
	return nil
}

// parseTaints reads the --register-with-taints form: KEY[=VALUE]:EFFECT,…
func parseTaints(s string) ([]corev1.Taint, error) {
	var out []corev1.Taint
	for _, spec := range strings.Split(s, ",") {
		if spec == "" {
			continue
		}
		kv, effect, ok := strings.Cut(spec, ":")
		if !ok {
			return nil, fmt.Errorf("taint %q has no effect", spec)
		}
		key, value, _ := strings.Cut(kv, "=")
		out = append(out, corev1.Taint{Key: key, Value: value, Effect: corev1.TaintEffect(effect)})
	}
	return out, nil
}

// register creates the Node, and treats one that already exists as ours: a
// restarted kubelet picks up where it left off.
func (k *kubelet) register(ctx context.Context) error {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: k.name,
			Labels: map[string]string{
				corev1.LabelHostname:   k.name,
				corev1.LabelArchStable: runtime.GOARCH,
			},
		},
		Spec: corev1.NodeSpec{Taints: k.taints},
	}
	_, err := k.cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
