// A kubelet: register a Node, keep it reported, and run the pods the
// scheduler assigns to it.
//
//	app --kubeconfig PATH --hostname-override NAME [--register-with-taints K=V:EFFECT,…]
//	    --root-dir DIR --containerd-namespace NS --pod-manifest-path DIR --port N
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	node   *corev1.Node
	// readySince is when Ready last changed: a heartbeat refreshes
	// lastHeartbeatTime, never lastTransitionTime.
	readySince metav1.Time
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

	node, err := k.register(ctx)
	if err != nil {
		return err
	}
	k.node = node
	k.readySince = metav1.Now()
	fmt.Println("registered", k.name)
	k.every(ctx, 10*time.Second, k.postStatus)
	k.every(ctx, 10*time.Second, k.renewLease)
	<-ctx.Done()
	return nil
}

// every runs f now and then once per interval until ctx ends, logging what
// fails: one missed heartbeat is not a reason to stop.
func (k *kubelet) every(ctx context.Context, interval time.Duration, f func(context.Context) error) {
	go func() {
		for {
			if err := f(ctx); err != nil && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()
}

// postStatus reports the machine and a fresh Ready heartbeat through the
// Node's status subresource.
func (k *kubelet) postStatus(ctx context.Context) error {
	m, err := readMachine()
	if err != nil {
		return err
	}
	now := metav1.Now()
	resources := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(int64(runtime.NumCPU()), resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(m.memBytes, resource.BinarySI),
		corev1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
	}
	status := corev1.NodeStatus{
		Capacity:    resources,
		Allocatable: resources,
		Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionTrue,
			Reason:             "KubeletReady",
			Message:            "kubelet is posting ready status",
			LastHeartbeatTime:  now,
			LastTransitionTime: k.readySince,
		}},
		Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: m.ip},
			{Type: corev1.NodeHostName, Address: k.name},
		},
		NodeInfo: corev1.NodeSystemInfo{
			MachineID:       m.machineID,
			KernelVersion:   m.kernel,
			OSImage:         m.osImage,
			OperatingSystem: runtime.GOOS,
			Architecture:    runtime.GOARCH,
			KubeletVersion:  "v0.0.0-byok8s",
		},
	}
	// A strategic merge patch merges conditions and addresses by type, so a
	// condition another component wrote survives.
	patch, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return err
	}
	_, err = k.cs.CoreV1().Nodes().Patch(ctx, k.name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "status")
	return err
}

type machine struct {
	memBytes                       int64
	ip, kernel, osImage, machineID string
}

// readMachine reads what the node is from the files Linux keeps about it.
func readMachine() (*machine, error) {
	m := &machine{}
	var memKi int64
	if err := scanFile("/proc/meminfo", func(l string) {
		fmt.Sscanf(l, "MemTotal: %d kB", &memKi)
	}); err != nil {
		return nil, err
	}
	m.memBytes = memKi * 1024
	if err := scanFile("/etc/os-release", func(l string) {
		if v, ok := strings.CutPrefix(l, "PRETTY_NAME="); ok {
			m.osImage = strings.Trim(v, `"`)
		}
	}); err != nil {
		return nil, err
	}
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return nil, err
	}
	m.kernel = strings.TrimSpace(string(kernel))
	id, _ := os.ReadFile("/etc/machine-id")
	m.machineID = strings.TrimSpace(string(id))

	// The node's address is what its own hostname resolves to.
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		if ip.To4() != nil {
			m.ip = ip.String()
			break
		}
	}
	if m.ip == "" {
		return nil, fmt.Errorf("hostname %s has no IPv4 address", host)
	}
	return m, nil
}

func scanFile(path string, line func(string)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line(s.Text())
	}
	return s.Err()
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
func (k *kubelet) register(ctx context.Context) (*corev1.Node, error) {
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
	created, err := k.cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return k.cs.CoreV1().Nodes().Get(ctx, k.name, metav1.GetOptions{})
	}
	return created, err
}

// leaseDuration is how long the node counts as alive after a renewal; the
// kubelet renews at a quarter of it.
const leaseDuration = 40

// renewLease creates or renews the Node's Lease in kube-node-lease. The Node
// owns it, so deleting the Node deletes the Lease.
func (k *kubelet) renewLease(ctx context.Context) error {
	leases := k.cs.CoordinationV1().Leases("kube-node-lease")
	now := metav1.NewMicroTime(time.Now())
	l, err := leases.Get(ctx, k.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = leases.Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      k.name,
				Namespace: "kube-node-lease",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       k.node.Name,
					UID:        k.node.UID,
				}},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &k.name,
				LeaseDurationSeconds: ptr(int32(leaseDuration)),
				RenewTime:            &now,
			},
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	l.Spec.RenewTime = &now
	_, err = leases.Update(ctx, l, metav1.UpdateOptions{})
	return err
}

func ptr[T any](v T) *T { return &v }
