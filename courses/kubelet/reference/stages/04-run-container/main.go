// A kubelet: register a Node, keep it reported, and run the pods the
// scheduler assigns to it.
//
//	app --kubeconfig PATH --hostname-override NAME [--register-with-taints K=V:EFFECT,…]
//	    --root-dir DIR --containerd-namespace NS --pod-manifest-path DIR --port N
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/distribution/reference"
	"github.com/opencontainers/runtime-spec/specs-go"
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
	ctr    *client.Client
	root   string
	pods   cache.Store

	mu      sync.Mutex
	workers map[types.UID]bool
	// readySince is when Ready last changed: a heartbeat refreshes
	// lastHeartbeatTime, never lastTransitionTime.
	readySince metav1.Time
}

func run() error {
	kubeconfig := flag.String("kubeconfig", "", "path to the kubeconfig")
	name := flag.String("hostname-override", "", "the Node's name")
	taints := flag.String("register-with-taints", "", "taints to register the Node with, K=V:EFFECT,…")
	root := flag.String("root-dir", "/var/lib/kubelet", "where the kubelet keeps pod logs and volumes")
	ns := flag.String("containerd-namespace", "k8s.io", "the containerd namespace pods run in")
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
	ctr, err := client.New("/run/containerd/containerd.sock", client.WithDefaultNamespace(*ns))
	if err != nil {
		return err
	}
	defer ctr.Close()
	k := &kubelet{cs: cs, name: *name, ctr: ctr, root: *root, workers: map[types.UID]bool{}}
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
	k.watchPods(ctx)
	k.every(ctx, time.Second, k.housekeeping)
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

// watchPods keeps k.pods filled with the pods bound to this Node, and
// returns once it holds them all.
func (k *kubelet) watchPods(ctx context.Context) {
	f := informers.NewSharedInformerFactoryWithOptions(k.cs, 0, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", k.name).String()
	}))
	inf := f.Core().V1().Pods().Informer()
	k.pods = inf.GetStore()
	f.Start(ctx.Done())
	cache.WaitForCacheSync(ctx.Done(), inf.HasSynced)
}

// housekeeping starts a worker for every pod that has none.
func (k *kubelet) housekeeping(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, obj := range k.pods.List() {
		p := obj.(*corev1.Pod)
		if !k.workers[p.UID] {
			k.workers[p.UID] = true
			go k.podWorker(ctx, p.UID, cache.MetaObjectToName(p).String())
		}
	}
	return nil
}

// podWorker syncs one pod every second for as long as it exists.
func (k *kubelet) podWorker(ctx context.Context, uid types.UID, key string) {
	for ctx.Err() == nil {
		obj, ok, _ := k.pods.GetByKey(key)
		if !ok || obj.(*corev1.Pod).UID != uid {
			return
		}
		if err := k.syncPod(ctx, obj.(*corev1.Pod)); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "error: sync pod %s: %v\n", key, err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

// syncPod starts every container of p that has not been started.
func (k *kubelet) syncPod(ctx context.Context, p *corev1.Pod) error {
	running, err := k.podContainers(ctx, p.UID)
	if err != nil {
		return err
	}
	for i := range p.Spec.Containers {
		c := &p.Spec.Containers[i]
		if _, ok := running[c.Name]; ok {
			continue
		}
		if err := k.startContainer(ctx, p, c); err != nil {
			return fmt.Errorf("start container %s: %w", c.Name, err)
		}
	}
	return nil
}

// Labels tie a containerd container back to its pod, the way the CRI
// labels its containers.
const (
	labelPodUID       = "io.kubernetes.pod.uid"
	labelPodName      = "io.kubernetes.pod.name"
	labelPodNamespace = "io.kubernetes.pod.namespace"
	labelContainer    = "io.kubernetes.container.name"
)

// podContainers is the containerd containers of pod uid, by container name.
func (k *kubelet) podContainers(ctx context.Context, uid types.UID) (map[string]client.Container, error) {
	cs, err := k.ctr.Containers(ctx, fmt.Sprintf("labels.%q==%s", labelPodUID, uid))
	if err != nil {
		return nil, err
	}
	out := map[string]client.Container{}
	for _, c := range cs {
		labels, err := c.Labels(ctx)
		if err != nil {
			return nil, err
		}
		out[labels[labelContainer]] = c
	}
	return out, nil
}

// startContainer creates container c of pod p in containerd and starts it.
func (k *kubelet) startContainer(ctx context.Context, p *corev1.Pod, c *corev1.Container) error {
	img, err := k.image(ctx, c.Image)
	if err != nil {
		return err
	}
	cfg, err := img.Spec(ctx)
	if err != nil {
		return err
	}
	logPath := filepath.Join(k.root, "logs", fmt.Sprintf("%s_%s_%s", p.Namespace, p.Name, p.UID), c.Name, "0.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return err
	}
	id := randomID()
	opts := []oci.SpecOpts{
		oci.WithImageConfig(img),
		oci.WithProcessArgs(argv(cfg.Config.Entrypoint, cfg.Config.Cmd, c.Command, c.Args)...),
		oci.WithHostNamespace(specs.NetworkNamespace),
		oci.WithHostHostsFile,
		oci.WithHostResolvconf,
	}
	if c.WorkingDir != "" {
		opts = append(opts, oci.WithProcessCwd(c.WorkingDir))
	}
	ctr, err := k.ctr.NewContainer(ctx, id,
		client.WithImage(img),
		client.WithNewSnapshot(id, img),
		client.WithContainerLabels(map[string]string{
			labelPodUID:       string(p.UID),
			labelPodName:      p.Name,
			labelPodNamespace: p.Namespace,
			labelContainer:    c.Name,
		}),
		client.WithNewSpec(opts...),
	)
	if err != nil {
		return err
	}
	task, err := ctr.NewTask(ctx, cio.LogFile(logPath))
	if err != nil {
		return err
	}
	return task.Start(ctx)
}

// argv is what a container runs: command replaces the image's entrypoint and
// args its cmd, and setting command alone drops the image's cmd too.
func argv(entrypoint, cmd, command, args []string) []string {
	if len(command) > 0 {
		return slices.Concat(command, args)
	}
	if len(args) > 0 {
		return slices.Concat(entrypoint, args)
	}
	return slices.Concat(entrypoint, cmd)
}

// image returns the image ref names, pulling it when containerd lacks it.
func (k *kubelet) image(ctx context.Context, ref string) (client.Image, error) {
	named, err := reference.ParseDockerRef(ref)
	if err != nil {
		return nil, err
	}
	img, err := k.ctr.GetImage(ctx, named.String())
	if errdefs.IsNotFound(err) {
		return k.ctr.Pull(ctx, named.String(), client.WithPullUnpack)
	}
	return img, err
}

func randomID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}
