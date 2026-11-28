// A kubelet: register a Node, keep it reported, and run the pods the
// scheduler assigns to it.
//
//	app --kubeconfig PATH --hostname-override NAME [--register-with-taints K=V:EFFECT,…]
//	    --root-dir DIR --containerd-namespace NS --pod-manifest-path DIR --port N
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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
	ip     string
	// capacity is what the node reports it has.
	capacity corev1.ResourceList
	pods     cache.Store

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
	ns := flag.String("containerd-namespace", "", "the containerd namespace pods run in, never k8s.io: the CRI owns that one")
	flag.String("pod-manifest-path", "", "a directory of static pod manifests")
	flag.Int("port", 10250, "the port the kubelet serves its API on")
	flag.Parse()
	if *name == "" {
		return fmt.Errorf("--hostname-override is required")
	}
	if *ns == "" || *ns == "k8s.io" {
		return fmt.Errorf("--containerd-namespace must name a namespace of the kubelet's own, not k8s.io")
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
	m, err := readMachine()
	if err != nil {
		return err
	}
	k.ip = m.ip
	k.capacity = corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(int64(runtime.NumCPU()), resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(m.memBytes, resource.BinarySI),
		corev1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
	}
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
			w := &worker{uid: p.UID, key: cache.MetaObjectToName(p).String(), waiting: map[string]*corev1.ContainerStateWaiting{}, tokens: map[string]token{}}
			go k.podWorker(ctx, w)
		}
	}
	return nil
}

// worker is one pod's goroutine and what it remembers between syncs.
type worker struct {
	uid types.UID
	key string
	// waiting says why a container that should be running is not.
	waiting map[string]*corev1.ContainerStateWaiting
	// paths is where this sync set up the pod's volumes, or volErr why not.
	paths  map[string]string
	volErr error
	// tokens are the service account tokens issued for the pod, by path.
	tokens map[string]token
}

type token struct {
	value     string
	refreshAt time.Time
}

// podWorker syncs one pod every second for as long as it exists.
func (k *kubelet) podWorker(ctx context.Context, w *worker) {
	for ctx.Err() == nil {
		obj, ok, _ := k.pods.GetByKey(w.key)
		if !ok || obj.(*corev1.Pod).UID != w.uid {
			k.removePod(ctx, w)
			return
		}
		if err := k.syncPod(ctx, w, obj.(*corev1.Pod)); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "error: sync pod %s: %v\n", w.key, err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

// syncPod starts every container of p that has not been started, then
// writes back the status containerd reports.
func (k *kubelet) syncPod(ctx context.Context, w *worker, p *corev1.Pod) error {
	if p.DeletionTimestamp != nil {
		return k.terminate(ctx, p)
	}
	if terminal(p.Status.Phase) {
		return nil
	}
	states, err := k.podContainers(ctx, p.UID)
	if err != nil {
		return err
	}
	// Volumes are set up on every sync, so ConfigMap and Secret files
	// follow their objects.
	w.paths, w.volErr = k.volumes(ctx, w, p)
	initialized, failed := initDone(p, states)
	if !initialized && !failed {
		// Init containers run one at a time: the first not yet done.
		for i := range p.Spec.InitContainers {
			c := &p.Spec.InitContainers[i]
			if st := states[c.Name]; st != nil && !st.running && st.exitCode == 0 {
				continue
			}
			k.ensure(ctx, w, p, c, states[c.Name], initPolicy(p.Spec.RestartPolicy))
			break
		}
	}
	if initialized {
		for i := range p.Spec.Containers {
			k.ensure(ctx, w, p, &p.Spec.Containers[i], states[p.Spec.Containers[i].Name], p.Spec.RestartPolicy)
		}
	}
	if states, err = k.podContainers(ctx, p.UID); err != nil {
		return err
	}
	return k.writeStatus(ctx, p, k.podStatus(p, states, w))
}

// ensure starts container c, whose latest run is st, if it has not run or
// should run again and its back-off is over.
func (k *kubelet) ensure(ctx context.Context, w *worker, p *corev1.Pod, c *corev1.Container, st *ctrState, policy corev1.RestartPolicy) {
	restart := 0
	if st != nil {
		if st.running || !restarts(policy, st.exitCode) {
			return
		}
		restart = st.restart + 1
		if wait := time.Until(st.finished.Add(backoff(st.restart))); wait > 0 {
			w.waiting[c.Name] = &corev1.ContainerStateWaiting{
				Reason:  "CrashLoopBackOff",
				Message: fmt.Sprintf("back-off %s restarting failed container=%s pod=%s", backoff(st.restart), c.Name, p.Name),
			}
			return
		}
	}
	if w.volErr != nil {
		w.waiting[c.Name] = waitingFor(w.volErr)
		return
	}
	if err := k.startContainer(ctx, p, c, restart, w.paths); err != nil {
		w.waiting[c.Name] = waitingFor(err)
		fmt.Fprintf(os.Stderr, "error: start container %s of %s: %v\n", c.Name, w.key, err)
		return
	}
	delete(w.waiting, c.Name)
}

// initDone reports whether every init container has exited 0, and whether
// one has failed for good.
func initDone(p *corev1.Pod, states map[string]*ctrState) (done, failed bool) {
	for _, c := range p.Spec.InitContainers {
		st := states[c.Name]
		if st == nil || st.running {
			return false, false
		}
		if st.exitCode != 0 {
			return false, !restarts(initPolicy(p.Spec.RestartPolicy), st.exitCode)
		}
	}
	return true, false
}

// initPolicy is how init containers restart: a failed one runs again unless
// the pod never restarts anything, and one that succeeded never does.
func initPolicy(policy corev1.RestartPolicy) corev1.RestartPolicy {
	if policy == corev1.RestartPolicyNever {
		return policy
	}
	return corev1.RestartPolicyOnFailure
}

// terminate stops a deleted pod's containers within its grace period, then
// deletes the pod for good: the API server waits for the kubelet to say its
// containers are gone.
func (k *kubelet) terminate(ctx context.Context, p *corev1.Pod) error {
	grace := int64(30)
	if p.DeletionGracePeriodSeconds != nil {
		grace = *p.DeletionGracePeriodSeconds
	}
	if err := k.stopContainers(ctx, p.UID, time.Duration(grace)*time.Second); err != nil {
		return err
	}
	err := k.cs.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
		GracePeriodSeconds: ptr(int64(0)),
		Preconditions:      metav1.NewUIDPreconditions(string(p.UID)),
	})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

// stopContainers sends every running container of pod uid SIGTERM, and
// SIGKILL to any still running when grace is up.
func (k *kubelet) stopContainers(ctx context.Context, uid types.UID, grace time.Duration) error {
	cs, err := k.ctr.Containers(ctx, fmt.Sprintf("labels.%q==%s", labelPodUID, uid))
	if err != nil {
		return err
	}
	deadline := time.Now().Add(grace)
	var wg sync.WaitGroup
	errs := make([]error, len(cs))
	for i, c := range cs {
		task, err := c.Task(ctx, nil)
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = stopTask(ctx, task, deadline)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func stopTask(ctx context.Context, task client.Task, deadline time.Time) error {
	exited, err := task.Wait(ctx)
	if err != nil {
		return err
	}
	if err := task.Kill(ctx, syscall.SIGTERM); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	select {
	case <-exited:
		return nil
	case <-time.After(time.Until(deadline)):
	}
	if err := task.Kill(ctx, syscall.SIGKILL); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	select {
	case <-exited:
	case <-ctx.Done():
	}
	return nil
}

// removePod kills and deletes every container of a pod that is gone from
// the API server, and forgets it.
func (k *kubelet) removePod(ctx context.Context, w *worker) {
	cs, err := k.ctr.Containers(ctx, fmt.Sprintf("labels.%q==%s", labelPodUID, w.uid))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: remove pod %s: %v\n", w.key, err)
	}
	for _, c := range cs {
		if task, err := c.Task(ctx, nil); err == nil {
			task.Delete(ctx, client.WithProcessKill)
		}
		if err := c.Delete(ctx, client.WithSnapshotCleanup); err != nil {
			fmt.Fprintf(os.Stderr, "error: remove container %s: %v\n", c.ID(), err)
		}
	}
	ns, name, _ := strings.Cut(w.key, "/")
	os.RemoveAll(filepath.Join(k.root, "logs", fmt.Sprintf("%s_%s_%s", ns, name, w.uid)))
	if err := k.removeVolumes(w.uid); err != nil {
		fmt.Fprintf(os.Stderr, "error: remove volumes of %s: %v\n", w.key, err)
	}
	k.mu.Lock()
	delete(k.workers, w.uid)
	k.mu.Unlock()
}

// restarts reports whether a container that exited with code runs again.
func restarts(policy corev1.RestartPolicy, code int32) bool {
	return policy == corev1.RestartPolicyAlways || policy == corev1.RestartPolicyOnFailure && code != 0
}

// backoff is how long a container waits after its restart-th run exits
// before it runs again: 10s, doubling, at most five minutes.
func backoff(restart int) time.Duration {
	return min(10*time.Second<<min(restart, 5), 5*time.Minute)
}

func terminal(phase corev1.PodPhase) bool {
	return phase == corev1.PodSucceeded || phase == corev1.PodFailed
}

// pullError is an image that could not be had.
type pullError struct{ error }

func waitingFor(err error) *corev1.ContainerStateWaiting {
	if _, ok := err.(pullError); ok {
		return &corev1.ContainerStateWaiting{Reason: "ErrImagePull", Message: err.Error()}
	}
	if _, ok := err.(volumeError); ok {
		return &corev1.ContainerStateWaiting{Reason: "ContainerCreating", Message: err.Error()}
	}
	if _, ok := err.(configError); ok {
		return &corev1.ContainerStateWaiting{Reason: "CreateContainerConfigError", Message: err.Error()}
	}
	return &corev1.ContainerStateWaiting{Reason: "CreateContainerError", Message: err.Error()}
}

// Labels tie a containerd container back to its pod, the way the CRI
// labels its containers, and keep what the kubelet learned about it.
const (
	labelPodUID       = "io.kubernetes.pod.uid"
	labelPodName      = "io.kubernetes.pod.name"
	labelPodNamespace = "io.kubernetes.pod.namespace"
	labelContainer    = "io.kubernetes.container.name"
	labelRestartCount = "io.kubernetes.container.restartCount"
	labelImageID      = "byok8s.dev/image-id"
	labelStartedAt    = "byok8s.dev/started-at"
	labelFinishedAt   = "byok8s.dev/finished-at"
	labelExitCode     = "byok8s.dev/exit-code"
)

// ctrState is what one containerd container says about a run of a pod's
// container.
type ctrState struct {
	restart int
	// prev is the run before this one, which a status reports as lastState.
	prev              *ctrState
	id                string
	imageID           string
	running           bool
	exitCode          int32
	started, finished time.Time
}

// podContainers is the state of pod uid's containers in containerd, by
// container name.
func (k *kubelet) podContainers(ctx context.Context, uid types.UID) (map[string]*ctrState, error) {
	cs, err := k.ctr.Containers(ctx, fmt.Sprintf("labels.%q==%s", labelPodUID, uid))
	if err != nil {
		return nil, err
	}
	runs := map[string][]*ctrState{}
	ctrs := map[string]client.Container{}
	for _, c := range cs {
		labels, err := c.Labels(ctx)
		if err != nil {
			return nil, err
		}
		st, err := k.inspect(ctx, c, labels)
		if err != nil {
			return nil, err
		}
		st.restart, _ = strconv.Atoi(labels[labelRestartCount])
		runs[labels[labelContainer]] = append(runs[labels[labelContainer]], st)
		ctrs[st.id] = c
	}
	out := map[string]*ctrState{}
	for name, rs := range runs {
		slices.SortFunc(rs, func(a, b *ctrState) int { return a.restart - b.restart })
		// Only the last two runs are ever reported; older ones go.
		for _, old := range rs[:max(len(rs)-2, 0)] {
			if err := ctrs[old.id].Delete(ctx, client.WithSnapshotCleanup); err != nil {
				return nil, err
			}
		}
		latest := rs[len(rs)-1]
		if len(rs) > 1 {
			latest.prev = rs[len(rs)-2]
		}
		out[name] = latest
	}
	return out, nil
}

// inspect reads a container's state. An exited task's status lives only as
// long as the task, so its exit code and time move into the container's
// labels and the task is deleted.
func (k *kubelet) inspect(ctx context.Context, c client.Container, labels map[string]string) (*ctrState, error) {
	st := &ctrState{id: c.ID(), imageID: labels[labelImageID]}
	st.started, _ = time.Parse(time.RFC3339Nano, labels[labelStartedAt])
	if code, ok := labels[labelExitCode]; ok {
		n, _ := strconv.Atoi(code)
		st.exitCode = int32(n)
		st.finished, _ = time.Parse(time.RFC3339Nano, labels[labelFinishedAt])
		return st, nil
	}
	task, err := c.Task(ctx, nil)
	if errdefs.IsNotFound(err) {
		// Created but never started: count it as a run that failed.
		st.exitCode, st.finished = 128, st.started
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	status, err := task.Status(ctx)
	if err != nil {
		return nil, err
	}
	if status.Status != client.Stopped {
		st.running = true
		return st, nil
	}
	st.exitCode, st.finished = int32(status.ExitStatus), status.ExitTime
	if _, err := c.SetLabels(ctx, map[string]string{
		labelExitCode:   strconv.Itoa(int(status.ExitStatus)),
		labelFinishedAt: status.ExitTime.Format(time.RFC3339Nano),
	}); err != nil {
		return nil, err
	}
	_, err = task.Delete(ctx)
	return st, err
}

// startContainer creates container c of pod p in containerd and starts it.
func (k *kubelet) startContainer(ctx context.Context, p *corev1.Pod, c *corev1.Container, restart int, paths map[string]string) error {
	env, vars, err := k.containerEnv(ctx, p, c)
	if err != nil {
		return err
	}
	ms, err := mounts(c, paths)
	if err != nil {
		return err
	}
	img, err := k.image(ctx, c.Image)
	if err != nil {
		return err
	}
	cfg, err := img.Spec(ctx)
	if err != nil {
		return err
	}
	logPath := filepath.Join(k.root, "logs", fmt.Sprintf("%s_%s_%s", p.Namespace, p.Name, p.UID), c.Name, strconv.Itoa(restart)+".log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return err
	}
	id := randomID()
	opts := []oci.SpecOpts{
		oci.WithImageConfig(img),
		oci.WithProcessArgs(argv(cfg.Config.Entrypoint, cfg.Config.Cmd, expandAll(c.Command, vars), expandAll(c.Args, vars))...),
		oci.WithEnv(env),
		oci.WithMounts(ms),
		oci.WithHostNamespace(specs.NetworkNamespace),
		oci.WithHostHostsFile,
		oci.WithHostResolvconf,
	}
	if c.WorkingDir != "" {
		opts = append(opts, oci.WithProcessCwd(c.WorkingDir))
	}
	named, _ := reference.ParseNormalizedNamed(img.Name())
	ctr, err := k.ctr.NewContainer(ctx, id,
		client.WithImage(img),
		client.WithNewSnapshot(id, img),
		client.WithContainerLabels(map[string]string{
			labelPodUID:       string(p.UID),
			labelPodName:      p.Name,
			labelPodNamespace: p.Namespace,
			labelContainer:    c.Name,
			labelRestartCount: strconv.Itoa(restart),
			labelImageID:      named.Name() + "@" + img.Target().Digest.String(),
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
	if _, err := ctr.SetLabels(ctx, map[string]string{labelStartedAt: time.Now().Format(time.RFC3339Nano)}); err != nil {
		return err
	}
	return task.Start(ctx)
}

// podStatus is p's status as its containers' states make it.
func (k *kubelet) podStatus(p *corev1.Pod, states map[string]*ctrState, w *worker) corev1.PodStatus {
	s := *p.Status.DeepCopy()
	s.HostIP, s.HostIPs = k.ip, []corev1.HostIP{{IP: k.ip}}
	s.PodIP, s.PodIPs = k.ip, []corev1.PodIP{{IP: k.ip}}
	if s.StartTime == nil {
		s.StartTime = ptr(stamp(time.Now()))
	}
	initialized, failed := initDone(p, states)
	s.InitContainerStatuses = nil
	for _, c := range p.Spec.InitContainers {
		s.InitContainerStatuses = append(s.InitContainerStatuses, containerStatus(c, states[c.Name], w.waiting[c.Name]))
	}
	s.ContainerStatuses = nil
	for _, c := range p.Spec.Containers {
		waiting := w.waiting[c.Name]
		if !initialized {
			waiting = &corev1.ContainerStateWaiting{Reason: "PodInitializing"}
		}
		s.ContainerStatuses = append(s.ContainerStatuses, containerStatus(c, states[c.Name], waiting))
	}
	switch {
	case failed:
		s.Phase = corev1.PodFailed
	case !initialized:
		s.Phase = corev1.PodPending
	default:
		s.Phase = phase(p.Spec.RestartPolicy, s.ContainerStatuses)
	}
	ready := s.Phase == corev1.PodRunning
	for _, cs := range s.ContainerStatuses {
		ready = ready && cs.Ready
	}
	s.Conditions = setConditions(s.Conditions, map[corev1.PodConditionType]bool{
		corev1.PodScheduled:    true,
		corev1.PodInitialized:  initialized,
		corev1.ContainersReady: ready,
		corev1.PodReady:        ready,
	})
	return s
}

func containerStatus(c corev1.Container, st *ctrState, waiting *corev1.ContainerStateWaiting) corev1.ContainerStatus {
	cs := corev1.ContainerStatus{Name: c.Name, Image: c.Image, Started: ptr(false)}
	switch {
	case st == nil:
		if waiting == nil {
			waiting = &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}
		}
		cs.State.Waiting = waiting
		return cs
	case st.running:
		cs.State.Running = &corev1.ContainerStateRunning{StartedAt: stamp(st.started)}
		cs.Ready, cs.Started = true, ptr(true)
	case waiting != nil:
		// Exited and waiting to run again: the run that ended is lastState.
		cs.State.Waiting = waiting
		cs.LastTerminationState.Terminated = terminated(st)
	default:
		cs.State.Terminated = terminated(st)
	}
	if st.prev != nil && cs.LastTerminationState.Terminated == nil {
		cs.LastTerminationState.Terminated = terminated(st.prev)
	}
	cs.RestartCount = int32(st.restart)
	cs.ContainerID, cs.ImageID = "containerd://"+st.id, st.imageID
	return cs
}

func terminated(st *ctrState) *corev1.ContainerStateTerminated {
	reason := "Completed"
	if st.exitCode != 0 {
		reason = "Error"
	}
	return &corev1.ContainerStateTerminated{
		ExitCode:    st.exitCode,
		Reason:      reason,
		StartedAt:   stamp(st.started),
		FinishedAt:  stamp(st.finished),
		ContainerID: "containerd://" + st.id,
	}
}

// phase follows the kubelet's rule: Pending while any container waits to
// start, Running while any runs or will run again, and otherwise Succeeded
// only if every container exited 0.
func phase(policy corev1.RestartPolicy, statuses []corev1.ContainerStatus) corev1.PodPhase {
	var waiting, running, stopped, succeeded int
	for _, cs := range statuses {
		switch {
		case cs.State.Running != nil:
			running++
		case cs.State.Terminated != nil:
			stopped++
			if cs.State.Terminated.ExitCode == 0 {
				succeeded++
			}
		case cs.LastTerminationState.Terminated != nil:
			stopped++
		default:
			waiting++
		}
	}
	switch {
	case waiting > 0:
		return corev1.PodPending
	case running > 0, policy == corev1.RestartPolicyAlways:
		return corev1.PodRunning
	case policy == corev1.RestartPolicyOnFailure && succeeded < stopped:
		return corev1.PodRunning
	case succeeded == stopped:
		return corev1.PodSucceeded
	}
	return corev1.PodFailed
}

// setConditions sets each condition's status, keeping its transition time
// unless the status changed.
func setConditions(old []corev1.PodCondition, want map[corev1.PodConditionType]bool) []corev1.PodCondition {
	var out []corev1.PodCondition
	for _, t := range []corev1.PodConditionType{corev1.PodScheduled, corev1.PodInitialized, corev1.ContainersReady, corev1.PodReady} {
		c := corev1.PodCondition{Type: t, Status: corev1.ConditionFalse, LastTransitionTime: stamp(time.Now())}
		if want[t] {
			c.Status = corev1.ConditionTrue
		}
		for _, o := range old {
			if o.Type == t && o.Status == c.Status {
				c.LastTransitionTime = o.LastTransitionTime
			}
		}
		out = append(out, c)
	}
	return out
}

// stamp is t as the API stores it, to the second, so a status that has not
// changed compares equal to the one already written.
func stamp(t time.Time) metav1.Time { return metav1.NewTime(t.Truncate(time.Second)) }

// writeStatus writes s as p's status, unless it already is.
func (k *kubelet) writeStatus(ctx context.Context, p *corev1.Pod, s corev1.PodStatus) error {
	if equality.Semantic.DeepEqual(p.Status, s) {
		return nil
	}
	q := p.DeepCopy()
	q.Status = s
	_, err := k.cs.CoreV1().Pods(p.Namespace).UpdateStatus(ctx, q, metav1.UpdateOptions{})
	return err
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
		return nil, pullError{err}
	}
	img, err := k.ctr.GetImage(ctx, named.String())
	if errdefs.IsNotFound(err) {
		img, err = k.ctr.Pull(ctx, named.String(), client.WithPullUnpack)
		if err != nil {
			return nil, pullError{err}
		}
	}
	return img, err
}

func randomID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// configError is a container whose spec names config that cannot be had.
type configError struct{ error }

// containerEnv is c's environment, in order: envFrom, then env, each value
// with the $(VAR) references to variables before it expanded.
func (k *kubelet) containerEnv(ctx context.Context, p *corev1.Pod, c *corev1.Container) ([]string, map[string]string, error) {
	vars := map[string]string{}
	var order []string
	set := func(name, value string) {
		if _, ok := vars[name]; !ok {
			order = append(order, name)
		}
		vars[name] = value
	}
	for _, from := range c.EnvFrom {
		data, err := k.envFromSource(ctx, p.Namespace, from)
		if err != nil {
			return nil, nil, err
		}
		for _, key := range slices.Sorted(maps.Keys(data)) {
			set(from.Prefix+key, data[key])
		}
	}
	for _, e := range c.Env {
		if e.ValueFrom == nil {
			set(e.Name, expand(e.Value, vars))
			continue
		}
		v, ok, err := k.envValue(ctx, p, c, e.ValueFrom)
		if err != nil {
			return nil, nil, configError{fmt.Errorf("env %s: %w", e.Name, err)}
		}
		if ok {
			set(e.Name, v)
		}
	}
	env := make([]string, 0, len(order))
	for _, name := range order {
		env = append(env, name+"="+vars[name])
	}
	return env, vars, nil
}

func (k *kubelet) envFromSource(ctx context.Context, ns string, from corev1.EnvFromSource) (map[string]string, error) {
	switch {
	case from.ConfigMapRef != nil:
		cm, err := k.cs.CoreV1().ConfigMaps(ns).Get(ctx, from.ConfigMapRef.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) && ptrTrue(from.ConfigMapRef.Optional) {
			return nil, nil
		}
		if err != nil {
			return nil, configError{err}
		}
		return cm.Data, nil
	case from.SecretRef != nil:
		s, err := k.cs.CoreV1().Secrets(ns).Get(ctx, from.SecretRef.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) && ptrTrue(from.SecretRef.Optional) {
			return nil, nil
		}
		if err != nil {
			return nil, configError{err}
		}
		data := map[string]string{}
		for key, v := range s.Data {
			data[key] = string(v)
		}
		return data, nil
	}
	return nil, nil
}

// envValue resolves a valueFrom; ok is false for an optional source that
// is missing, which sets nothing.
func (k *kubelet) envValue(ctx context.Context, p *corev1.Pod, c *corev1.Container, src *corev1.EnvVarSource) (v string, ok bool, err error) {
	switch {
	case src.FieldRef != nil:
		v, err = k.podField(p, src.FieldRef.FieldPath)
		return v, err == nil, err
	case src.ResourceFieldRef != nil:
		v, err = k.resourceField(c, src.ResourceFieldRef)
		return v, err == nil, err
	case src.ConfigMapKeyRef != nil:
		r := src.ConfigMapKeyRef
		cm, err := k.cs.CoreV1().ConfigMaps(p.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) && ptrTrue(r.Optional) {
				return "", false, nil
			}
			return "", false, err
		}
		v, ok = cm.Data[r.Key]
		if !ok && !ptrTrue(r.Optional) {
			return "", false, fmt.Errorf("ConfigMap %s has no key %s", r.Name, r.Key)
		}
		return v, ok, nil
	case src.SecretKeyRef != nil:
		r := src.SecretKeyRef
		s, err := k.cs.CoreV1().Secrets(p.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) && ptrTrue(r.Optional) {
				return "", false, nil
			}
			return "", false, err
		}
		b, ok := s.Data[r.Key]
		if !ok && !ptrTrue(r.Optional) {
			return "", false, fmt.Errorf("Secret %s has no key %s", r.Name, r.Key)
		}
		return string(b), ok, nil
	}
	return "", false, nil
}

// podField is the downward API: a field of the pod, by its path.
func (k *kubelet) podField(p *corev1.Pod, path string) (string, error) {
	if key, ok := strings.CutPrefix(path, "metadata.labels['"); ok {
		return p.Labels[strings.TrimSuffix(key, "']")], nil
	}
	if key, ok := strings.CutPrefix(path, "metadata.annotations['"); ok {
		return p.Annotations[strings.TrimSuffix(key, "']")], nil
	}
	switch path {
	case "metadata.name":
		return p.Name, nil
	case "metadata.namespace":
		return p.Namespace, nil
	case "metadata.uid":
		return string(p.UID), nil
	case "spec.nodeName":
		return p.Spec.NodeName, nil
	case "spec.serviceAccountName":
		return p.Spec.ServiceAccountName, nil
	case "status.hostIP", "status.podIP":
		return k.ip, nil
	}
	return "", fmt.Errorf("unsupported fieldPath %s", path)
}

// resourceField is a container's request or limit, in units of the
// divisor, rounded up; a limit that is unset is the node's capacity.
func (k *kubelet) resourceField(c *corev1.Container, r *corev1.ResourceFieldSelector) (string, error) {
	kind, name, _ := strings.Cut(r.Resource, ".")
	list := c.Resources.Limits
	if kind == "requests" {
		list = c.Resources.Requests
	}
	q, ok := list[corev1.ResourceName(name)]
	if !ok {
		q = k.capacity[corev1.ResourceName(name)]
	}
	divisor := r.Divisor
	if divisor.IsZero() {
		divisor = resource.MustParse("1")
	}
	return strconv.FormatInt(int64(math.Ceil(float64(q.MilliValue())/float64(divisor.MilliValue()))), 10), nil
}

func ptrTrue(b *bool) bool { return b != nil && *b }

// expand replaces $(NAME) with the variable's value when it is defined and
// leaves it as written when not; $$ is a literal $.
func expand(s string, vars map[string]string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '$' || i+1 == len(s) {
			b.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case '$':
			b.WriteByte('$')
			i++
		case '(':
			end := strings.IndexByte(s[i+2:], ')')
			if end < 0 {
				b.WriteByte('$')
				continue
			}
			name := s[i+2 : i+2+end]
			if v, ok := vars[name]; ok {
				b.WriteString(v)
			} else {
				b.WriteString(s[i : i+3+end])
			}
			i += 2 + end
		default:
			b.WriteByte('$')
		}
	}
	return b.String()
}

func expandAll(args []string, vars map[string]string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = expand(a, vars)
	}
	return out
}

// podDir is where the kubelet keeps a pod's volumes on the node.
func (k *kubelet) podDir(uid types.UID) string {
	return filepath.Join(k.root, "pods", string(uid))
}

// volumes makes p's volumes on the node and returns where each one is.
func (k *kubelet) volumes(ctx context.Context, w *worker, p *corev1.Pod) (map[string]string, error) {
	paths := map[string]string{}
	for _, v := range p.Spec.Volumes {
		switch {
		case v.EmptyDir != nil:
			dir := filepath.Join(k.podDir(p.UID), "volumes", "kubernetes.io~empty-dir", v.Name)
			if err := os.MkdirAll(dir, 0o777); err != nil {
				return nil, err
			}
			if v.EmptyDir.Medium == corev1.StorageMediumMemory && !mounted(dir) {
				if err := syscall.Mount("tmpfs", dir, "tmpfs", 0, ""); err != nil {
					return nil, fmt.Errorf("mount tmpfs for volume %s: %w", v.Name, err)
				}
			}
			paths[v.Name] = dir
		case v.ConfigMap != nil:
			dir := filepath.Join(k.podDir(p.UID), "volumes", "kubernetes.io~configmap", v.Name)
			files, err := k.configMapFiles(ctx, p.Namespace, v.ConfigMap)
			if err != nil {
				return nil, volumeError{fmt.Errorf("volume %s: %w", v.Name, err)}
			}
			if err := writeFiles(dir, files); err != nil {
				return nil, err
			}
			paths[v.Name] = dir
		case v.Secret != nil:
			dir := filepath.Join(k.podDir(p.UID), "volumes", "kubernetes.io~secret", v.Name)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
			if !mounted(dir) {
				if err := syscall.Mount("tmpfs", dir, "tmpfs", 0, ""); err != nil {
					return nil, fmt.Errorf("mount tmpfs for volume %s: %w", v.Name, err)
				}
			}
			files, err := k.secretFiles(ctx, p.Namespace, v.Secret)
			if err != nil {
				return nil, volumeError{fmt.Errorf("volume %s: %w", v.Name, err)}
			}
			if err := writeFiles(dir, files); err != nil {
				return nil, err
			}
			paths[v.Name] = dir
		case v.Projected != nil, v.DownwardAPI != nil:
			dir := filepath.Join(k.podDir(p.UID), "volumes", "kubernetes.io~projected", v.Name)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
			if !mounted(dir) {
				if err := syscall.Mount("tmpfs", dir, "tmpfs", 0, ""); err != nil {
					return nil, fmt.Errorf("mount tmpfs for volume %s: %w", v.Name, err)
				}
			}
			src := v.Projected
			if v.DownwardAPI != nil {
				src = &corev1.ProjectedVolumeSource{DefaultMode: v.DownwardAPI.DefaultMode, Sources: []corev1.VolumeProjection{{DownwardAPI: &corev1.DownwardAPIProjection{Items: v.DownwardAPI.Items}}}}
			}
			files, err := k.projectedFiles(ctx, w, p, v.Name, src)
			if err != nil {
				return nil, volumeError{fmt.Errorf("volume %s: %w", v.Name, err)}
			}
			if err := writeFiles(dir, files); err != nil {
				return nil, err
			}
			paths[v.Name] = dir
		case v.HostPath != nil:
			if t := v.HostPath.Type; t != nil && *t == corev1.HostPathDirectoryOrCreate {
				if err := os.MkdirAll(v.HostPath.Path, 0o755); err != nil {
					return nil, err
				}
			}
			paths[v.Name] = v.HostPath.Path
		default:
			return nil, configError{fmt.Errorf("volume %s is of a kind this kubelet does not support", v.Name)}
		}
	}
	return paths, nil
}

// mounted reports whether dir is a mount point: on another device than its
// parent.
func mounted(dir string) bool {
	var a, b syscall.Stat_t
	return syscall.Stat(dir, &a) == nil && syscall.Stat(filepath.Dir(dir), &b) == nil && a.Dev != b.Dev
}

// mounts are c's volume mounts as bind mounts of the volumes' paths.
func mounts(c *corev1.Container, paths map[string]string) ([]specs.Mount, error) {
	var out []specs.Mount
	for _, m := range c.VolumeMounts {
		src, ok := paths[m.Name]
		if !ok {
			return nil, configError{fmt.Errorf("volume mount %s names no volume", m.Name)}
		}
		mode := "rw"
		if m.ReadOnly {
			mode = "ro"
		}
		out = append(out, specs.Mount{
			Destination: m.MountPath,
			Type:        "bind",
			Source:      filepath.Join(src, m.SubPath),
			Options:     []string{"rbind", mode},
		})
	}
	return out, nil
}

// removeVolumes unmounts a pod's in-memory emptyDirs and removes its
// directory. A hostPath is the node's, so it is never under it.
func (k *kubelet) removeVolumes(uid types.UID) error {
	dirs, _ := filepath.Glob(filepath.Join(k.podDir(uid), "volumes", "*", "*"))
	for _, d := range dirs {
		if mounted(d) {
			if err := syscall.Unmount(d, syscall.MNT_DETACH); err != nil {
				return err
			}
		}
	}
	return os.RemoveAll(k.podDir(uid))
}

// volumeError is a volume that cannot be set up yet.
type volumeError struct{ error }

// file is one file of a ConfigMap or Secret volume.
type file struct {
	data []byte
	mode os.FileMode
}

func (k *kubelet) configMapFiles(ctx context.Context, ns string, src *corev1.ConfigMapVolumeSource) (map[string]file, error) {
	cm, err := k.cs.CoreV1().ConfigMaps(ns).Get(ctx, src.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && ptrTrue(src.Optional) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data := map[string][]byte{}
	for key, v := range cm.Data {
		data[key] = []byte(v)
	}
	maps.Copy(data, cm.BinaryData)
	return keyFiles(data, src.Items, src.DefaultMode)
}

func (k *kubelet) secretFiles(ctx context.Context, ns string, src *corev1.SecretVolumeSource) (map[string]file, error) {
	s, err := k.cs.CoreV1().Secrets(ns).Get(ctx, src.SecretName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && ptrTrue(src.Optional) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return keyFiles(s.Data, src.Items, src.DefaultMode)
}

// keyFiles lays an object's keys out as files: every key under its own
// name, or only the items, each at its path.
func keyFiles(data map[string][]byte, items []corev1.KeyToPath, defaultMode *int32) (map[string]file, error) {
	mode := os.FileMode(0o644)
	if defaultMode != nil {
		mode = os.FileMode(*defaultMode)
	}
	out := map[string]file{}
	if len(items) == 0 {
		for key, v := range data {
			out[key] = file{v, mode}
		}
		return out, nil
	}
	for _, it := range items {
		v, ok := data[it.Key]
		if !ok {
			return nil, fmt.Errorf("no key %s", it.Key)
		}
		m := mode
		if it.Mode != nil {
			m = os.FileMode(*it.Mode)
		}
		out[it.Path] = file{v, m}
	}
	return out, nil
}

// writeFiles makes dir hold exactly files. Each is written to a temporary
// name and renamed into place, so a reader never sees half a file.
// ponytail: per-file swaps; the real kubelet swaps a ..data symlink so all
// files change at once.
func writeFiles(dir string, files map[string]file) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for name, f := range files {
		path := filepath.Join(dir, name)
		if old, err := os.ReadFile(path); err == nil && bytes.Equal(old, f.data) {
			if st, err := os.Stat(path); err == nil && st.Mode().Perm() == f.mode {
				continue
			}
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, f.data, f.mode); err != nil {
			return err
		}
		if err := os.Chmod(tmp, f.mode); err != nil {
			return err
		}
		if err := os.Rename(tmp, path); err != nil {
			return err
		}
	}
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if _, ok := files[rel]; !ok {
			return os.Remove(path)
		}
		return nil
	})
}

// projectedFiles gathers a projected volume's sources into one set of files.
func (k *kubelet) projectedFiles(ctx context.Context, w *worker, p *corev1.Pod, volume string, src *corev1.ProjectedVolumeSource) (map[string]file, error) {
	mode := os.FileMode(0o644)
	if src.DefaultMode != nil {
		mode = os.FileMode(*src.DefaultMode)
	}
	out := map[string]file{}
	for _, s := range src.Sources {
		var files map[string]file
		var err error
		switch {
		case s.ServiceAccountToken != nil:
			var t string
			t, err = k.token(ctx, w, p, volume, s.ServiceAccountToken)
			files = map[string]file{s.ServiceAccountToken.Path: {[]byte(t), mode}}
		case s.ConfigMap != nil:
			files, err = k.configMapFiles(ctx, p.Namespace, &corev1.ConfigMapVolumeSource{LocalObjectReference: s.ConfigMap.LocalObjectReference, Items: s.ConfigMap.Items, Optional: s.ConfigMap.Optional, DefaultMode: ptr(int32(mode))})
		case s.Secret != nil:
			files, err = k.secretFiles(ctx, p.Namespace, &corev1.SecretVolumeSource{SecretName: s.Secret.Name, Items: s.Secret.Items, Optional: s.Secret.Optional, DefaultMode: ptr(int32(mode))})
		case s.DownwardAPI != nil:
			files = map[string]file{}
			for _, it := range s.DownwardAPI.Items {
				if it.FieldRef == nil {
					continue
				}
				v, ferr := k.podField(p, it.FieldRef.FieldPath)
				if ferr != nil {
					return nil, ferr
				}
				m := mode
				if it.Mode != nil {
					m = os.FileMode(*it.Mode)
				}
				files[it.Path] = file{[]byte(v), m}
			}
		}
		if err != nil {
			return nil, err
		}
		maps.Copy(out, files)
	}
	return out, nil
}

// token is a service account token for the pod, bound to it so it dies
// with the pod, requested again once 80% of its life is gone.
func (k *kubelet) token(ctx context.Context, w *worker, p *corev1.Pod, volume string, src *corev1.ServiceAccountTokenProjection) (string, error) {
	key := volume + "/" + src.Path
	if t, ok := w.tokens[key]; ok && time.Now().Before(t.refreshAt) {
		return t.value, nil
	}
	expiry := int64(3600)
	if src.ExpirationSeconds != nil {
		expiry = *src.ExpirationSeconds
	}
	req := &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{
		ExpirationSeconds: &expiry,
		BoundObjectRef:    &authenticationv1.BoundObjectReference{Kind: "Pod", APIVersion: "v1", Name: p.Name, UID: p.UID},
	}}
	if src.Audience != "" {
		req.Spec.Audiences = []string{src.Audience}
	}
	sa := p.Spec.ServiceAccountName
	if sa == "" {
		sa = "default"
	}
	got, err := k.cs.CoreV1().ServiceAccounts(p.Namespace).CreateToken(ctx, sa, req, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	life := got.Status.ExpirationTimestamp.Sub(time.Now())
	w.tokens[key] = token{got.Status.Token, time.Now().Add(life * 8 / 10)}
	return got.Status.Token, nil
}
