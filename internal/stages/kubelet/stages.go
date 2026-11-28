// Package kubelet holds the assertions for the "Build your own kubelet"
// course — one function per stage, registered by slug.
//
// The learner's program is built for Linux and runs inside a kind worker
// through runner.Node, as a second kubelet on that machine. It registers a
// Node of its own under a name the stage picks, so the worker's real kubelet
// and node are never touched, and the stage deletes that Node when it ends.
// Its pods run in a containerd namespace named for that Node, never k8s.io:
// the real kubelet kills CRI sandboxes it did not create, and never looks
// outside CRI. Every stage removes that namespace when it ends.
package kubelet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/madhank93/byo-k8s-x/internal/cluster"
	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

var stageRegistry = map[string]Stage{}

func register(s Stage) { stageRegistry[s.Slug] = s }

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := stageRegistry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "node-register", Run: stageNodeRegister})
	register(Stage{Slug: "node-status", Run: stageNodeStatus})
	register(Stage{Slug: "node-lease", Run: stageNodeLease})
	register(Stage{Slug: "run-container", Run: stageRunContainer})
	register(Stage{Slug: "pod-status", Run: stagePodStatus})
	register(Stage{Slug: "graceful-delete", Run: stageGracefulDelete})
	register(Stage{Slug: "restart-policy", Run: stageRestartPolicy})
	register(Stage{Slug: "init-containers", Run: stageInitContainers})
	register(Stage{Slug: "env", Run: stageEnv})
	register(Stage{Slug: "volumes", Run: stageVolumes})
	register(Stage{Slug: "config-volumes", Run: stageConfigVolumes})
	register(Stage{Slug: "service-account-token", Run: stageServiceAccountToken})
	register(Stage{Slug: "resources", Run: stageResources})
	register(Stage{Slug: "liveness-probe", Run: stageLivenessProbe})
	register(Stage{Slug: "readiness-probe", Run: stageReadinessProbe})
	register(Stage{Slug: "lifecycle-hooks", Run: stageLifecycleHooks})
	register(Stage{Slug: "static-pod", Run: stageStaticPod})
	register(Stage{Slug: "reconcile", Run: stageReconcile})
	register(Stage{Slug: "eviction", Run: stageEviction})
	register(Stage{Slug: "logs-api", Run: stageLogsAPI})
}

// node is the worker the program runs in.
const node = runner.Node(cluster.Name + "-worker")

// namePrefix starts every Node a stage registers, so one a killed run left
// behind can be found and removed.
const namePrefix = "bko-kubelet-"

// taint keeps the scheduler from placing other courses' pods on a Node whose
// kubelet only lives for one stage.
const taint = "byok8s.dev/kubelet=learner:NoSchedule"

// work is one stage's fixtures: the program running in the node as kubelet
// for the Node called name, keeping its files under root and running its
// pods in the containerd namespace of the same name.
type work struct {
	ctx  context.Context
	env  *kube.Env
	name string
	root string
	port int
	p    *runner.Process
}

// start installs the program, hands it a kubeconfig, and starts it as the
// kubelet for a freshly named Node.
func start(ctx context.Context, env *kube.Env, bin string) (*work, error) {
	if err := sweepNodes(ctx, env); err != nil {
		return nil, err
	}
	if err := node.Install(ctx, bin); err != nil {
		return nil, err
	}
	w := &work{ctx: ctx, env: env, name: namePrefix + randHex(4)}
	w.root = "/var/lib/byok8s-kubelet/" + w.name
	w.port = 12000 + int(randByte())*4
	data, err := nodeKubeconfig(env)
	if err != nil {
		return nil, err
	}
	if err := node.WriteFile(ctx, w.root+"/kubeconfig", data, 0o600); err != nil {
		return nil, err
	}
	return w, w.launch()
}

// launch starts the program as the kubelet for w.name.
func (w *work) launch() (err error) {
	w.p, err = node.Start(w.ctx, nil,
		"--kubeconfig", w.root+"/kubeconfig",
		"--hostname-override", w.name,
		"--register-with-taints", taint,
		"--root-dir", w.root,
		"--containerd-namespace", w.name,
		"--pod-manifest-path", w.root+"/manifests",
		"--port", strconv.Itoa(w.port))
	return err
}

// close stops the program and removes its Node, the Lease, any pod bound to
// it — forcibly: nothing will be left to finish a graceful delete — and every
// container it left in containerd.
func (w *work) close() {
	w.p.Stop(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	removeNode(ctx, w.env, w.name)
}

func removeNode(ctx context.Context, env *kube.Env, name string) {
	cs := env.Client
	pods, _ := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", name).String()})
	if pods != nil {
		for _, p := range pods.Items {
			cs.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)})
		}
	}
	cs.CoordinationV1().Leases("kube-node-lease").Delete(ctx, name, metav1.DeleteOptions{})
	cs.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
	node.Exec(ctx, 60*time.Second, "sh", "-c", teardown, "teardown", name)
	node.Exec(ctx, 30*time.Second, "rm", "-rf", "/var/lib/byok8s-kubelet/"+name)
}

// teardown empties containerd namespace $1 — tasks, containers, images,
// snapshots, leases, content, in that order — and removes it.
const teardown = `ns=$1; c="ctr -n $ns"
ctr ns ls -q | grep -qx "$ns" || exit 0
for t in $($c t ls -q); do $c t rm -f "$t" >/dev/null; done
$c c ls -q | xargs -r $c c rm
$c i ls -q | xargs -r $c i rm >/dev/null
for i in 1 2 3 4 5 6; do $c snapshots ls | awk 'NR>1{print $1}' | xargs -r $c snapshots rm 2>/dev/null; done
$c leases ls -q | xargs -r $c leases rm
$c content ls -q | xargs -r $c content rm >/dev/null
ctr ns rm "$ns" >/dev/null`

// sweepNodes removes course Nodes whose kubelet has stopped heartbeating:
// what a killed run leaves. A live run's Node stays Ready.
func sweepNodes(ctx context.Context, env *kube.Env) error {
	nodes, err := env.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	live := map[string]bool{}
	for _, n := range nodes.Items {
		if !strings.HasPrefix(n.Name, namePrefix) {
			continue
		}
		if ready(&n) {
			live[n.Name] = true
		} else {
			removeNode(ctx, env, n.Name)
		}
	}
	// A containerd namespace can outlive its Node when a run dies mid-close.
	res, err := node.Exec(ctx, 30*time.Second, "ctr", "ns", "ls", "-q")
	if err != nil {
		return err
	}
	for _, ns := range strings.Fields(res.Stdout) {
		if strings.HasPrefix(ns, namePrefix) && !live[ns] {
			removeNode(ctx, env, ns)
		}
	}
	return nil
}

func ready(n *corev1.Node) bool {
	c := condition(n, corev1.NodeReady)
	return c != nil && c.Status == corev1.ConditionTrue
}

func condition(n *corev1.Node, t corev1.NodeConditionType) *corev1.NodeCondition {
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == t {
			return &n.Status.Conditions[i]
		}
	}
	return nil
}

// nodeKubeconfig is the harness's own credentials, addressed to the API
// server the way a node reaches it over the kind network.
func nodeKubeconfig(env *kube.Env) ([]byte, error) {
	tls := env.Config.TLSClientConfig
	if len(tls.CAData) == 0 || len(tls.CertData) == 0 || len(tls.KeyData) == 0 {
		return nil, fmt.Errorf("the %s kubeconfig does not embed its certificates; recreate the cluster with byok8s up", cluster.Context)
	}
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["byok8s"] = &clientcmdapi.Cluster{Server: "https://" + cluster.Name + "-control-plane:6443", CertificateAuthorityData: tls.CAData}
	cfg.AuthInfos["byok8s"] = &clientcmdapi.AuthInfo{ClientCertificateData: tls.CertData, ClientKeyData: tls.KeyData}
	cfg.Contexts["byok8s"] = &clientcmdapi.Context{Cluster: "byok8s", AuthInfo: "byok8s"}
	cfg.CurrentContext = "byok8s"
	return clientcmd.Write(*cfg)
}

// alive fails if the program has exited, naming how.
func (w *work) alive() error {
	if exited, res := w.p.Exited(); exited {
		return fmt.Errorf("the kubelet exited %d; it should keep running until it is stopped\nstdout:\n%s\nstderr:\n%s", res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return nil
}

// waitNode waits until the program's Node satisfies ok, reporting why not.
func (w *work) waitNode(what string, timeout time.Duration, ok func(*corev1.Node) error) (*corev1.Node, error) {
	ctx, cancel := context.WithTimeout(w.ctx, timeout)
	defer cancel()
	var last error
	var got *corev1.Node
	err := kube.WaitFor(ctx, what, func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		n, err := w.env.Client.CoreV1().Nodes().Get(ctx, w.name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			last = fmt.Errorf("there is no Node %s", w.name)
			return false, nil
		}
		if err != nil {
			return false, nil
		}
		got = n
		last = ok(n)
		return last == nil, nil
	})
	if err != nil && last != nil {
		return got, fmt.Errorf("%w: %v", err, last)
	}
	return got, err
}

func registered(name string) func(*corev1.Node) error {
	return func(n *corev1.Node) error {
		if n.Labels[corev1.LabelHostname] != name {
			return fmt.Errorf("label %s is %q; it should be the node name %s", corev1.LabelHostname, n.Labels[corev1.LabelHostname], name)
		}
		if n.Labels[corev1.LabelArchStable] == "" {
			return fmt.Errorf("label %s is missing; set it to the architecture the kubelet runs on", corev1.LabelArchStable)
		}
		if !slices.ContainsFunc(n.Spec.Taints, func(t corev1.Taint) bool {
			return t.ToString() == taint
		}) {
			return fmt.Errorf("the Node's taints are %v; --register-with-taints asked for %s", n.Spec.Taints, taint)
		}
		return nil
	}
}

// stageNodeRegister — the kubelet creates its Node, labelled with its name
// and architecture and carrying the taints it was asked to register with,
// and a kubelet restarted onto a Node that already exists keeps running.
func stageNodeRegister(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	n, err := w.waitNode("Node "+w.name+" to be registered", 30*time.Second, registered(w.name))
	if err != nil {
		return err
	}
	arch, err := runner.NodeArch(ctx)
	if err != nil {
		return err
	}
	if got := n.Labels[corev1.LabelArchStable]; got != arch {
		return fmt.Errorf("label %s is %q; the node is %s (runtime.GOARCH)", corev1.LabelArchStable, got, arch)
	}

	// A restart finds its Node already there; that is not an error.
	uid := n.UID
	w.p.Stop(5 * time.Second)
	if err := w.launch(); err != nil {
		return err
	}
	time.Sleep(5 * time.Second)
	if err := w.alive(); err != nil {
		return fmt.Errorf("restarted with Node %s already registered: %w", w.name, err)
	}
	n, err = env.Client.CoreV1().Nodes().Get(ctx, w.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if n.UID != uid {
		return fmt.Errorf("the restarted kubelet deleted and recreated Node %s; an existing Node is reused", w.name)
	}
	return nil
}

// stageNodeStatus — the kubelet reports what the machine is: Ready, its
// capacity and allocatable, its address and system info, and a heartbeat
// that keeps advancing.
func stageNodeStatus(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	n, err := w.waitNode("Node "+w.name+" to report Ready", 30*time.Second, func(n *corev1.Node) error {
		if !ready(n) {
			return fmt.Errorf("its Ready condition is %+v; the kubelet should set it True", condition(n, corev1.NodeReady))
		}
		return nil
	})
	if err != nil {
		return err
	}

	facts, err := w.machine()
	if err != nil {
		return err
	}
	for _, r := range []struct {
		name corev1.ResourceName
		want resource.Quantity
	}{
		{corev1.ResourceCPU, resource.MustParse(facts["cpus"])},
		{corev1.ResourceMemory, resource.MustParse(facts["memKi"] + "Ki")},
		{corev1.ResourcePods, resource.MustParse("110")},
	} {
		for what, list := range map[string]corev1.ResourceList{"capacity": n.Status.Capacity, "allocatable": n.Status.Allocatable} {
			got, ok := list[r.name]
			if !ok || got.Cmp(r.want) != 0 {
				return fmt.Errorf("status.%s.%s is %q; the node has %s (pods: 110, the kubelet's default)", what, r.name, got.String(), r.want.String())
			}
		}
	}
	addrs := map[corev1.NodeAddressType]string{}
	for _, a := range n.Status.Addresses {
		addrs[a.Type] = a.Address
	}
	if addrs[corev1.NodeInternalIP] != facts["ip"] || addrs[corev1.NodeHostName] != w.name {
		return fmt.Errorf("status.addresses is %v; expected InternalIP %s (what the machine's hostname resolves to) and Hostname %s", n.Status.Addresses, facts["ip"], w.name)
	}
	info := n.Status.NodeInfo
	arch, _ := runner.NodeArch(ctx)
	for _, c := range [][3]string{
		{"kernelVersion", info.KernelVersion, facts["kernel"]},
		{"osImage", info.OSImage, facts["osImage"]},
		{"operatingSystem", info.OperatingSystem, "linux"},
		{"architecture", info.Architecture, arch},
	} {
		if c[1] != c[2] {
			return fmt.Errorf("status.nodeInfo.%s is %q; expected %q", c[0], c[1], c[2])
		}
	}

	first := condition(n, corev1.NodeReady).LastHeartbeatTime
	since := condition(n, corev1.NodeReady).LastTransitionTime
	_, err = w.waitNode("the Ready heartbeat to advance", 25*time.Second, func(n *corev1.Node) error {
		c := condition(n, corev1.NodeReady)
		if c == nil || !c.LastHeartbeatTime.After(first.Time) {
			return fmt.Errorf("Ready's lastHeartbeatTime is still %s; post status again every 10 seconds", first.UTC().Format(time.RFC3339))
		}
		if !c.LastTransitionTime.Equal(&since) {
			return fmt.Errorf("Ready's lastTransitionTime moved from %s to %s while Ready stayed True; it changes only when the status does", since.UTC().Format(time.RFC3339), c.LastTransitionTime.UTC().Format(time.RFC3339))
		}
		return nil
	})
	return err
}

// machine is what the node itself says about its cpus, memory, address,
// kernel and OS.
func (w *work) machine() (map[string]string, error) {
	res, err := node.Exec(w.ctx, 30*time.Second, "sh", "-c", `. /etc/os-release
echo "cpus=$(nproc)"
echo "memKi=$(awk '/^MemTotal:/{print $2}' /proc/meminfo)"
echo "ip=$(getent ahostsv4 $(hostname) | awk 'NR==1{print $1}')"
echo "kernel=$(uname -r)"
echo "osImage=$PRETTY_NAME"`)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("read the machine's facts in node %s: %s", node, res.Stderr)
	}
	facts := map[string]string{}
	for _, l := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		k, v, _ := strings.Cut(l, "=")
		facts[k] = v
	}
	return facts, nil
}

// stageNodeLease — the kubelet holds a Lease named for its Node in
// kube-node-lease, owned by the Node, and renews it well inside its duration.
func stageNodeLease(ctx context.Context, env *kube.Env, bin string) error {
	w, err := start(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	n, err := w.waitNode("Node "+w.name+" to be registered", 30*time.Second, registered(w.name))
	if err != nil {
		return err
	}
	leases := env.Client.CoordinationV1().Leases("kube-node-lease")
	var l *coordinationv1.Lease
	var why error
	if err := kube.WaitFor(ctx, "Lease kube-node-lease/"+w.name, func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		l, why = leases.Get(ctx, w.name, metav1.GetOptions{})
		return why == nil, nil
	}); err != nil {
		return fmt.Errorf("%w: %v", err, why)
	}
	switch s := l.Spec; {
	case s.HolderIdentity == nil || *s.HolderIdentity != w.name:
		return fmt.Errorf("the Lease's holderIdentity is %v; it is held by the node, %s", ptrString(s.HolderIdentity), w.name)
	case s.LeaseDurationSeconds == nil || *s.LeaseDurationSeconds != 40:
		return fmt.Errorf("the Lease's leaseDurationSeconds is %v; the kubelet's default is 40", ptrInt(s.LeaseDurationSeconds))
	case s.RenewTime == nil:
		return fmt.Errorf("the Lease has no renewTime")
	}
	if !slices.ContainsFunc(l.OwnerReferences, func(o metav1.OwnerReference) bool {
		return o.Kind == "Node" && o.Name == w.name && o.UID == n.UID
	}) {
		return fmt.Errorf("the Lease's ownerReferences are %v; the Node owns it, so deleting the Node deletes the Lease", l.OwnerReferences)
	}

	// Renewed at a quarter of the duration, two renewals land inside 25s.
	seen := []time.Time{l.Spec.RenewTime.Time}
	ctx2, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if err := kube.WaitFor(ctx2, "the Lease to be renewed twice", func(ctx context.Context) (bool, error) {
		if err := w.alive(); err != nil {
			return false, err
		}
		l, err := leases.Get(ctx, w.name, metav1.GetOptions{})
		if err != nil || l.Spec.RenewTime == nil {
			return false, nil
		}
		if t := l.Spec.RenewTime.Time; t.After(seen[len(seen)-1]) {
			seen = append(seen, t)
		}
		return len(seen) >= 3, nil
	}); err != nil {
		return fmt.Errorf("%w: renewTime took the values %v; renew every 10 seconds, a quarter of the 40-second duration", err, seen)
	}
	return nil
}

func ptrString(p *string) string {
	if p == nil {
		return "unset"
	}
	return strconv.Quote(*p)
}

func ptrInt(p *int32) string {
	if p == nil {
		return "unset"
	}
	return strconv.Itoa(int(*p))
}

func tail(s string) string {
	ls := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(ls) > 40 {
		ls = append([]string{"…"}, ls[len(ls)-40:]...)
	}
	return "  " + strings.Join(ls, "\n  ")
}

func randByte() byte {
	b := make([]byte, 1)
	rand.Read(b)
	return b[0]
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
