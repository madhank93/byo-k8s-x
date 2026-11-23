package kubeproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/madhank93/byo-k8s-x/internal/cluster"
	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
)

const (
	// proxyName is the value of proxyLabel that marks a Service as this
	// program's. The real kube-proxy skips every Service carrying the label,
	// which is what lets the two run on one node without fighting.
	proxyName  = "byok8s"
	proxyLabel = "service.kubernetes.io/service-proxy-name"

	// table is the nftables table the course tells the program to own.
	table = "byok8s"

	// nodeKubeconfig is where the program finds the cluster inside the node.
	nodeKubeconfig = "/etc/byok8s/kubeconfig"

	image = "busybox:1.36"
)

// Three roles, three workers. The program runs on proxyNode only, so a client
// that should go through it sits there too; remoteNode is "somewhere else in
// the cluster" for clients reaching a node port, and farNode holds backends
// that are neither the client's nor the proxy's.
var (
	proxyNode  = cluster.Name + "-worker2"
	remoteNode = cluster.Name + "-worker3"
	farNode    = cluster.Name + "-worker"
	node       = runner.Node(proxyNode)
)

var (
	installMu sync.Mutex
	installed string
)

// launch starts the program inside proxyNode with a kubeconfig that reaches
// the API server over the kind network. The cleanup stops it and deletes its
// table, so a stage never leaves rules on a node that other courses share.
// before runs once the node is clean and before the program starts.
func launch(ctx context.Context, env *kube.Env, bin string, before ...func(context.Context) error) (*runner.Process, func(), error) {
	installMu.Lock()
	if installed != bin {
		if err := node.Install(ctx, bin); err != nil {
			installMu.Unlock()
			return nil, nil, err
		}
		installed = bin
	}
	installMu.Unlock()
	if err := node.Sweep(ctx); err != nil {
		return nil, nil, err
	}
	if err := writeKubeconfig(ctx); err != nil {
		return nil, nil, err
	}
	dropTable(ctx)
	for _, b := range before {
		if err := b(ctx); err != nil {
			dropTable(ctx)
			return nil, nil, err
		}
	}
	p, err := node.Start(ctx, []string{"KUBECONFIG=" + nodeKubeconfig})
	if err != nil {
		return nil, nil, err
	}
	return p, func() {
		p.Stop(5 * time.Second)
		dropTable(context.WithoutCancel(ctx))
	}, nil
}

// writeKubeconfig copies the harness's own credentials into the node, pointed
// at the control plane by the name kind gives it on its network.
func writeKubeconfig(ctx context.Context) error {
	raw, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).RawConfig()
	if err != nil {
		return fmt.Errorf("read kubeconfig: %w", err)
	}
	raw.CurrentContext = cluster.Context
	if err := clientcmdapi.FlattenConfig(&raw); err != nil {
		return fmt.Errorf("flatten kubeconfig: %w", err)
	}
	min := raw.DeepCopy()
	if err := clientcmdapi.MinifyConfig(min); err != nil {
		return fmt.Errorf("minify kubeconfig: %w", err)
	}
	for _, c := range min.Clusters {
		c.Server = "https://" + cluster.Name + "-control-plane:6443"
	}
	data, err := clientcmd.Write(*min)
	if err != nil {
		return err
	}
	return node.WriteFile(ctx, nodeKubeconfig, data, 0o600)
}

// dropTable deletes the program's table if it is there. The harness owns the
// name for the length of a stage, and touches no other table.
func dropTable(ctx context.Context) {
	_, _ = node.Exec(ctx, 10*time.Second, "nft", "delete", "table", "ip", table)
}

// nftApply feeds a ruleset to nft in the proxy node.
func nftApply(ctx context.Context, ruleset string) error {
	res, err := node.Exec(ctx, 15*time.Second, "sh", "-c", "printf '%s' \"$0\" | nft -f -", ruleset)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("nft -f: %s", strings.TrimSpace(res.Stderr))
	}
	return nil
}

// nft runs nft in the proxy node and returns its stdout.
func nft(ctx context.Context, args ...string) (string, int, error) {
	res, err := node.Exec(ctx, 15*time.Second, append([]string{"nft"}, args...)...)
	if err != nil {
		return "", -1, err
	}
	return res.Stdout, res.ExitCode, nil
}

// nftObject is one entry of `nft -j list ...`: exactly one field is set.
type nftObject struct {
	Table *struct {
		Name   string `json:"name"`
		Handle int    `json:"handle"`
	} `json:"table"`
	Chain *struct {
		Name   string `json:"name"`
		Handle int    `json:"handle"`
		Type   string `json:"type"`
		Hook   string `json:"hook"`
	} `json:"chain"`
	Rule *struct {
		Chain  string          `json:"chain"`
		Handle int             `json:"handle"`
		Expr   json.RawMessage `json:"expr"`
	} `json:"rule"`
}

// tableObjects lists the program's table as nft's JSON, or nil when there is
// no such table.
func tableObjects(ctx context.Context) ([]nftObject, error) {
	out, code, err := nft(ctx, "-j", "list", "table", "ip", table)
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, nil
	}
	var doc struct {
		Nftables []nftObject `json:"nftables"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		return nil, fmt.Errorf("parse nft -j output: %w", err)
	}
	return doc.Nftables, nil
}

// fixture is a stage's own pods and Services; done deletes them all, so a
// passing stage leaves no backends running on the shared cluster.
type fixture struct {
	env *kube.Env
}

func (f fixture) done(ctx context.Context) {
	ctx = context.WithoutCancel(ctx)
	zero := int64(0)
	_ = f.env.Client.CoreV1().Pods(f.env.Namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{GracePeriodSeconds: &zero}, metav1.ListOptions{})
	list, err := f.env.Client.CoreV1().Services(f.env.Namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, s := range list.Items {
			_ = f.env.Client.CoreV1().Services(f.env.Namespace).Delete(ctx, s.Name, metav1.DeleteOptions{})
		}
	}
	_ = f.env.Client.DiscoveryV1().EndpointSlices(f.env.Namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "endpointslice.kubernetes.io/managed-by=" + graderManager})
}

// graderManager marks EndpointSlices the grader writes by hand.
const graderManager = "byok8s-grader"

// backendScript serves the same answer over HTTP on 8080 and 8081 and over
// UDP on 9000: the pod's name, the address the request came from, and the
// port it arrived on. Those three are everything a stage judges a proxy by.
const backendScript = `mkdir -p /www/cgi-bin
printf '#!/bin/sh\necho Content-Type: text/plain\necho\necho "$(hostname) $REMOTE_ADDR $SERVER_PORT"\n' > /www/cgi-bin/id
chmod +x /www/cgi-bin/id
httpd -p 8080 -h /www
httpd -p 8081 -h /www
while true; do nc -u -l -p 9000 -e hostname; done`

// backend is a pod answering on 8080, 8081 and udp/9000, pinned to a node
// so no scheduler decides where traffic has to travel.
func backend(name, nodeName string, labels map[string]string) *corev1.Pod {
	zero := int64(0)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec: corev1.PodSpec{
			NodeName:                      nodeName,
			TerminationGracePeriodSeconds: &zero,
			Containers: []corev1.Container{{
				Name:            "web",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sh", "-c", backendScript},
			}},
		},
	}
}

// client is a pod that only exists to have commands run in it.
func client(name, nodeName string, hostNetwork bool) *corev1.Pod {
	zero := int64(0)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PodSpec{
			NodeName:                      nodeName,
			HostNetwork:                   hostNetwork,
			TerminationGracePeriodSeconds: &zero,
			Containers: []corev1.Container{{
				Name:            "client",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sleep", "3600"},
			}},
		},
	}
}

// create makes pods and waits for every one of them to be Ready.
func (f fixture) create(ctx context.Context, pods ...*corev1.Pod) error {
	for _, p := range pods {
		if _, err := f.env.Client.CoreV1().Pods(f.env.Namespace).Create(ctx, p, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create pod %s: %w", p.Name, err)
		}
	}
	for _, p := range pods {
		if err := f.ready(ctx, p.Name); err != nil {
			return err
		}
	}
	return nil
}

func (f fixture) ready(ctx context.Context, name string) error {
	return waitFor(ctx, "pod "+name+" to be ready", 90*time.Second, func(ctx context.Context) (bool, error) {
		p, err := f.env.Client.CoreV1().Pods(f.env.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
}

func (f fixture) pod(ctx context.Context, name string) (*corev1.Pod, error) {
	return f.env.Client.CoreV1().Pods(f.env.Namespace).Get(ctx, name, metav1.GetOptions{})
}

// deletePod deletes a pod; grace nil means immediately.
func (f fixture) deletePod(ctx context.Context, name string, grace *int64) error {
	if grace == nil {
		zero := int64(0)
		grace = &zero
	}
	err := f.env.Client.CoreV1().Pods(f.env.Namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: grace})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// service creates a Service this program owns: labelled for it, selecting
// app=<name>, port 80 on to 8080 unless mutate says otherwise.
func (f fixture) service(ctx context.Context, name string, mutate func(*corev1.Service)) (*corev1.Service, error) {
	s := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{proxyLabel: proxyName}},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": name},
			Ports: []corev1.ServicePort{{
				Name: "http", Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8080),
			}},
		},
	}
	if mutate != nil {
		mutate(s)
	}
	out, err := f.env.Client.CoreV1().Services(f.env.Namespace).Create(ctx, s, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create service %s: %w", name, err)
	}
	return out, nil
}

// slice writes an EndpointSlice by hand, labelled the way the EndpointSlice
// controller labels the slices it makes for a Service of this program's.
func (f fixture) slice(ctx context.Context, name, svc string, eps []discoveryv1.Endpoint) error {
	port := int32(8080)
	proto := corev1.ProtocolTCP
	pname := "http"
	s := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
			discoveryv1.LabelServiceName: svc,
			discoveryv1.LabelManagedBy:   graderManager,
			proxyLabel:                   proxyName,
		}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   eps,
		Ports:       []discoveryv1.EndpointPort{{Name: &pname, Port: &port, Protocol: &proto}},
	}
	cur, err := f.env.Client.DiscoveryV1().EndpointSlices(f.env.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = f.env.Client.DiscoveryV1().EndpointSlices(f.env.Namespace).Create(ctx, s, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	s.ResourceVersion = cur.ResourceVersion
	_, err = f.env.Client.DiscoveryV1().EndpointSlices(f.env.Namespace).Update(ctx, s, metav1.UpdateOptions{})
	return err
}

func endpoint(ip string, ready bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{Addresses: []string{ip}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}}
}

// nodeIP is a node's InternalIP.
func (f fixture) nodeIP(ctx context.Context, name string) (string, error) {
	n, err := f.env.Client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for _, a := range n.Status.Addresses {
		if a.Type == corev1.NodeInternalIP && !strings.Contains(a.Address, ":") {
			return a.Address, nil
		}
	}
	return "", fmt.Errorf("node %s has no IPv4 InternalIP", name)
}

// exec runs a command in a pod and returns what it printed, stdout and stderr
// together, and whether it exited zero.
func (f fixture) exec(ctx context.Context, pod string, cmd ...string) (string, error) {
	req := f.env.Client.CoreV1().RESTClient().Post().Resource("pods").
		Namespace(f.env.Namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Command: cmd, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	ex, err := remotecommand.NewSPDYExecutor(f.env.Config, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	err = ex.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &out, Stderr: &out})
	return out.String(), err
}

// answers is what a batch of requests came back with.
type answers struct {
	by     map[string]int    // pod name -> requests it answered
	from   map[string]string // pod name -> the client address it saw
	port   map[string]string // pod name -> the port the request arrived on
	failed int
	errs   []string
	n      int
}

func (a answers) pods() []string {
	var out []string
	for p := range a.by {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func (a answers) String() string {
	var parts []string
	for _, p := range a.pods() {
		parts = append(parts, fmt.Sprintf("%s=%d", p, a.by[p]))
	}
	if a.failed > 0 {
		parts = append(parts, fmt.Sprintf("failed=%d", a.failed))
	}
	s := fmt.Sprintf("%d requests: %s", a.n, strings.Join(parts, " "))
	if len(a.errs) > 0 {
		s += " (" + a.errs[0] + ")"
	}
	return s
}

// get sends n HTTP requests from a client pod, each on its own connection,
// and tallies who answered.
func (f fixture) get(ctx context.Context, from, host string, port int32, n int) (answers, error) {
	url := fmt.Sprintf("http://%s:%d/cgi-bin/id", host, port)
	script := fmt.Sprintf(`for i in $(seq %d); do wget -qO- -T 2 %s 2>&1 || echo FAIL; done`, n, url)
	return f.tally(ctx, from, script, n)
}

// udp sends n datagrams from a client pod, from a fixed source port when
// sport is non-zero, and tallies who answered.
func (f fixture) udp(ctx context.Context, from, host string, port int32, n, sport int) (answers, error) {
	src := ""
	if sport != 0 {
		src = fmt.Sprintf("-p %d ", sport)
	}
	script := fmt.Sprintf(`for i in $(seq %d); do r=$(echo x | nc -u -w1 %s%s %d 2>&1); [ -n "$r" ] && echo "$r" || echo FAIL; done`, n, src, host, port)
	return f.tally(ctx, from, script, n)
}

func (f fixture) tally(ctx context.Context, from, script string, n int) (answers, error) {
	out, err := f.exec(ctx, from, "sh", "-c", script)
	a := answers{by: map[string]int{}, from: map[string]string{}, port: map[string]string{}, n: n}
	if err != nil {
		return a, fmt.Errorf("run requests in pod %s: %w\n%s", from, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
		case line == "FAIL":
			a.failed++
		case strings.HasPrefix(line, "wget:") || strings.HasPrefix(line, "nc:"):
			if !slices.Contains(a.errs, line) {
				a.errs = append(a.errs, line)
			}
		default:
			fields := strings.Fields(line)
			a.by[fields[0]]++
			if len(fields) > 1 {
				a.from[fields[0]] = strings.TrimPrefix(fields[1], "::ffff:")
			}
			if len(fields) > 2 {
				a.port[fields[0]] = fields[2]
			}
		}
	}
	return a, nil
}

// await repeats a batch until check accepts it or the timeout passes, and
// returns the last batch with check's last complaint. Rules land a moment
// after the API change that causes them, so every traffic assertion polls.
func await(ctx context.Context, timeout time.Duration, batch func() (answers, error), check func(answers) error) (answers, error) {
	deadline := time.Now().Add(timeout)
	for {
		a, err := batch()
		if err == nil {
			err = check(a)
		}
		if err == nil {
			return a, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return a, err
		}
		select {
		case <-ctx.Done():
			return a, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// only accepts a batch every request of which was answered by one of pods.
func only(pods ...string) func(answers) error {
	return func(a answers) error {
		if a.failed > 0 {
			return fmt.Errorf("%d of %d requests failed", a.failed, a.n)
		}
		for p := range a.by {
			if !slices.Contains(pods, p) {
				return fmt.Errorf("pod %s answered, and only %s should", p, strings.Join(pods, ", "))
			}
		}
		return nil
	}
}

// all accepts a batch with no failures in which every one of pods answered.
func all(pods ...string) func(answers) error {
	return func(a answers) error {
		if err := only(pods...)(a); err != nil {
			return err
		}
		for _, p := range pods {
			if a.by[p] == 0 {
				return fmt.Errorf("pod %s never answered", p)
			}
		}
		return nil
	}
}

// refused accepts a batch in which every request failed, and failed fast.
func refused(a answers) error {
	if len(a.by) > 0 {
		return fmt.Errorf("pods answered: %v", a.pods())
	}
	for _, e := range a.errs {
		if !strings.Contains(e, "refused") {
			return fmt.Errorf("requests failed, but not by being refused: %s", e)
		}
	}
	if len(a.errs) == 0 {
		return fmt.Errorf("requests failed without saying why")
	}
	return nil
}

// failing accepts a batch in which nothing answered, however it failed.
func failing(a answers) error {
	if len(a.by) > 0 {
		return fmt.Errorf("pods answered: %v", a.pods())
	}
	return nil
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

// alive fails when the program has already exited, which a long-running
// proxy never should.
func alive(p *runner.Process) error {
	if done, res := p.Exited(); done {
		return fmt.Errorf("the program exited (%d)\nstdout:\n%s\nstderr:\n%s", res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return nil
}

// said is the program's recent output, for the end of a failure message.
func said(p *runner.Process) string {
	return fmt.Sprintf("\nthe program said:\n%s\nstderr:\n%s", tail(p.Stdout()), tail(p.Stderr()))
}

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
