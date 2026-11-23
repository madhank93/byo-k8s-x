// Package kubeproxy holds the assertions for the "Build your own kube-proxy"
// course — one function per stage, registered by slug.
//
// The learner's program is built for Linux and runs as root inside one kind
// worker through runner.Node, beside the real kube-proxy. It owns only the
// Services labelled service.kubernetes.io/service-proxy-name=byok8s, which
// the real one skips, and writes its rules into an nftables table of its own.
// The verdict is traffic: a stage sends requests from pods and judges which
// backend answered, from what address, or that nothing did.
package kubeproxy

import (
	"context"
	"fmt"
	"strings"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

var registry = map[string]Stage{}

func register(s Stage) { registry[s.Slug] = s }

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := registry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "watch-services", Run: stageWatchServices})
	register(Stage{Slug: "watch-endpointslices", Run: stageWatchEndpointSlices})
	register(Stage{Slug: "nft-table", Run: stageNftTable})
	register(Stage{Slug: "clusterip", Run: stageClusterIP})
	register(Stage{Slug: "load-balance", Run: func(ctx context.Context, env *kube.Env, bin string) error {
		if err := stageLoadBalance(ctx, env, bin); err != nil {
			return err
		}
		return stageEndpointChanges(ctx, env, bin)
	}})
	register(Stage{Slug: "named-ports", Run: stageNamedPorts})
	register(Stage{Slug: "no-endpoints", Run: stageNoEndpoints})
	register(Stage{Slug: "nodeport", Run: stageNodePort})
	register(Stage{Slug: "masquerade", Run: stageMasquerade})
}

// stageWatchServices checks the program finds the Services that are its own:
// labelled for it, with a cluster IP to proxy. One exists before it starts and
// one is created while it runs; the real kube-proxy's, another proxy's and a
// headless one are not its business.
func stageWatchServices(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	before, err := f.service(ctx, "before", nil)
	if err != nil {
		return err
	}
	if _, err := f.service(ctx, "unlabelled", func(s *corev1.Service) { s.Labels = nil }); err != nil {
		return err
	}
	if _, err := f.service(ctx, "elsewhere", func(s *corev1.Service) { s.Labels[proxyLabel] = "someone-else" }); err != nil {
		return err
	}
	if _, err := f.service(ctx, "headless", func(s *corev1.Service) { s.Spec.ClusterIP = corev1.ClusterIPNone }); err != nil {
		return err
	}

	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	want := fmt.Sprintf("service %s/before %s\n", env.Namespace, before.Spec.ClusterIP)
	if err := awaitLine(ctx, p, want, 60*time.Second); err != nil {
		return fmt.Errorf("Service before is labelled %s=%s and existed when the program started, and it was never reported: %w", proxyLabel, proxyName, err)
	}
	after, err := f.service(ctx, "after", nil)
	if err != nil {
		return err
	}
	want = fmt.Sprintf("service %s/after %s\n", env.Namespace, after.Spec.ClusterIP)
	if err := awaitLine(ctx, p, want, 30*time.Second); err != nil {
		return fmt.Errorf("Service after was created while the program ran, and it was never reported: %w", err)
	}

	// The others existed before "after", so a report of them would be out by now.
	out := p.Stdout()
	for name, why := range map[string]string{
		"unlabelled": "it has no " + proxyLabel + " label, so it is the real kube-proxy's",
		"elsewhere":  proxyLabel + " names another proxy",
		"headless":   "it is headless: clusterIP None leaves nothing to proxy",
	} {
		if strings.Contains(out, env.Namespace+"/"+name+" ") {
			return fmt.Errorf("Service %s was reported, and %s%s", name, why, said(p))
		}
	}
	return nil
}

// stageWatchEndpointSlices checks the program joins a Service's EndpointSlices
// into one list of ready addresses and follows it as the slices change. The
// slices are written by hand, so one Service has two of them, and an address
// that is not ready yet is held back until it is.
func stageWatchEndpointSlices(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	if _, err := f.service(ctx, "split", func(s *corev1.Service) { s.Spec.Selector = nil }); err != nil {
		return err
	}
	if err := f.slice(ctx, "split-a", "split", []discoveryv1.Endpoint{
		endpoint("10.250.0.1", true), endpoint("10.250.0.2", true),
	}); err != nil {
		return err
	}
	if err := f.slice(ctx, "split-b", "split", []discoveryv1.Endpoint{
		endpoint("10.250.0.3", true), endpoint("10.250.0.4", false),
	}); err != nil {
		return err
	}

	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	line := func(ips string) string { return fmt.Sprintf("endpoints %s/split %s\n", env.Namespace, ips) }
	if err := awaitLine(ctx, p, line("10.250.0.1,10.250.0.2,10.250.0.3"), 60*time.Second); err != nil {
		return fmt.Errorf("Service split has two EndpointSlices holding three ready addresses between them, and 10.250.0.4 is not ready: %w", err)
	}
	if err := f.slice(ctx, "split-b", "split", []discoveryv1.Endpoint{
		endpoint("10.250.0.3", true), endpoint("10.250.0.4", true),
	}); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, line("10.250.0.1,10.250.0.2,10.250.0.3,10.250.0.4"), 30*time.Second); err != nil {
		return fmt.Errorf("10.250.0.4 became ready and the program did not follow: %w", err)
	}
	if err := f.env.Client.DiscoveryV1().EndpointSlices(env.Namespace).Delete(ctx, "split-a", metav1.DeleteOptions{}); err != nil {
		return err
	}
	if err := awaitLine(ctx, p, line("10.250.0.3,10.250.0.4"), 30*time.Second); err != nil {
		return fmt.Errorf("EndpointSlice split-a was deleted, taking 10.250.0.1 and 10.250.0.2 with it, and the program did not follow: %w", err)
	}
	return nil
}

// canary is a table the grader plants beside the program's, standing for
// every table on the node that belongs to someone else.
const canary = "byok8s-grader-canary"

// stageNftTable checks the program claims its table: it replaces whatever a
// previous run left under that name, hooks a nat chain into prerouting (for
// pods' traffic) and output (for the node's own), and touches no other table.
func stageNftTable(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)
	defer nft(context.WithoutCancel(ctx), "delete", "table", "ip", canary)

	plant := func(ctx context.Context) error {
		return nftApply(ctx, fmt.Sprintf(`table ip %s {
	chain stale {
		ip daddr 10.250.9.9 tcp dport 80 counter
	}
}
table ip %s {
	chain kept {
		counter
	}
}
`, table, canary))
	}
	p, stop, err := launch(ctx, env, bin, plant)
	if err != nil {
		return err
	}
	defer stop()

	// A Service created now forces at least one sync after the first.
	svc, err := f.service(ctx, "poke", nil)
	if err != nil {
		return err
	}
	if err := awaitLine(ctx, p, fmt.Sprintf("service %s/poke %s\n", env.Namespace, svc.Spec.ClusterIP), 60*time.Second); err != nil {
		return err
	}

	var last error
	err = waitFor(ctx, "the program's table", 30*time.Second, func(ctx context.Context) (bool, error) {
		objs, err := tableObjects(ctx)
		if err != nil {
			return false, err
		}
		last = checkTable(objs)
		return last == nil, nil
	})
	if err != nil {
		return fmt.Errorf("%v%s", last, said(p))
	}
	if _, code, err := nft(ctx, "list", "table", "ip", canary); err != nil || code != 0 {
		return fmt.Errorf("table ip %s belongs to someone else, and it is gone: never flush the ruleset or delete a table you did not create%s", canary, said(p))
	}
	return alive(p)
}

func checkTable(objs []nftObject) error {
	if objs == nil {
		return fmt.Errorf("there is no table ip %s on %s", table, proxyNode)
	}
	hooked := map[string]bool{}
	for _, o := range objs {
		if o.Chain == nil {
			continue
		}
		if o.Chain.Name == "stale" {
			return fmt.Errorf("table ip %s still holds chain stale, left by an earlier run: start from a table of your own making, not from what you find", table)
		}
		if o.Chain.Type == "nat" {
			hooked[o.Chain.Hook] = true
		}
	}
	for _, h := range []string{"prerouting", "output"} {
		if !hooked[h] {
			return fmt.Errorf("table ip %s has no nat chain hooked into %s", table, h)
		}
	}
	return nil
}

// stageClusterIP checks a cluster IP reaches the one pod behind it, both from
// a pod on the proxy's node (prerouting) and from the node itself, by way of
// a host-network pod (output). The backend is on another node, so the
// rewritten packet really is routed somewhere.
func stageClusterIP(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	if err := f.create(ctx,
		backend("web", farNode, map[string]string{"app": "web"}),
		client("client", proxyNode, false),
		client("host", proxyNode, true),
	); err != nil {
		return err
	}
	svc, err := f.service(ctx, "web", nil)
	if err != nil {
		return err
	}
	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	ip := svc.Spec.ClusterIP
	for _, from := range []string{"client", "host"} {
		a, err := await(ctx, 45*time.Second, func() (answers, error) { return f.get(ctx, from, ip, 80, 3) }, all("web"))
		if err != nil {
			where := "a pod on " + proxyNode + ", whose packets pass prerouting"
			if from == "host" {
				where = proxyNode + " itself, whose packets pass output"
			}
			return fmt.Errorf("requests to Service web at %s:80 from %s never reached pod web: %v\n%s%s", ip, where, err, a, said(p))
		}
	}
	return alive(p)
}

// stageLoadBalance checks new connections are spread over every endpoint.
// Sixty requests over three pods leave each an expected twenty; a fair pick
// gives one fewer than six about once in twenty thousand runs.
func stageLoadBalance(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	lb := map[string]string{"app": "lb"}
	if err := f.create(ctx,
		backend("lb-1", farNode, lb), backend("lb-2", remoteNode, lb), backend("lb-3", proxyNode, lb),
		client("client", proxyNode, false),
	); err != nil {
		return err
	}
	svc, err := f.service(ctx, "lb", nil)
	if err != nil {
		return err
	}
	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	ip := svc.Spec.ClusterIP
	a, err := await(ctx, 60*time.Second, func() (answers, error) { return f.get(ctx, "client", ip, 80, 60) }, func(a answers) error {
		if err := all("lb-1", "lb-2", "lb-3")(a); err != nil {
			return err
		}
		for _, pod := range []string{"lb-1", "lb-2", "lb-3"} {
			if a.by[pod] < 6 {
				return fmt.Errorf("pod %s answered %d of 60, where a fair share is about 20", pod, a.by[pod])
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("Service lb has three ready pods, and new connections to %s:80 should be spread over all of them: %v\n%s%s", ip, err, a, said(p))
	}
	return alive(p)
}

// stageNamedPorts checks a target port given by name is resolved per pod.
// Two pods both name their ports web and admin, with the numbers swapped, so
// no single number is right for the whole Service: only the EndpointSlices,
// which carry each pod's resolved port, say where each request should go.
// The Service has two ports, so the program must keep them apart as well.
func stageNamedPorts(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	named := map[string]string{"app": "named"}
	declare := func(pod *corev1.Pod, web, admin int32) *corev1.Pod {
		pod.Spec.Containers[0].Ports = []corev1.ContainerPort{
			{Name: "web", ContainerPort: web}, {Name: "admin", ContainerPort: admin},
		}
		return pod
	}
	if err := f.create(ctx,
		declare(backend("named-a", farNode, named), 8080, 8081),
		declare(backend("named-b", remoteNode, named), 8081, 8080),
		client("client", proxyNode, false),
	); err != nil {
		return err
	}
	svc, err := f.service(ctx, "named", func(s *corev1.Service) {
		s.Spec.Ports = []corev1.ServicePort{
			{Name: "http", Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("web")},
			{Name: "admin", Port: 81, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromString("admin")},
		}
	})
	if err != nil {
		return err
	}
	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	ip := svc.Spec.ClusterIP
	for _, c := range []struct {
		port   int32
		target string
		want   map[string]string
	}{
		{80, "web", map[string]string{"named-a": "8080", "named-b": "8081"}},
		{81, "admin", map[string]string{"named-a": "8081", "named-b": "8080"}},
	} {
		a, err := await(ctx, 60*time.Second, func() (answers, error) { return f.get(ctx, "client", ip, c.port, 20) }, func(a answers) error {
			if err := all("named-a", "named-b")(a); err != nil {
				return err
			}
			for pod, port := range c.want {
				if a.port[pod] != port {
					return fmt.Errorf("pod %s was reached on port %s, and the port it calls %s is %s", pod, a.port[pod], c.target, port)
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("port %d of Service named targets the port called %q, which is a different number on each pod: %v\n%s%s", c.port, c.target, err, a, said(p))
		}
	}
	return alive(p)
}

// stageEndpointChanges is the second half of load-balance. It checks the
// rules follow the endpoints: a pod serving
// but not yet ready gets nothing until it is, a deleted pod stops getting
// anything, and a new one starts.
func stageEndpointChanges(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	app := map[string]string{"app": "changes"}
	if err := f.create(ctx, backend("steady", farNode, app), client("client", proxyNode, false)); err != nil {
		return err
	}
	late, err := f.started(ctx, gated(backend("late", remoteNode, app)))
	if err != nil {
		return err
	}
	// Pod late must really be serving, or holding it back would prove nothing.
	if _, err := await(ctx, 30*time.Second, func() (answers, error) { return f.get(ctx, "client", late.Status.PodIP, 8080, 1) }, all("late")); err != nil {
		return fmt.Errorf("pod late is not serving on its own address: %w", err)
	}
	svc, err := f.service(ctx, "changes", nil)
	if err != nil {
		return err
	}
	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	ip := svc.Spec.ClusterIP
	batch := func() (answers, error) { return f.get(ctx, "client", ip, 80, 20) }
	if a, err := await(ctx, 45*time.Second, batch, all("steady")); err != nil {
		return fmt.Errorf("Service changes should send everything to pod steady while pod late is not ready: %v\n%s%s", err, a, said(p))
	}
	if _, err := f.exec(ctx, "late", "touch", "/tmp/ready"); err != nil {
		return err
	}
	if a, err := await(ctx, 45*time.Second, batch, all("steady", "late")); err != nil {
		return fmt.Errorf("pod late became ready, and Service changes should now use it too: %v\n%s%s", err, a, said(p))
	}
	if err := f.deletePod(ctx, "steady", nil); err != nil {
		return err
	}
	if a, err := await(ctx, 45*time.Second, batch, all("late")); err != nil {
		return fmt.Errorf("pod steady was deleted, and requests should stop going to it: %v\n%s%s", err, a, said(p))
	}
	if err := f.create(ctx, backend("fresh", farNode, app)); err != nil {
		return err
	}
	if a, err := await(ctx, 45*time.Second, batch, all("late", "fresh")); err != nil {
		return fmt.Errorf("pod fresh was created and became ready, and Service changes should use it: %v\n%s%s", err, a, said(p))
	}
	return alive(p)
}

// stageNoEndpoints checks a Service with nothing behind it refuses
// connections at once, from a pod and from the node, rather than letting the
// packet wander off the default route until the client gives up — and that
// it stops refusing the moment a pod arrives.
func stageNoEndpoints(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	if err := f.create(ctx, client("client", proxyNode, false), client("host", proxyNode, true)); err != nil {
		return err
	}
	svc, err := f.service(ctx, "empty", nil)
	if err != nil {
		return err
	}
	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	ip := svc.Spec.ClusterIP
	for _, from := range []string{"client", "host"} {
		if a, err := await(ctx, 45*time.Second, func() (answers, error) { return f.get(ctx, from, ip, 80, 3) }, refused); err != nil {
			return fmt.Errorf("Service empty has no endpoints, and a connection to %s:80 from pod %s should be refused straight away: %v\n%s%s", ip, from, err, a, said(p))
		}
	}
	if err := f.create(ctx, backend("arrival", farNode, map[string]string{"app": "empty"})); err != nil {
		return err
	}
	if a, err := await(ctx, 45*time.Second, func() (answers, error) { return f.get(ctx, "client", ip, 80, 5) }, all("arrival")); err != nil {
		return fmt.Errorf("pod arrival is ready behind Service empty, and it should be reached: %v\n%s%s", err, a, said(p))
	}
	if err := f.deletePod(ctx, "arrival", nil); err != nil {
		return err
	}
	if a, err := await(ctx, 45*time.Second, func() (answers, error) { return f.get(ctx, "client", ip, 80, 3) }, refused); err != nil {
		return fmt.Errorf("pod arrival was deleted, leaving Service empty with nothing again: %v\n%s%s", err, a, said(p))
	}
	return alive(p)
}

// nodePort creates a NodePort Service of this program's and returns it with
// the port the API server allocated.
func (f fixture) nodePort(ctx context.Context, name string, mutate func(*corev1.Service)) (*corev1.Service, int32, error) {
	svc, err := f.service(ctx, name, func(s *corev1.Service) {
		s.Spec.Type = corev1.ServiceTypeNodePort
		if mutate != nil {
			mutate(s)
		}
	})
	if err != nil {
		return nil, 0, err
	}
	return svc, svc.Spec.Ports[0].NodePort, nil
}

// stageNodePort checks a node port on the proxy's node reaches the Service
// from a pod on another node. The backend is on the proxy's node, so the
// reply passes back through it without any help.
func stageNodePort(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	if err := f.create(ctx,
		backend("np", proxyNode, map[string]string{"app": "np"}),
		client("remote", remoteNode, false),
		client("client", proxyNode, false),
	); err != nil {
		return err
	}
	svc, port, err := f.nodePort(ctx, "np", nil)
	if err != nil {
		return err
	}
	nodeIP, err := f.nodeIP(ctx, proxyNode)
	if err != nil {
		return err
	}
	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	if a, err := await(ctx, 45*time.Second, func() (answers, error) { return f.get(ctx, "remote", nodeIP, port, 3) }, all("np")); err != nil {
		return fmt.Errorf("Service np has node port %d, and a pod on %s connecting to %s:%d should reach pod np: %v\n%s%s", port, remoteNode, nodeIP, port, err, a, said(p))
	}
	if a, err := await(ctx, 30*time.Second, func() (answers, error) { return f.get(ctx, "client", svc.Spec.ClusterIP, 80, 3) }, all("np")); err != nil {
		return fmt.Errorf("a NodePort Service is still a ClusterIP Service: %s:80 should reach pod np: %v\n%s%s", svc.Spec.ClusterIP, err, a, said(p))
	}
	return alive(p)
}

// stageMasquerade checks the program rewrites the source of traffic that
// would otherwise reply around it, and of no other.
//
// A pod on remoteNode reaches a node port on proxyNode whose only pod is on
// farNode: unless the source is rewritten to proxyNode, the pod replies
// straight to the client, which never asked it anything. A pod reaching its
// own Service and being picked would reply to itself without passing the
// node. A pod on proxyNode reaching a cluster IP needs neither, and its
// address must survive so the backend knows who called.
func stageMasquerade(ctx context.Context, env *kube.Env, bin string) error {
	f := fixture{env}
	defer f.done(ctx)

	if err := f.create(ctx,
		backend("far", farNode, map[string]string{"app": "far"}),
		backend("loner", proxyNode, map[string]string{"app": "loner"}),
		client("remote", remoteNode, false),
		client("client", proxyNode, false),
	); err != nil {
		return err
	}
	_, port, err := f.nodePort(ctx, "far", nil)
	if err != nil {
		return err
	}
	far, err := f.service(ctx, "far-internal", func(s *corev1.Service) { s.Spec.Selector = map[string]string{"app": "far"} })
	if err != nil {
		return err
	}
	loner, err := f.service(ctx, "loner", nil)
	if err != nil {
		return err
	}
	nodeIP, err := f.nodeIP(ctx, proxyNode)
	if err != nil {
		return err
	}
	clientPod, err := f.pod(ctx, "client")
	if err != nil {
		return err
	}
	p, stop, err := launch(ctx, env, bin)
	if err != nil {
		return err
	}
	defer stop()

	a, err := await(ctx, 45*time.Second, func() (answers, error) { return f.get(ctx, "remote", nodeIP, port, 3) }, func(a answers) error {
		if err := all("far")(a); err != nil {
			return err
		}
		if a.from["far"] != nodeIP {
			return fmt.Errorf("pod far saw the request come from %s, not from %s", a.from["far"], nodeIP)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("node port %d on %s forwards to pod far on %s; the reply only finds its way back if the source is rewritten to %s (masquerade): %v\n%s%s", port, proxyNode, farNode, nodeIP, err, a, said(p))
	}
	if a, err := await(ctx, 30*time.Second, func() (answers, error) { return f.get(ctx, "loner", loner.Spec.ClusterIP, 80, 3) }, all("loner")); err != nil {
		return fmt.Errorf("pod loner connecting to its own Service at %s:80 is sent back to itself, and only reaches itself if the source is rewritten (hairpin): %v\n%s%s", loner.Spec.ClusterIP, err, a, said(p))
	}
	a, err = await(ctx, 30*time.Second, func() (answers, error) { return f.get(ctx, "client", far.Spec.ClusterIP, 80, 3) }, func(a answers) error {
		if err := all("far")(a); err != nil {
			return err
		}
		if a.from["far"] != clientPod.Status.PodIP {
			return fmt.Errorf("pod far saw the request come from %s, and the client is %s", a.from["far"], clientPod.Status.PodIP)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("a pod reaching a cluster IP needs no masquerade, and the backend should see the client's own address: %v\n%s%s", err, a, said(p))
	}
	return alive(p)
}
