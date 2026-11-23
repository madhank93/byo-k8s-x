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
