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
