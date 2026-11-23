// A kube-proxy: Services and EndpointSlices in, nftables rules out.
//
// It runs as root on a node, watches the Services labelled
// service.kubernetes.io/service-proxy-name=byok8s and their EndpointSlices,
// and reports each Service with a cluster IP and the ready addresses behind it.
// Its rules live in one nftables table, ip byok8s, and nowhere else.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	proxyLabel = "service.kubernetes.io/service-proxy-name"
	proxyName  = "byok8s"
	table      = "byok8s"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Only this proxy's objects: the EndpointSlice controller copies a
	// Service's labels onto its slices, so one selector serves both.
	factory := informers.NewSharedInformerFactoryWithOptions(cs, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = proxyLabel + "=" + proxyName
		}))
	p := &proxier{
		services:  factory.Core().V1().Services().Lister(),
		slices:    factory.Discovery().V1().EndpointSlices().Lister(),
		changed:   make(chan struct{}, 1),
		reported:  map[string]string{},
		reportedE: map[string]string{},
	}
	poke := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { p.poke() },
		UpdateFunc: func(any, any) { p.poke() },
		DeleteFunc: func(any) { p.poke() },
	}
	if _, err := factory.Core().V1().Services().Informer().AddEventHandler(poke); err != nil {
		return err
	}
	if _, err := factory.Discovery().V1().EndpointSlices().Informer().AddEventHandler(poke); err != nil {
		return err
	}
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	p.poke()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.changed:
			if err := p.sync(); err != nil {
				fmt.Fprintln(os.Stderr, "sync:", err)
			}
		}
	}
}

// proxier turns the cluster's state into this node's rules. Events only mark
// it dirty; one sync then reads everything from the listers, so a burst of
// changes costs one pass.
type proxier struct {
	services listersv1.ServiceLister
	slices   discoverylisters.EndpointSliceLister
	changed  chan struct{}

	reported  map[string]string // service key -> cluster IP last printed
	reportedE map[string]string // service key -> endpoints last printed
}

func (p *proxier) poke() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

func (p *proxier) sync() error {
	svcs, err := p.services.List(labels.Everything())
	if err != nil {
		return err
	}
	sort.Slice(svcs, func(i, j int) bool { return key(svcs[i]) < key(svcs[j]) })
	for _, s := range svcs {
		if !proxied(s) {
			continue
		}
		if p.reported[key(s)] != s.Spec.ClusterIP {
			p.reported[key(s)] = s.Spec.ClusterIP
			fmt.Printf("service %s %s\n", key(s), s.Spec.ClusterIP)
		}
		eps, err := p.endpoints(s)
		if err != nil {
			return err
		}
		var ips []string
		for _, e := range eps {
			ips = append(ips, e.ip)
		}
		if joined := strings.Join(ips, ","); p.reportedE[key(s)] != joined {
			p.reportedE[key(s)] = joined
			fmt.Printf("endpoints %s %s\n", key(s), joined)
		}
	}
	return p.apply(render())
}

// render writes the whole table. Adding the table first makes the delete
// valid whether or not one exists, and nft applies the file as one
// transaction, so the node never sees a moment with no rules.
func render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "add table ip %s\ndelete table ip %s\n", table, table)
	fmt.Fprintf(&b, "table ip %s {\n", table)
	b.WriteString(`	chain nat-prerouting {
		type nat hook prerouting priority dstnat; policy accept;
		jump services
	}
	chain nat-output {
		type nat hook output priority -100; policy accept;
		jump services
	}
	chain services {
	}
}
`)
	return b.String()
}

func (p *proxier) apply(ruleset string) error {
	cmd := exec.Command("nft", "-f", "-")
	cmd.Stdin = strings.NewReader(ruleset)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nft: %v\n%s", err, out)
	}
	return nil
}

// endpoint is one ready address behind a Service.
type endpoint struct {
	ip string
}

// endpoints joins every EndpointSlice of a Service into one sorted list. A
// Service with many pods has many slices, and an address can briefly appear
// in two of them while the controller moves it.
func (p *proxier) endpoints(s *corev1.Service) ([]endpoint, error) {
	sel := labels.SelectorFromSet(labels.Set{discoveryv1.LabelServiceName: s.Name})
	list, err := p.slices.EndpointSlices(s.Namespace).List(sel)
	if err != nil {
		return nil, err
	}
	var out []endpoint
	for _, sl := range list {
		if sl.AddressType != discoveryv1.AddressTypeIPv4 {
			continue
		}
		for _, e := range sl.Endpoints {
			// A nil condition means ready: the field is newer than the API.
			if e.Conditions.Ready != nil && !*e.Conditions.Ready {
				continue
			}
			ep := endpoint{ip: e.Addresses[0]}
			if !slices.Contains(out, ep) {
				out = append(out, ep)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ip < out[j].ip })
	return out, nil
}

// proxied reports whether a Service has a virtual IP for a proxy to serve:
// headless and ExternalName Services do not.
func proxied(s *corev1.Service) bool {
	return s.Spec.ClusterIP != "" && s.Spec.ClusterIP != corev1.ClusterIPNone &&
		s.Spec.Type != corev1.ServiceTypeExternalName
}

func key(s *corev1.Service) string { return s.Namespace + "/" + s.Name }
