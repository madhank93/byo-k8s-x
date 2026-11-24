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
	var ports []servicePort
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
		for _, list := range eps {
			for _, e := range list {
				if !slices.Contains(ips, e.ip) {
					ips = append(ips, e.ip)
				}
			}
		}
		sort.Strings(ips)
		if joined := strings.Join(ips, ","); p.reportedE[key(s)] != joined {
			p.reportedE[key(s)] = joined
			fmt.Printf("endpoints %s %s\n", key(s), joined)
		}
		for _, sp := range s.Spec.Ports {
			ports = append(ports, servicePort{svc: s, port: sp, endpoints: eps[sp.Name]})
		}
	}
	return p.apply(render(ports))
}

// servicePort is one port of one Service: the unit a rule is written for.
type servicePort struct {
	svc       *corev1.Service
	port      corev1.ServicePort
	endpoints []endpoint
}

// chain names the chain holding one service port's rules. Namespace and name
// are DNS labels, and nft accepts '/' and '.' in a chain name.
func (sp servicePort) chain() string {
	return fmt.Sprintf("svc-%s/%s/%d", key(sp.svc), proto(sp.port.Protocol), sp.port.Port)
}

// endpointChain names the chain that sends a service port's traffic to one
// endpoint.
func (sp servicePort) endpointChain(ep endpoint) string {
	return fmt.Sprintf("ep-%s/%s/%d/%s", key(sp.svc), proto(sp.port.Protocol), sp.port.Port, ep.ip)
}

func proto(p corev1.Protocol) string { return strings.ToLower(string(p)) }

// render writes the whole table. Adding the table first makes the delete
// valid whether or not one exists, and nft applies the file as one
// transaction, so the node never sees a moment with no rules.
//
// Traffic is dispatched by one lookup: a verdict map keyed on destination
// address, protocol and port sends each packet to its service port's chain.
func render(ports []servicePort) string {
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
	map service-ips {
		type ipv4_addr . inet_proto . inet_service : verdict
	}
	map service-nodeports {
		type inet_proto . inet_service : verdict
	}
	chain services {
		ip daddr . meta l4proto . th dport vmap @service-ips
		fib daddr type local meta l4proto . th dport vmap @service-nodeports
	}
	map no-endpoint-services {
		type ipv4_addr . inet_proto . inet_service : verdict
	}
	chain filter-forward {
		type filter hook forward priority filter; policy accept;
		jump no-endpoints
	}
	chain filter-output {
		type filter hook output priority filter; policy accept;
		jump no-endpoints
	}
	chain no-endpoints {
		ip daddr . meta l4proto . th dport vmap @no-endpoint-services
	}
	chain reject-chain {
		reject
	}
`)
	for _, sp := range ports {
		if len(sp.endpoints) == 0 {
			continue
		}
		// numgen picks an endpoint per new connection; conntrack keeps the
		// rest of the connection on it.
		var picks []string
		for i, ep := range sp.endpoints {
			picks = append(picks, fmt.Sprintf("%d : goto %s", i, sp.endpointChain(ep)))
		}
		fmt.Fprintf(&b, "\tchain %s {\n", sp.chain())
		fmt.Fprintf(&b, "\t\tnumgen random mod %d vmap { %s }\n", len(sp.endpoints), strings.Join(picks, ", "))
		b.WriteString("\t}\n")
		for _, ep := range sp.endpoints {
			fmt.Fprintf(&b, "\tchain %s {\n", sp.endpointChain(ep))
			fmt.Fprintf(&b, "\t\tmeta l4proto %s dnat to %s:%d\n", proto(sp.port.Protocol), ep.ip, ep.port)
			b.WriteString("\t}\n")
		}
	}
	b.WriteString("}\n")
	for _, sp := range ports {
		// A cluster IP with nothing behind it is not DNATed, so its packets
		// reach the filter hooks with the address unchanged and are refused
		// there, instead of being routed out of the node to time out.
		if len(sp.endpoints) == 0 {
			fmt.Fprintf(&b, "add element ip %s no-endpoint-services { %s . %s . %d : goto reject-chain }\n",
				table, sp.svc.Spec.ClusterIP, proto(sp.port.Protocol), sp.port.Port)
			continue
		}
		fmt.Fprintf(&b, "add element ip %s service-ips { %s . %s . %d : goto %s }\n",
			table, sp.svc.Spec.ClusterIP, proto(sp.port.Protocol), sp.port.Port, sp.chain())
		// A node port is open on every address the node owns, which is
		// what fib daddr type local matches.
		if sp.port.NodePort != 0 {
			fmt.Fprintf(&b, "add element ip %s service-nodeports { %s . %d : goto %s }\n",
				table, proto(sp.port.Protocol), sp.port.NodePort, sp.chain())
		}
	}
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

// endpoint is one ready address behind a service port, and the port number
// that pod gives it. A targetPort can be a name, and each pod may give the
// name a different number; the slice records each pod's.
type endpoint struct {
	ip   string
	port int32
}

// endpoints joins every EndpointSlice of a Service into one sorted list per
// service port, keyed by port name. A Service with many pods has many
// slices, pods that number a named port differently land in different ones,
// and an address can briefly appear in two while the controller moves it.
func (p *proxier) endpoints(s *corev1.Service) (map[string][]endpoint, error) {
	sel := labels.SelectorFromSet(labels.Set{discoveryv1.LabelServiceName: s.Name})
	list, err := p.slices.EndpointSlices(s.Namespace).List(sel)
	if err != nil {
		return nil, err
	}
	out := map[string][]endpoint{}
	for _, sl := range list {
		if sl.AddressType != discoveryv1.AddressTypeIPv4 {
			continue
		}
		for _, port := range sl.Ports {
			if port.Port == nil {
				continue
			}
			name := ""
			if port.Name != nil {
				name = *port.Name
			}
			for _, e := range sl.Endpoints {
				// A nil condition means ready: the field is newer than the API.
				if e.Conditions.Ready != nil && !*e.Conditions.Ready {
					continue
				}
				ep := endpoint{ip: e.Addresses[0], port: *port.Port}
				if !slices.Contains(out[name], ep) {
					out[name] = append(out[name], ep)
				}
			}
		}
	}
	for _, eps := range out {
		sort.Slice(eps, func(i, j int) bool { return eps[i].ip < eps[j].ip })
	}
	return out, nil
}

// proxied reports whether a Service has a virtual IP for a proxy to serve:
// headless and ExternalName Services do not.
func proxied(s *corev1.Service) bool {
	return s.Spec.ClusterIP != "" && s.Spec.ClusterIP != corev1.ClusterIPNone &&
		s.Spec.Type != corev1.ServiceTypeExternalName
}

func key(s *corev1.Service) string { return s.Namespace + "/" + s.Name }
