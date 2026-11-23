// A kube-proxy: Services and EndpointSlices in, nftables rules out.
//
// It runs as root on a node, watches the Services labelled
// service.kubernetes.io/service-proxy-name=byok8s, and reports each one with
// a cluster IP to proxy.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	proxyLabel = "service.kubernetes.io/service-proxy-name"
	proxyName  = "byok8s"
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
		services: factory.Core().V1().Services().Lister(),
		changed:  make(chan struct{}, 1),
		reported: map[string]string{},
	}
	poke := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { p.poke() },
		UpdateFunc: func(any, any) { p.poke() },
		DeleteFunc: func(any) { p.poke() },
	}
	if _, err := factory.Core().V1().Services().Informer().AddEventHandler(poke); err != nil {
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
	changed  chan struct{}

	reported map[string]string // service key -> cluster IP last printed
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
	}
	return nil
}

// proxied reports whether a Service has a virtual IP for a proxy to serve:
// headless and ExternalName Services do not.
func proxied(s *corev1.Service) bool {
	return s.Spec.ClusterIP != "" && s.Spec.ClusterIP != corev1.ClusterIPNone &&
		s.Spec.Type != corev1.ServiceTypeExternalName
}

func key(s *corev1.Service) string { return s.Namespace + "/" + s.Name }
