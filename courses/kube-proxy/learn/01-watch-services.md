---
title: The Services that are yours
concepts: [service, informer, label selector, clusterIP, headless]
---

## Core concept

A proxy starts from one question: which Services are mine to serve? Here the
answer is every Service labelled `service.kubernetes.io/service-proxy-name:
byok8s`. The real kube-proxy asks the opposite question — every Service
*without* that label — which is how two proxies share one node without either
writing rules for the other's addresses.

Not every Service has an address to proxy. A **headless** Service sets
`clusterIP: None`: it exists so DNS returns the pods' own addresses, and there
is no virtual IP for anyone to rewrite. An `ExternalName` Service is only a
DNS alias. Both are skipped.

As in any controller, there are two halves to finding your objects, and each
misses what the other catches: a **list** sees what existed when you asked, a
**watch** sees what changes afterwards. An informer does both, in order, and
keeps a cache you can read at any time.

A proxy also has a choice about *when* to act. Reacting to each event one by
one means a burst of a hundred changes costs a hundred rule rewrites. The
shape kube-proxy uses, and the one this course builds, is a **sync loop**:
event handlers only mark the proxy dirty, and one sync reads the whole desired
state from the informer caches and acts on it. Ten events or one, it is one
pass.

For this stage, print one line per Service you own that has a cluster IP:

```
service <namespace>/<name> <clusterIP>
```

The tester creates one before your program starts and one while it runs. It
also creates a Service with no label, one labelled for another proxy, and a
headless one; none of them may appear.

## Go APIs

- `clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))` finds the
  cluster.
- `informers.NewSharedInformerFactoryWithOptions` with
  `informers.WithTweakListOptions` to set a `LabelSelector` on both the list
  and the watch.
- `factory.Core().V1().Services().Lister()` reads the cache;
  `cache.ResourceEventHandlerFuncs` on the informer tells you something
  changed.
- `signal.NotifyContext(ctx, syscall.SIGTERM)` — the program runs until it is
  stopped.

## Hints

<details><summary>Nudge</summary>

Can the API server do the filtering for you? Selectors work on labels, and a
watch accepts one too.
</details>

<details><summary>Approach</summary>

One informer factory with the label selector, a buffered channel of size one
that every handler sends to without blocking, and a loop that, on each
receive, lists Services from the lister and prints the ones whose cluster IP
it has not printed yet. Skip `None` and `ExternalName`.
</details>

## Further reading

- [Service](https://kubernetes.io/docs/concepts/services-networking/service/)
- [Virtual IPs and service proxies](https://kubernetes.io/docs/reference/networking/virtual-ips/)
- [KEP-2447: make kube-proxy service abstraction optional](https://github.com/kubernetes/enhancements/tree/master/keps/sig-network/2447-Make-kube-proxy-service-abstraction-optional)
