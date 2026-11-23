---
title: Where each Service's pods are
concepts: [endpointslice, readiness, informer, join]
---

## Core concept

A Service says what address and port exist. It does not say where the pods
are. That is the job of **EndpointSlices**: the EndpointSlice controller
watches each Service's selector and the pods it matches, and writes their
addresses into one or more slices labelled `kubernetes.io/service-name:
<service>`.

*One or more* matters. A slice holds at most 100 endpoints by default, so a
big Service is spread across several, and an address can sit in two slices
for a moment while the controller rebalances them. A Service's endpoints are
the union of every slice that names it — never just the first one you find.
Slices can also be written by hand, for a Service with no selector that fronts
something outside the cluster; the tester does exactly that here.

Each endpoint carries **conditions**. For now one matters: `ready`. A pod that
is running but failing its readiness probe is still in the slice, marked
`ready: false`, and a proxy must not send it traffic. If `ready` is missing
altogether, treat it as true: clients older than the field never set it.

The controller copies the Service's labels onto its slices, so the label
selector you already use for Services finds the right slices too.

For this stage, print the ready addresses behind each of your Services,
sorted and comma-separated, whenever that list changes:

```
endpoints <namespace>/<name> <ip>,<ip>,…
```

The tester gives one Service two hand-written slices with an address not yet
ready, then makes it ready, then deletes one slice.

## Go APIs

- `factory.Discovery().V1().EndpointSlices()` — its lister and its informer.
  Poke the same sync from its handlers.
- `labels.SelectorFromSet(labels.Set{discoveryv1.LabelServiceName: name})`
  with `Lister().EndpointSlices(namespace).List(sel)` finds a Service's slices.
- `slice.AddressType` — this course is IPv4 only; skip `IPv6` and `FQDN`
  slices.
- `endpoint.Conditions.Ready` is a `*bool`.

## Hints

<details><summary>Nudge</summary>

What does your program print for a Service whose addresses live in two
slices, if it only ever reads one?
</details>

<details><summary>Approach</summary>

In the sync, for each Service list its slices by the service-name label,
collect the first address of every ready endpoint, deduplicate, sort, and
print when the joined string differs from what you printed last time.
</details>

## Further reading

- [EndpointSlices](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/)
- [Services without selectors](https://kubernetes.io/docs/concepts/services-networking/service/#services-without-selectors)
