---
title: Build your own metrics server and HPA — primer
concepts: [metrics-server, summary api, aggregation layer, metrics.k8s.io, horizontal pod autoscaler, scale subresource]
---

## The mental model

`kubectl top` and the HorizontalPodAutoscaler look like features of the API
server. They are not. Both read `metrics.k8s.io`, an API the API server does
not implement: it forwards every request for it to whatever has registered
itself through the **aggregation layer** — in most clusters, metrics-server.
And metrics-server stores nothing. It asks every kubelet what its containers
are using, keeps the latest numbers in memory, and answers from there.

The autoscaler is a control loop on top of that API. For each
HorizontalPodAutoscaler it reads its target's current replica count through
the target's `scale` subresource, reads the metrics, works out how many
replicas would bring the metric to its target, and writes that number back
through `scale`.

This course builds both halves, as one program:

```
 kubelet /stats/summary ─┐
 kubelet /metrics/resource ─► scrape ─► rates ─► per pod ─► metrics.k8s.io   (stages 1-7)
                                                                │
                       Autoscaler ─► formula ─► behaviour ─► /scale           (stages 8-18)
```

## Why the autoscaler scales an Autoscaler

A kind cluster already runs the real HPA controller inside
kube-controller-manager, and two controllers acting on one
HorizontalPodAutoscaler fight. So from stage 8 your program watches
`autoscalers.byok8s.dev`: a custom resource with the same `spec` and `status`
as `autoscaling/v2` HorizontalPodAutoscaler, in a group the real controller
ignores. Everything you learn transfers field for field.

For those stages the grader serves `metrics.k8s.io`, `custom.metrics.k8s.io`
and `external.metrics.k8s.io` itself, with numbers each stage chooses, so the
replica count it expects is arithmetic and not a guess about load.

## The command line

```
app summary <node>     one kubelet's Summary API, a line per pod
app scrape <node>      CPU rates from /metrics/resource, a line per container
app pods               every node, a line per pod in your namespace
app serve --port P --external-host H     the metrics API, through the aggregator
app autoscale          the autoscaler, until it is stopped
```

The usage lines are `<name> <cpu>m <memory-bytes>`.

## How the stages are graded

Each stage runs in its own namespace with a kubeconfig pointing there. The
grader pins pods to nodes — some holding a known amount of memory, some
spinning under a CPU limit — and compares what your program reports. From
stage 4 it talks to your server only through the API server, the way
`kubectl` does; stage 7 runs the real `kubectl top`. From stage 8 it judges
the Autoscaler's `status` and the replicas you write. Nothing inspects your
source.

## Further reading

- [Resource metrics pipeline](https://kubernetes.io/docs/tasks/debug/debug-cluster/resource-metrics-pipeline/)
- [metrics-server](https://github.com/kubernetes-sigs/metrics-server)
- [Horizontal Pod Autoscaling](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
