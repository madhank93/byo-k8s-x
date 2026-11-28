---
title: Serve NodeMetrics
concepts: [NodeMetrics, node working set, cluster-scoped resources]
---

## Core concept

`nodes` is the cluster-scoped half of the API:

```
GET /apis/metrics.k8s.io/v1beta1/nodes           NodeMetricsList
GET /apis/metrics.k8s.io/v1beta1/nodes/<name>    NodeMetrics
```

A NodeMetrics has a name, a timestamp and window, and one `usage` for the
whole node — no containers.

It is tempting to add up the node's pods. Do not: a node also runs the
kubelet, the container runtime, the kernel and whatever else the operating
system needs, and none of that belongs to a pod. The kubelet measures the
node's own cgroup root, and `/metrics/resource` carries it in two lines with
no labels at all:

```
node_cpu_usage_seconds_total 41237.3 1732300000123
node_memory_working_set_bytes 7.0581e+08 1732300000123
```

Same counter-and-gauge pair as a container's, so the same rate code applies.
Only your parser has to accept a sample without braces.

## Go APIs

- One more `ServeMux` pattern pair, without `{ns}`.
- `strings.Cut` on the first space separates an unlabelled sample's name.

## Hints

<details><summary>Nudge</summary>

Keep the node's sample in the same map as its containers, under a key no
container can have — a namespace can never contain a colon.
</details>

## Further reading

- [Node metrics data](https://kubernetes.io/docs/reference/instrumentation/node-metrics/)
- [metrics-server: node and pod usage](https://github.com/kubernetes-sigs/metrics-server/blob/master/FAQ.md#how-cpu-usage-is-calculated)
