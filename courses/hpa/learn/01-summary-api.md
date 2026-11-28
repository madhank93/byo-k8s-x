---
title: Ask a kubelet what its pods use
concepts: [kubelet, summary api, node proxy, working set]
---

## Core concept

Every kubelet already knows what its containers use: it reads the cgroups on
its node and publishes the numbers over its own HTTPS port. You do not need a
network path to that port. The API server proxies to it:

```
GET /api/v1/nodes/<node>/proxy/stats/summary
```

The answer is the **Summary API**, a JSON document with one entry for the node
and one per pod. Each pod carries a `podRef` (name, namespace), a `cpu` block
with `usageNanoCores`, and a `memory` block with `workingSetBytes`.

Of the memory figures, **working set** is the one that matters: memory in use
minus the page cache the kernel could reclaim. It is what the kubelet compares
against eviction thresholds, and what `kubectl top` shows.

A kubelet only knows about its own node, so the summary lists every pod
running there — `kube-system` pods included — and nothing running elsewhere.

## Go APIs

- `clientset.CoreV1().RESTClient().Get().AbsPath("/api/v1/nodes", node, "proxy/stats/summary").DoRaw(ctx)`.
- `encoding/json` into a struct holding only the fields you read.
- `usageNanoCores / 1e6` is millicores.

## Hints

<details><summary>Nudge</summary>

You never connect to the kubelet. Ask the API server to proxy for you, and
decode only the fields the output line needs.
</details>

## Further reading

- [Node metrics data](https://kubernetes.io/docs/reference/instrumentation/node-metrics/)
- [The Summary API types](https://github.com/kubernetes/kubelet/blob/master/pkg/apis/stats/v1alpha1/types.go)
