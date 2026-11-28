---
title: Let kubectl top drive it
concepts: [kubectl top, labelSelector, all-namespaces list]
---

## Core concept

`kubectl top` has no metrics code of its own. It asks the API server for the
same lists you already serve, and it asks in two ways you do not answer yet:

```
kubectl top pod -l tier=hot   GET .../namespaces/<ns>/pods?labelSelector=tier%3Dhot
kubectl top pod -A            GET /apis/metrics.k8s.io/v1beta1/pods
```

A selector is the server's job, not kubectl's — kubectl prints whatever comes
back. But your metrics carry no labels: the kubelet reports a pod by namespace
and name only. The labels live on the Pod objects, so the API server already
knows which pods match. Ask it, and keep only theirs.

The list across every namespace is the same grouping with no namespace
filter. Once it spans namespaces, two pods can share a name, so key them by
`namespace/name`.

## Go APIs

- `r.URL.Query().Get("labelSelector")`
- `clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: sel})`
  — with `ns` empty, every namespace.
- A malformed selector comes back as an error from that call; answer it with a
  400 `Status`, as the API server would.

## Hints

<details><summary>Nudge</summary>

Register one handler for both `/pods` and `/namespaces/{ns}/pods`.
`PathValue("ns")` is empty on the first, which is exactly the "every
namespace" you want.
</details>

## Further reading

- [kubectl top](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_top/)
- [Label selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors)
