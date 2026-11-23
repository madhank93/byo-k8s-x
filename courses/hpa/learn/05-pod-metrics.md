---
title: Serve PodMetrics
concepts: [PodMetrics, metrics.k8s.io, scrape loop, Status, timestamp and window]
---

## Core concept

A metrics server answers from memory. Scraping on request would make every
`kubectl top` wait ten seconds for two kubelet samples, so the work happens on
a timer instead: a loop scrapes every node, round after round, and replaces
what it holds with the newest rates. A request just reads the latest round.

Two paths make up the pods resource:

```
GET /apis/metrics.k8s.io/v1beta1/namespaces/<ns>/pods          PodMetricsList
GET /apis/metrics.k8s.io/v1beta1/namespaces/<ns>/pods/<name>   PodMetrics
```

A PodMetrics is a pod's name and namespace, its containers each with a
`usage` of `cpu` and `memory` as quantities (`"183m"`, `"41943040"`), and two
fields that are easy to forget:

- `timestamp` — when the sample was taken;
- `window` — the span the CPU rate was measured over, like `"10.003s"`.

A rate without them is a number with no age. The autoscaler you build later
uses the timestamp to decide whether a sample is recent enough to trust.

A pod you hold nothing for is not an empty object. Answer the way the API
server would: a `Status` with `code: 404` and `reason: NotFound`, so a client
can tell "no such pod" from "the server broke".

## Go APIs

- `http.ServeMux` patterns with wildcards: `"GET /apis/.../namespaces/{ns}/pods/{name}"`,
  read with `r.PathValue("ns")`.
- `resource.NewMilliQuantity(cpu, resource.DecimalSI)`,
  `resource.NewQuantity(mem, resource.BinarySI)` — both marshal as JSON strings.
- `metav1.NewTime`, `metav1.Duration` marshal as the API's own formats.
- `metav1.Status` with `Reason: metav1.StatusReasonNotFound`.

## Hints

<details><summary>Nudge</summary>

`collect` from stage 3 already produces a whole round of rates. Run it in a
loop on a goroutine, and swap the result in under a mutex.
</details>

<details><summary>Approach</summary>

Keep `usage` per `namespace/pod/container`, plus the sample's time and window.
A list request walks the keys in the asked namespace and groups them by pod;
a get is the same, then looks up one name.
</details>

## Further reading

- [metrics.k8s.io types](https://github.com/kubernetes/metrics/blob/master/pkg/apis/metrics/v1beta1/types.go)
- [metrics-server design](https://github.com/kubernetes-sigs/metrics-server/blob/master/docs/design.md)
