---
title: Scale on something outside the cluster
concepts: [external.metrics.k8s.io, metric selectors]
---

## Core concept

Some load is not in the cluster at all: the depth of a cloud queue, or the
lag of a Kafka topic. `external.metrics.k8s.io` serves those numbers.
Because one metric name can cover many series, the Autoscaler picks one with
a selector:

```yaml
- type: External
  external:
    metric:
      name: queue_depth
      selector: {matchLabels: {queue: jobs}}
    target: {type: AverageValue, averageValue: "20"}
```

```
GET /apis/external.metrics.k8s.io/v1beta1/namespaces/<ns>/queue_depth?labelSelector=queue%3Djobs
```

Note whose labels these are. The selector matches the metric's own labels,
not the pods'. Add up every series that comes back. Then the targets work
exactly as they do for an Object metric. 130 jobs waiting at 20 per pod
want `ceil(130/20)` = 7, and a `Value` target scales the ready pods by
`value / target`.

## Go APIs

- `metav1.LabelSelectorAsSelector(metric.Selector)`, then `.String()` for
  the query.

## Hints

<details><summary>Nudge</summary>

Leave the selector off and you read every queue at once, a much larger
number. If one Autoscaler wildly overshoots, check the selector first.
</details>

## Further reading

- [Scaling on metrics not related to Kubernetes objects](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/#autoscaling-on-metrics-not-related-to-kubernetes-objects)
