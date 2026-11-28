---
title: Scale on a metric of your own
concepts: [custom.metrics.k8s.io, Pods metrics, Object metrics, AverageValue, Value]
---

## Core concept

CPU is a proxy. What a web service really has too much of is requests.
`custom.metrics.k8s.io` is a second aggregated API, served in real clusters
by an adapter such as prometheus-adapter, with numbers about pods and other
objects. The HPA reads two kinds of them.

A **Pods** metric is one number per pod, compared on average:

```
GET /apis/custom.metrics.k8s.io/v1beta2/namespaces/<ns>/pods/*/requests_per_second?labelSelector=app%3Drps
ratio = average / target.averageValue        desired = ceil(ratio × pods)
```

An **Object** metric describes one object, such as the Service in front of
the pods:

```
GET /apis/custom.metrics.k8s.io/v1beta2/namespaces/<ns>/services/front/hits
```

With a `Value` target, `ratio = value / target`, and the ready pods are
scaled by it. With an `AverageValue` target, the value is shared out:
`desired = ceil(value / averageValue)`. The tolerance applies to both.

The path names the resource the way kubectl does: `services`, or
`deployments.apps` for a group. Your RESTMapper already turns a kind into
that.

## Go APIs

- Each item is a `MetricValue`, and its `value` is a `resource.Quantity`.
  Read it with `MilliValue()`, since 250m hits per second is a real value.
- `mapping.Resource.GroupResource().String()` gives `services` or
  `deployments.apps`.

## Hints

<details><summary>Nudge</summary>

An empty list is not zero. It means nothing reported the metric. Make it an
error, as the next stages rely on.
</details>

## Further reading

- [Scaling on custom metrics](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/#autoscaling-on-multiple-metrics-and-custom-metrics)
- [custom-metrics-apiserver](https://github.com/kubernetes-sigs/custom-metrics-apiserver)
