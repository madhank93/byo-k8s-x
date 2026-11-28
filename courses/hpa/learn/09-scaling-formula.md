---
title: The scaling formula
concepts: [resource utilization, desired replicas, minReplicas and maxReplicas]
---

## Core concept

A CPU target is a **utilization**: a percentage of what the pods *request*,
not of a core.

```yaml
metrics:
- type: Resource
  resource:
    name: cpu
    target: {type: Utilization, averageUtilization: 50}
```

Read every pod the target's selector matches from `metrics.k8s.io` (the API
you built, or any other server registered for it). Add up their usage, add up
their requests, and divide:

```
utilization = 100 × Σ usage / Σ requests          (integer percent)
ratio       = utilization / target
desired     = ceil(ratio × pods)
```

Four pods using 33m each, each requesting 50m: utilization 66%, ratio 1.32,
desired `ceil(5.28)` = 6. Always round **up**. Rounding to the nearest number
would leave a service that needs 5.28 pods running on 5.

Then clamp to `[minReplicas, maxReplicas]`. `minReplicas` defaults to 1.

Report what you read as well as what you decided. `status.currentMetrics`
carries the utilization, and `status.desiredReplicas` the answer. Nothing
scales yet. That comes once the number can be trusted.

## Go APIs

- `cs.Discovery().RESTClient().Get().AbsPath("/apis/metrics.k8s.io/v1beta1/namespaces/<ns>/pods").Param("labelSelector", sel).DoRaw(ctx)`
  reaches any aggregated path through your kubeconfig's credentials.
- `scale.status.selector` is the label selector of the target's pods, already
  serialized.
- `corev1.ResourceList.Name(corev1.ResourceCPU, resource.DecimalSI).MilliValue()`.
- `autoscalingv2.MetricStatus{Type: "Resource", Resource: &autoscalingv2.ResourceMetricStatus{...}}`.

## Hints

<details><summary>Nudge</summary>

A pod with no request has no utilization. Treat it as an error for the
metric, not as zero.
</details>

## Further reading

- [Algorithm details](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details)
