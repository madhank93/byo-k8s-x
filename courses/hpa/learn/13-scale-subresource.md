---
title: Scale anything with a scale
concepts: [scale subresource, lastScaleTime, custom resources]
---

## Core concept

Until now the autoscaler only recommended. Now it acts, through the same
subresource it reads:

```
PUT /apis/apps/v1/namespaces/<ns>/deployments/web/scale
{"apiVersion": "autoscaling/v1", "kind": "Scale", "spec": {"replicas": 4}, ...}
```

Writing `/scale` changes `spec.replicas` and nothing else. The Deployment
controller does the rest. A custom resource whose definition declares a scale
subresource takes the same write, so `Workload` scales the same way, with no
code that knows what a Workload is. That is why the HPA works with
StatefulSets, ReplicaSets and any operator's custom resource.

Write only when the decision differs from the target's `spec.replicas`.
When you do write, set `status.lastScaleTime`.

Once the replicas change, the pods change too. New ones are `Pending` for a
while, and old ones linger as they terminate. The rules from the last two
stages already cover both.

## Go APIs

- `dyn.Resource(gvr).Namespace(ns).Update(ctx, scaleObj, opts, "scale")`.
  Send back the Scale you read, so its `resourceVersion` guards against a
  write that raced yours.
- `runtime.DefaultUnstructuredConverter.ToUnstructured(&scale)`.

## Hints

<details><summary>Nudge</summary>

The autoscaler needs the target's GroupVersionResource to write `/scale`, and
it already worked that out to read it. Return it from the same function.
</details>

## Further reading

- [Scale subresource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#scale-subresource)
