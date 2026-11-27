---
title: Watch autoscalers, read the scale
concepts: [HorizontalPodAutoscaler, scale subresource, RESTMapper, resync]
---

## Core concept

The second half of the course is the autoscaler. `app autoscale` reconciles
every **Autoscaler** in its namespace. An Autoscaler has the same spec and
status as an `autoscaling/v2` HorizontalPodAutoscaler. It lives in group
`byok8s.dev` so the cluster's own HPA controller never touches it, because
two controllers acting on one object fight.

```yaml
apiVersion: byok8s.dev/v1alpha1
kind: Autoscaler
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: api}
  minReplicas: 1
  maxReplicas: 5
  metrics: [...]
status:
  currentReplicas: 2     # you write this
  desiredReplicas: 2
```

The target can be any kind. A Deployment, a StatefulSet and a custom
resource all expose one shape through the **scale subresource**:

```
GET /apis/apps/v1/namespaces/<ns>/deployments/api/scale
{"spec": {"replicas": 2}, "status": {"replicas": 2, "selector": "app=api"}}
```

So the autoscaler never has to know what a Deployment is. It turns
`apiVersion` + `kind` into a resource with a RESTMapper, then reads `/scale`.
`status.replicas` is how many replicas exist now. That number becomes the
Autoscaler's `status.currentReplicas`.

A target's replica count changes without its Autoscaler changing, so a watch
on Autoscalers alone never notices. The real controller resyncs every
Autoscaler on a timer (15s by default). Yours should resync every couple of
seconds.

## Go APIs

- `autoscalingv2.HorizontalPodAutoscalerSpec` / `...Status` from
  `k8s.io/api/autoscaling/v2`. Convert the unstructured object into them with
  `runtime.DefaultUnstructuredConverter.FromUnstructured`.
- `restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(cs.Discovery()))`,
  then `RESTMapping(groupKind, version)`. Call `Reset()` when a kind is not
  found, since it may be newer than the cache.
- `dyn.Resource(gvr).Namespace(ns).Get(ctx, name, opts, "scale")`. The last
  argument names a subresource.
- `UpdateStatus` writes `status` and ignores `spec`.

## Hints

<details><summary>Nudge</summary>

A plain loop of list, sync each one, then sleep is enough. An error on one
Autoscaler should be logged and must not stop the others.
</details>

## Further reading

- [Horizontal Pod Autoscaling](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [The scale subresource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#scale-subresource)
