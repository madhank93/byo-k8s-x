---
title: Scale through a subresource
concepts: [subresources, rbac, api design]
---

## What this stage teaches

`/scale` is a **subresource**: a separate endpoint on the same object, with its
own tiny schema.

```
  PATCH /apis/apps/v1/namespaces/web/deployments/api          the whole Deployment
  PATCH /apis/apps/v1/namespaces/web/deployments/api/scale    just the replica count
```

It exists because permissions are written against resources. Being able to
change the replica count and being able to change the pod template are
different powers, and a subresource is the unit RBAC can grant separately — so
an autoscaler gets `deployments/scale` and never the ability to swap your
image. It also keeps two writers off the same endpoint: the HPA patches
`/scale` while the deploy pipeline applies the Deployment, and they stop
fighting over one object.

The other half is the schema. Scale is a shared shape — `spec.replicas`,
`status.replicas`, `status.selector` — implemented by Deployments,
StatefulSets, ReplicaSets and any CRD that opts in. That is what lets one code
path scale things it knows nothing about. The `spec.replicas` in your patch
body is *Scale's* field, not the Deployment's, even though setting it moves the
same number.

## Go you'll reach for

- The dynamic resource interface takes trailing subresource names:
  `Patch(ctx, name, types.MergePatchType, body, opts, "scale")`.
- A body of `{"spec":{"replicas":N}}` — no apiVersion or kind needed for a
  merge patch.
- The typed alternative is `cs.AppsV1().Deployments(ns).UpdateScale(...)`, which
  works only for the kinds you compiled in.

## Hints

<details><summary>Nudge</summary>

Look at the last parameter of the dynamic client's `Patch`. It is variadic for
a reason.
</details>

<details><summary>Approach</summary>

Patch `spec.replicas` at the `scale` subresource with a merge patch. Confirm
with `kubectl get deploy` that the parent object's replica count moved — same
number, different endpoint.
</details>

## Going deeper

- [Scale subresource](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/deployment-v1/#DeploymentSpec)
- [RBAC on subresources](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources)
