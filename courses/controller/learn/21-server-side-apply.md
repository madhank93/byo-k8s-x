---
title: Apply as a field manager
concepts: [server-side apply, managedFields, field ownership, force]
---

## Core concept

Everything so far has been "check, then create or patch". **Server-side apply**
replaces that with one call: you send the object as you want it, the API server
records which fields *you* set, and it merges them with whatever anyone else
owns.

The record is `metadata.managedFields`, and it changes what a write means. With
apply, dropping a field from your object means you have *stopped managing* it,
so the API server removes the value you previously set — while leaving fields
owned by other managers alone. Create-or-patch cannot express that; it either
overwrites everything or nothing.

For a controller this collapses three cases into one. The child does not exist:
it is created. It exists and matches: nothing changes. It drifted: it is put
back. And an object someone else edited stays edited except in the fields you
own — unless you pass `force`, which takes ownership of the conflicting fields
and is usually right for a controller, because the spec is the intent.

## Go APIs

- The generated apply configurations, e.g.
  `appsv1apply.Deployment(name, ns).WithSpec(...)`.
- `clientset.AppsV1().Deployments(ns).Apply(ctx, ac, metav1.ApplyOptions{FieldManager: "byok8s-controller", Force: true})`.
- `types.ApplyPatchType` for the same thing through a dynamic client.

## Hints

<details><summary>Nudge</summary>

The field manager name is part of the contract. Change it between releases and
your controller no longer owns what it set last time, and the old fields are
left orphaned.
</details>

<details><summary>Approach</summary>

Build the whole desired child, every time, and apply it. Delete the "does it
exist" branch — that is the code this stage is meant to remove.
</details>

<details><summary>Implementation</summary>

```go
ac := appsv1apply.Deployment(name, ns).
    WithLabels(labels).
    WithOwnerReferences(ownerRefApply(site)).
    WithSpec(appsv1apply.DeploymentSpec().WithReplicas(replicas)...)

_, err := deployments.Apply(ctx, ac, metav1.ApplyOptions{
    FieldManager: "byok8s-controller", Force: true,
})
```
</details>

## Further reading

- [Server-Side Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
- [Field management](https://kubernetes.io/docs/reference/using-api/server-side-apply/#field-management)
