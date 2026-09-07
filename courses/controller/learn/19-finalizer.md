---
title: Clean up before you let go
concepts: [finalizers, deletionTimestamp, cleanup ordering, stuck objects]
---

## Core concept

Garbage collection handles anything inside the cluster that carries an owner
reference. It cannot handle anything outside it — a DNS record, a bucket, a
row in someone else's database — and it cannot run code.

A **finalizer** buys you the time to. It is a string in `metadata.finalizers`,
and while it is there the API server will not remove the object: a delete sets
`deletionTimestamp` and stops. Your controller sees an object that is being
deleted, does the work, removes its finalizer, and the object disappears.

The order is not negotiable. Do the work, *then* remove the finalizer. The
moment it is gone the object is unreachable and anything left undone is
undone forever.

The other half is the warning. A finalizer nobody removes — a controller that
was uninstalled, a bug in this path — leaves an object that cannot be deleted
and a namespace stuck in `Terminating` behind it. Every finalizer you add is a
promise that something will come back for it.

## Go APIs

- `site.GetDeletionTimestamp()` — non-nil means "being deleted"; the object is
  still readable and still yours to finish with.
- `site.GetFinalizers()` / `site.SetFinalizers()`.
- `slices.Contains` and `slices.DeleteFunc` for the list.

## Hints

<details><summary>Nudge</summary>

Add the finalizer *before* creating the children. A delete that arrives in the
gap leaves you holding nothing to clean up with.
</details>

<details><summary>Approach</summary>

Check for the deletion timestamp first in the pass, ahead of everything else. A
Website being deleted should not have its Deployment repaired on the way out.
</details>

<details><summary>Implementation</summary>

```go
if site.GetDeletionTimestamp() != nil {
    return finalize(ctx, clientset, dyn, site)
}
if !slices.Contains(site.GetFinalizers(), finalizerName) {
    return addFinalizer(ctx, dyn, site)
}
```
</details>

## Further reading

- [Finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
- [Using finalizers to control deletion](https://kubernetes.io/blog/2021/05/14/using-finalizers-to-control-deletion/)
