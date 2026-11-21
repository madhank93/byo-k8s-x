---
title: Never delete the wrong thing
concepts: [pruning, preconditions, guard rails]
---

## Core concept

Now act on the plan: `apply --set web --prune` deletes the set's leftovers.
Every other command in this course can be undone by applying again. This one
cannot. A deleted object takes its data, its status and, for a namespace,
everything in it. So the interesting part of this stage is the set of
deletes it refuses to make.

```
  guard                                    why
  ──────────────────────────────────────   ─────────────────────────────────────
  prune only after every apply succeeded   a refused change must not half-land
  refuse an empty manifest                 a bad glob or an empty render would
                                           otherwise delete the whole set
  skip byok8s.dev/prune: disabled          some objects, like a PVC holding
                                           data, must outlive the manifest
  delete with preconditions                the object you planned to delete is
                                           the object you delete
```

The last one closes a real race. Between listing a leftover and deleting it,
someone can relabel it into another set or delete and recreate it under the
same name. A delete by name alone removes whatever is there now. A delete
with `Preconditions{UID, ResourceVersion}` from the listed copy fails with a
409 if anything changed, and you keep the object and say why:

```
ConfigMap/gone pruned
ConfigMap/precious kept: prune disabled
```

A kind that stage 12 could not list is not pruned at all. You never saw its
leftovers, so there is nothing to delete.

The opt-out annotation is the same idea as Argo CD's `Prune=false` and
Flux's `prune: disabled`. `prune` (the plan) prints the kept line too, so the
plan and the apply agree.

## Go APIs

- `metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}`.
- `apierrors.IsConflict(err)`: a failed precondition. `apierrors.IsNotFound`:
  someone got there first, which is fine.

## Hints

<details><summary>Nudge</summary>

Write the guards first, before a single `Delete` call exists.
</details>

<details><summary>Approach</summary>

Check `--set` and the empty manifest before connecting. Prune at the very end
of `apply`, after the last object has been applied, reusing `leftovers` from
stage 12.
</details>

<details><summary>Implementation</summary>

```go
uid, rv := o.GetUID(), o.GetResourceVersion()
err := r.Delete(ctx, o.GetName(), metav1.DeleteOptions{
    Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv},
})
```
</details>

## Further reading

- [DeleteOptions preconditions](https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/delete-options/)
- [Argo CD: no prune resources](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/#no-prune-resources)
