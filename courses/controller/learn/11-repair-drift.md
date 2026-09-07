---
title: Put back what drifts
concepts: [drift, patch, conflicts, single source of truth]
---

## Core concept

Someone runs `kubectl scale`. The Deployment now says 0 and the Website still
says 2. A controller does not ask how that happened, or whether the human meant
it: the Website's spec is the only intent that counts, so the next pass puts it
back.

That is uncomfortable the first time you see it, and it is the point. If
manual changes survived, the cluster's real configuration would live in
whatever anyone last typed, and nothing could be reproduced. Anything you want
to keep goes in the spec.

The write is a **patch**, not a read-modify-write update. The Deployment's
status is being rewritten constantly by the controllers behind it, so an update
built on the copy you just read loses the race often enough to matter. A patch
says what to change and leaves the rest alone.

## Go APIs

- `deployments.Patch(ctx, name, types.StrategicMergePatchType, body, metav1.PatchOptions{})`.
- `types.StrategicMergePatchType` — merges lists by their merge key rather than
  replacing them, which is why a container can be addressed by name.
- `apierrors.IsConflict(err)` for the update path you are avoiding.

## Hints

<details><summary>Nudge</summary>

Compare before writing. A patch on every pass is a write on every pass, and
every write produces an event that queues another pass.
</details>

<details><summary>Approach</summary>

Nothing about the Website changed, so no Website event will arrive — this stage
is triggered by touching the Website. Catching drift with nothing to trigger it
is what the timer two stages later is for.
</details>

<details><summary>Implementation</summary>

```go
patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
_, err := deployments.Patch(ctx, existing.Name,
    types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
```
</details>

## Further reading

- [Update API objects in place using kubectl patch](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/)
- [API concepts: patch](https://kubernetes.io/docs/reference/using-api/api-concepts/#patch-and-apply)
