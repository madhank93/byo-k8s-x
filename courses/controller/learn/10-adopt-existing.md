---
title: Adopt what is already there
concepts: [adoption, orphans, idempotence, migrations]
---

## Core concept

Sooner or later the controller meets a Deployment with the right name that
belongs to nobody: someone created it by hand, or an earlier version of this
controller did not set owner references, or a migration half-finished.

Creating a second one is impossible — the name is taken. Failing is worse than
it looks: the cluster then needs a human before it can converge, which defeats
the purpose of running a controller. **Adopting** is the only outcome that
leaves the cluster correct, so the controller claims the object by adding its
owner reference and carries on.

Adoption is also where the `controller: true` flag earns its keep. Claim only
what nobody else controls; a child that already has a controller belongs to
someone, and taking it is how two controllers end up rewriting each other's
work in a loop.

## Go APIs

- `metav1.GetControllerOf(existing)` — nil means unclaimed.
- `existing.DeepCopy()` before mutating an object that came from a client or a
  cache; the copy in the cache is shared.
- `deployments.Update(ctx, updated, metav1.UpdateOptions{})`.

## Hints

<details><summary>Nudge</summary>

Adoption must keep the object. If the UID changes, you deleted and recreated
it — which for a Deployment means every pod restarted.
</details>

<details><summary>Approach</summary>

Get, check for a controller owner, append yours if there is none, update. Then
fall through to the code that fixes the spec, so an adopted object is also
corrected in the same pass.
</details>

<details><summary>Implementation</summary>

```go
if metav1.GetControllerOf(existing) != nil {
    return nil
}
updated := existing.DeepCopy()
updated.OwnerReferences = append(updated.OwnerReferences, ownerRef(site))
_, err := deployments.Update(ctx, updated, metav1.UpdateOptions{})
```
</details>

## Further reading

- [Owners and dependents](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/)
- [How the ReplicaSet controller adopts pods](https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/controller_ref_manager.go)
