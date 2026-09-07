---
title: Leave other people's objects alone
concepts: [ownership, controller flag, blast radius, safety]
---

## Core concept

A controller that matches objects by name is a controller that will eventually
overwrite something it did not create. Names collide: a Website called
`grafana` in a namespace that already has a Deployment called `grafana`, owned
by a Helm release, is not a hypothetical.

The rule is simple and absolute: **touch only what you control.** An object
with a controller owner reference pointing at something else belongs to that
something else, and the correct response is to do nothing and say why — not to
adopt it, not to patch it, not to delete it.

Saying why matters. A Website that silently never converges is a support
ticket; a Website whose status explains that its Deployment is owned by another
object is a two-minute fix.

Compare with stage 10: an object with *no* controller is unclaimed and can be
adopted. An object with a *different* controller is someone else's, forever.

## Go APIs

- `metav1.GetControllerOf(existing)` — nil, yours, or somebody else's.
- Compare `owner.UID` against the Website's UID, not its name: a recreated
  Website is a different object.

## Hints

<details><summary>Nudge</summary>

Return an error rather than silently skipping. It goes through the retry path,
which is where the operator will see it.
</details>

<details><summary>Approach</summary>

Do the check before every write to a child, in one helper, so a child added
later cannot forget it.
</details>

<details><summary>Implementation</summary>

```go
if owner := metav1.GetControllerOf(existing); owner != nil && owner.UID != site.GetUID() {
    return fmt.Errorf("deployment %s/%s is controlled by %s %q — leaving it alone",
        ns, name, owner.Kind, owner.Name)
}
```
</details>

## Further reading

- [Owners and dependents](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/)
- [ControllerRefManager, how the built-ins do it](https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/controller_ref_manager.go)
