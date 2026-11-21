---
title: Apply exactly the plan
concepts: [plans, optimistic concurrency, preconditions, resourceVersion, uid]
---

## Core concept

`apply --plan PLAN` applies the file stage 16 wrote. It applies the file, not
the manifest: the manifest may have moved on since review, and what was
approved is the plan. The objects in it are already complete, with namespace
and set label filled in, and the deletes are already listed. Of the recorded
options, only `force` still has a job at apply time.

A plan only means something while the cluster still looks the way it did when
the plan was made, so apply first checks every pin:

```
  planned     still true at apply time
  ─────────   ───────────────────────────────────────────────
  create      the object does not exist
  update      same uid, same resourceVersion
  unchanged   same uid, same resourceVersion
  delete      same uid, same resourceVersion
```

If any pin fails, nothing is written. Each stale object gets a line, and
only the stale ones: an `unchanged` object someone edited counts, one nobody
touched does not. The command exits 1, and the answer is to plan again:

```
ConfigMap/same-a changed since the plan
error: nothing applied: the cluster has changed since this plan was made; plan again
```

This is stage 10's rule again: a change lands whole or not at all. Checking as
you go is not enough. An update that fails its pin halfway through the plan
leaves the earlier objects already applied.

The check and the write are separate requests, so something can still change
in between. The write carries its own pin as well:

- **Update.** Put the planned `uid` and `resourceVersion` in the body's
  `metadata`. A server-side apply that carries a `resourceVersion` is refused
  with a 409 if the object has moved on. The `uid` matters too: a
  `resourceVersion` alone does not stop an apply from *recreating* an object
  someone deleted, but a `uid` that matches nothing is refused.
- **Delete.** `Preconditions{UID, ResourceVersion}`, as in stage 13.
- **Create.** Apply has no "must not exist" precondition. The absence check is
  all you get. A POST would close the gap, but its write would be recorded as
  an `Update`, not an `Apply`, and the course's ownership model would no
  longer hold.

These pins are tested by a change that lands after your first write, once
every up-front check has passed. Only the pin on each later write can catch
it.

`unchanged` objects are checked but not written. Applying the plan means
making the changes it lists, and a no-op apply would not change anything
anyway.

## Go APIs

- `o.SetUID(types.UID(ch.UID))`, `o.SetResourceVersion(ch.ResourceVersion)`
  on a copy of the planned object, never on the copy you keep.
- A pin that fails at write time comes back as a 409 Conflict, or as a 422
  Invalid when the `uid` now names a different object (deleted and
  recreated). Don't branch on `apierrors.IsConflict(err)` alone.

## Hints

<details><summary>Nudge</summary>

Split `setup` so that connecting no longer needs a manifest. Then the plan
file is enough to start from.
</details>

<details><summary>Approach</summary>

Loop over the changes twice. The first loop fetches each object and compares
its `uid` and `resourceVersion` with the pin. Both are empty for an object
that doesn't exist, which is exactly a create's pin. After that, dry-run the
creates and updates the way `preflight` already does. The second loop writes.
</details>

<details><summary>Implementation</summary>

```go
var uid, rv string
if live != nil {
    uid, rv = string(live.GetUID()), live.GetResourceVersion()
}
if uid != ch.UID || rv != ch.ResourceVersion {
    fmt.Println(ref(o), "changed since the plan")
    stale = true
}
```
</details>

## Further reading

- [Server-side apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
- [terraform apply with a saved plan](https://developer.hashicorp.com/terraform/cli/commands/apply#apply-saved-plan)
