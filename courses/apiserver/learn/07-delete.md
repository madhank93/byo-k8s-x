---
title: Taking it back out
concepts: [delete, notfound, idempotency, uid]
---

## Core concept

```
DELETE /api/v1/namespaces/default/configmaps/doomed
200 OK   → the object as it last was

DELETE /api/v1/namespaces/default/configmaps/doomed   (again)
404 Not Found   → Status, reason NotFound
```

**200 with a body.** The real server answers most resources with the object it
removed and a few with a `Status` saying `Success`; either tells the caller the
delete happened. An empty body tells it nothing, and a client that has to infer
success from a bare code cannot log what it removed.

**A delete moves the counter.** It is a write like any other. A watcher that saw
the create has to be able to place the removal after it, and that only works if
the deletion has a version of its own. This matters two stages from now and is
invisible today, which is exactly why it is easy to leave out.

**Deleting what is already gone is a 404.** A cleanup path that runs twice needs
to tell "I removed it" from "it was already gone" — and neither is an error it
should stop on. `IsNotFound` is how it does that, which means the reason field
has to be right.

**Then the interesting part: the name comes free, and what takes it is a
different object.**

A name is a slot. The `uid` is the object. Recreate `doomed` and it gets a new
uid, and everything that referenced the old one is now referencing something
that does not exist — deliberately. This is why:

- owner references are uid-based, so a controller cannot adopt a stranger that
  happens to have the name of a child it created earlier,
- a `Pod`'s uid appears in its volume paths on the node,
- and events, ownership and garbage collection all key on uid rather than name.

The controller course's "adopt an existing child" stage is only safe because of
what this stage does.

**What a real delete also does, and this one does not:** finalizers hold an
object in a "deleting" state until their owners clear them, `deletionTimestamp`
marks it, grace periods let a kubelet stop a container properly, and
`propagationPolicy` decides whether children go too. All of it is built on top
of the simple removal here.

## Go APIs

- `mux.HandleFunc("DELETE /api/v1/namespaces/{namespace}/configmaps/{name}", …)`.
- `delete(s.objects, key)` — the builtin, under the same lock as everything
  else, after the counter moves.

## Hints

<details><summary>Nudge</summary>

You already have the not-found error from the update. The delete needs nothing
new but the removal itself.
</details>

<details><summary>Approach</summary>

`remove(namespace, name) (object, error)`: look the key up, return not-found if
it is absent, bump the counter, `delete` from the map, and return what was
there. The handler writes 200 with it, or the 404 Status.
</details>

## Further reading

- [Object names and uids](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/)
- [Owners and dependents](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/)
- [Garbage collection and finalizers](https://kubernetes.io/docs/concepts/architecture/garbage-collection/)
