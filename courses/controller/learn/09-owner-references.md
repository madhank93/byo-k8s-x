---
title: Own what you create
concepts: [owner references, garbage collection, uid, controller flag]
---

## Core concept

The controller never writes deletion code for its children. It writes down who
owns what, and the API server's **garbage collector** does the rest: when the
Website goes, everything carrying an owner reference to it goes too, in the
right order, whether or not your controller is running at the time.

Two details in the reference are load-bearing:

- The owner is identified by **UID**, not name. Delete a Website and create
  another with the same name and it is a different object; children of the old
  one are collected rather than silently re-parented.
- `controller: true` marks *which* owner is in charge. An object can have
  several owners, but only one controller, and that is what stops two
  controllers adopting the same child and fighting over its spec forever.

`blockOwnerDeletion: true` additionally makes the owner wait for the child to
go first, which is what you want when the child holds something real.

## Go APIs

- `metav1.OwnerReference{APIVersion, Kind, Name, UID, Controller, BlockOwnerDeletion}`.
- `metav1.GetControllerOf(obj)` — the one owner marked `controller: true`, or nil.
- `site.GetUID()` on the unstructured Website.

## Hints

<details><summary>Nudge</summary>

`Controller` and `BlockOwnerDeletion` are `*bool`. A shared `yes := true` and
two pointers to it is the usual way.
</details>

<details><summary>Approach</summary>

Set the reference when the child is built, not afterwards — a child created
without one and updated a moment later is an orphan for that moment, and a
crash in between makes it a permanent one.
</details>

<details><summary>Implementation</summary>

```go
yes := true
return metav1.OwnerReference{
    APIVersion:         group + "/" + version,
    Kind:               kind,
    Name:               site.GetName(),
    UID:                site.GetUID(),
    Controller:         &yes,
    BlockOwnerDeletion: &yes,
}
```
</details>

## Further reading

- [Owners and dependents](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/)
- [Garbage collection](https://kubernetes.io/docs/concepts/architecture/garbage-collection/)
