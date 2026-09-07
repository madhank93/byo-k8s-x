---
title: Delete an object
concepts: [write verbs, api semantics, finalizers]
---

## Core concept

The first write, and the lesson is that delete is **asynchronous**. The call
returns when the server has accepted the request and marked the object — not
when it is gone. A pod with a grace period stays visible, now carrying a
`deletionTimestamp`, until its containers stop; an object with a finalizer
stays until whatever registered that finalizer removes it.

So "deleted" in your output means accepted, which is what kubectl means too.
A tool that prints "deleted" and a follow-up `get` that still lists the object
are both correct.

Deleting something that does not exist is a **404**, and it should stay an
error rather than being smoothed into success — the same distinction stage 11
made for Get.

## Go APIs

- `dyn.Resource(gvr).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{})`.
- `metav1.DeleteOptions` is where grace period, propagation policy and
  preconditions live, if you want to look.

## Hints

<details><summary>Nudge</summary>

You already resolve a GVR and build a dynamic client; this is one more verb on
the same resource interface.
</details>

## Further reading

- [Object deletion and garbage collection](https://kubernetes.io/docs/concepts/architecture/garbage-collection/)
- [Finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
