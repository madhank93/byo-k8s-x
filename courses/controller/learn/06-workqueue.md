---
title: Queue keys, not objects
concepts: [workqueue, deduplication, keys, rate limiting]
---

## Core concept

Handlers must not do work. They run on the informer's thread, and anything slow
in one blocks every other event. What they do instead is add a **key** —
`namespace/name` — to a queue that a worker drains.

A key is deliberately less information than an object, and that is the point:

- Two events about the same object collapse into **one** queue entry, so a
  burst of updates costs one pass.
- The pass looks the object up itself, so it always works from the current
  state rather than from whatever was true when the event fired.
- A key survives the object. A delete queues a key that no longer resolves,
  which is exactly the signal that cleanup is needed.

The queue also tracks what is in flight: a key being worked on is not handed
out again until `Done`, so the same object is never reconciled twice at once.

## Go APIs

- `workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())`.
- `cache.DeletionHandlingMetaNamespaceKeyFunc(obj)` — like
  `MetaNamespaceKeyFunc`, but it unwraps a tombstone.
- `queue.Get()` / `queue.Done(key)` / `queue.Forget(key)`.
- `cache.SplitMetaNamespaceKey(key)` on the way back out.

## Hints

<details><summary>Nudge</summary>

`Done` has to be called for every key you `Get`, including the ones that fail —
a `defer` right after the `Get` is the only version of this that stays correct.
</details>

<details><summary>Approach</summary>

Write the worker as a function that handles exactly one key and returns whether
to keep going, then loop on it in a goroutine. `Get` blocks until there is
work, so the loop is not a busy wait.
</details>

<details><summary>Implementation</summary>

```go
key, shutdown := queue.Get()
if shutdown {
    return false
}
defer queue.Done(key)
reconcile(ctx, key)
queue.Forget(key)
return true
```
</details>

## Further reading

- [client-go/util/workqueue](https://pkg.go.dev/k8s.io/client-go/util/workqueue)
- [sample-controller](https://github.com/kubernetes/sample-controller)
