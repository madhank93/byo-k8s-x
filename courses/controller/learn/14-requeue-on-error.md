---
title: Retry instead of dying
concepts: [rate limiting, exponential backoff, requeue, error handling]
---

## Core concept

A pass fails. Not because your code is wrong — because the spec asks for
something the cluster refuses, or a webhook is down, or a quota is full. The
controller's job is to survive that: one Website that cannot be reconciled must
not stop the other nine hundred.

So a failed key goes back on the queue, and the delay before it comes round
again **grows with each attempt**. Immediate retries turn a persistent failure
into a hot loop against the API server, which is how a small problem becomes an
outage. The rate limiter is not politeness; it is the thing that keeps a broken
object from taking the cluster down with it.

`Forget` is the other half. It resets a key's backoff after a success, so the
next failure starts from a short delay rather than from wherever the last one
left off however long ago.

## Go APIs

- `queue.AddRateLimited(key)` — requeue with the limiter's next delay.
- `queue.Forget(key)` — reset the count, after success.
- `queue.NumRequeues(key)` — how many attempts so far.
- `workqueue.DefaultTypedControllerRateLimiter[string]()` — 5ms doubling to
  1000s, plus a global token bucket.

## Hints

<details><summary>Nudge</summary>

Make `reconcile` return an `error` and let the worker decide what to do with
it. A function that both fails and decides how to retry is two jobs.
</details>

<details><summary>Approach</summary>

Not every failure is worth retrying. A key that cannot be parsed will never
parse — drop it. A write the API server rejected because the spec is wrong will
succeed the moment the spec is fixed, so keep it.
</details>

<details><summary>Implementation</summary>

```go
if err := reconcile(ctx, lister, clientset, key); err != nil {
    fmt.Printf("retry %s attempt %d: %v\n", key, queue.NumRequeues(key)+1, err)
    queue.AddRateLimited(key)
    return true
}
queue.Forget(key)
```
</details>

## Further reading

- [workqueue rate limiters](https://pkg.go.dev/k8s.io/client-go/util/workqueue#DefaultTypedControllerRateLimiter)
- [API priority and fairness](https://kubernetes.io/docs/concepts/cluster-administration/flow-control/)
