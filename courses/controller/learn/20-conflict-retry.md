---
title: Lose a race and recover
concepts: [optimistic concurrency, resourceVersion, RetryOnConflict, 409]
---

## Core concept

Kubernetes has no locks. Every write carries the `resourceVersion` the writer
read, and the API server rejects it if the object has moved on — HTTP 409,
`IsConflict`. This is **optimistic concurrency**: writers assume they will not
collide, and the loser is told to try again.

A conflict is therefore not a bug and not an outage. It is the normal outcome
of two writers touching one object, and your controller is always one of
several: the user editing spec, another controller adding a label, and you
writing status.

The fix is never to hold the object longer. It is to re-read and re-apply:
fetch fresh, make the change again on the new copy, write. `RetryOnConflict`
does exactly that, and it is the reason a busy Website converges instead of
retrying every ten seconds through the workqueue.

The trap is retrying the *write* without redoing the *read*. That resubmits
the same stale object and fails identically forever.

## Go APIs

- `retry.RetryOnConflict(retry.DefaultRetry, func() error { ... })` from
  `k8s.io/client-go/util/retry`.
- `apierrors.IsConflict(err)`.
- The alternative, where it fits: a `Patch`, which carries no resourceVersion
  and so cannot conflict at all.

## Hints

<details><summary>Nudge</summary>

Everything inside the retry closure must be re-done, the `Get` included. A
closure that captures the object from outside is the stale-read bug with extra
steps.
</details>

<details><summary>Approach</summary>

Status writes are the obvious place, because they happen on every pass while
the user is editing spec. Adding the finalizer is the other one.
</details>

<details><summary>Implementation</summary>

```go
return retry.RetryOnConflict(retry.DefaultRetry, func() error {
    latest, err := client.Get(ctx, name, metav1.GetOptions{})
    if err != nil {
        return err
    }
    setStatus(latest)
    _, err = client.UpdateStatus(ctx, latest, metav1.UpdateOptions{})
    return err
})
```
</details>

## Further reading

- [Concurrency control and consistency](https://kubernetes.io/docs/reference/using-api/api-concepts/#concurrency-control-and-consistency)
- [retry.RetryOnConflict](https://pkg.go.dev/k8s.io/client-go/util/retry#RetryOnConflict)
