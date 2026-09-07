---
title: Reconcile on a timer
concepts: [resync, periodic reconcile, drift detection, safety net]
---

## Core concept

Nothing has to happen for a Website to stop being satisfied. A hand-run
`kubectl scale` changes the *child*, and no event about the *parent* will ever
arrive — so a controller that only reacts to its own resource never notices.

The cheapest fix is to come back on a timer: after a successful pass, requeue
the same key with a delay. It bounds how long the world can be wrong without
anyone noticing, and it costs one pass per object per period.

It is a floor, not a mechanism. A ten-second timer means ten seconds of drift,
which is fine for a safety net and much too slow to be how changes are
normally noticed — that is what watching your own children does, three stages
later. Keep both: the watch is fast, the timer is what catches whatever the
watch missed.

## Go APIs

- `queue.AddAfter(key, d)` — enqueue once, after a delay, deduplicated like any
  other add.
- The informer factory's resync period is the same idea one layer down: it
  replays every object as an update on an interval.

## Hints

<details><summary>Nudge</summary>

Requeue after success, not instead of `Forget`. They answer different
questions: one resets the backoff, the other schedules the next look.
</details>

<details><summary>Approach</summary>

Pick a period by asking how long you could tolerate being wrong, then multiply
by the number of objects to see what it costs the API server.
</details>

<details><summary>Implementation</summary>

```go
queue.Forget(key)
queue.AddAfter(key, resyncEvery)
return true
```
</details>

## Further reading

- [Informer resync vs. relist](https://github.com/kubernetes/client-go/blob/master/tools/cache/shared_informer.go)
- [controller-runtime: RequeueAfter](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile#Result)
