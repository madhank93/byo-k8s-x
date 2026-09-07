---
title: List, then watch from there
concepts: [watch, resourceversion, list-then-watch, missed events]
---

## Core concept

A watch is not a feed of everything that ever happened. It is a stream of
changes **after a point in time**, and that point is a `resourceVersion`. So a
program that only watches never learns about the objects that already existed,
and a program that lists and then starts a fresh watch loses whatever changed
in between.

The pairing is the answer, and it is one operation in two calls: the list
returns the state *at* a version, the watch continues *from* that version.
Nothing is missed and nothing is seen twice.

The other half of the lesson is that a watch ends. Load balancers, API server
restarts and idle timeouts all cut it, and a closed channel is routine rather
than an error — you resume from the last version you saw. Occasionally the
server has discarded that far back and says so; then, and only then, you list
again.

## Go APIs

- `client.List(ctx, metav1.ListOptions{})` and `list.GetResourceVersion()`.
- `client.Watch(ctx, metav1.ListOptions{ResourceVersion: rv})`.
- `w.ResultChan()` yielding `watch.Event` with `Type` of `Added`, `Modified`,
  `Deleted` or `Error`.
- `signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)` — from here on the
  program runs until something stops it.

## Hints

<details><summary>Nudge</summary>

Track the resourceVersion of the last object you saw, not the one you started
with, or a reconnect replays everything since startup.
</details>

<details><summary>Approach</summary>

Wrap the watch in a loop. When the channel closes, open another one from the
version you have. Treat a `watch.Error` event as "my version is too old" and
list again from scratch.
</details>

<details><summary>Implementation</summary>

```go
for ctx.Err() == nil {
    w, err := client.Watch(ctx, metav1.ListOptions{ResourceVersion: rv})
    if err != nil {
        return err
    }
    rv, err = drain(ctx, w, rv) // "" means: too old, list again
}
```
</details>

## Further reading

- [Efficient detection of changes](https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes)
- [Resource versions](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions)
