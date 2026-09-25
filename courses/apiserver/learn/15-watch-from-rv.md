---
title: Catching up without starting over
concepts: [resourceVersion, list-then-watch, informers, expired, compaction]
---

## Core concept

A watch that sends the whole current state before it starts is fine once. A
client that reconnects every few minutes and is handed the entire collection
each time is a poll with extra steps. **The client says what it already has, and
the server sends only what happened since.**

```
GET …/configmaps                          → items + metadata.resourceVersion: "6"
GET …/configmaps?watch=true&resourceVersion=6
                                          → only the changes after version 6
```

This pair — **list, then watch from the list's own `resourceVersion`** — is
every informer in Kubernetes. Nothing is repeated, nothing is missed, and there
is no window in between. It is why the list has carried a version of its own
since stage 5.

**`resourceVersion=0` is not version zero.** It means "whatever you have
already, and do not make me wait for it": the client has nothing, so the current
state comes back as `ADDED`. Read as a version to resume after, it would replay
the whole history — including deletes of objects that no longer exist, which is
a client being told about an object it has never seen.

**Replay then stream, on one connection.** The missed changes run straight into
the live ones. A client that had to reconnect after catching up would have a
window it sees nothing through, which is the bug this whole design exists to
avoid.

**`410 Gone`, `reason: Expired`.** The server keeps a bounded history — here a
buffer of recent changes, in the real one whatever etcd has not compacted, a few
minutes. A client resuming from older than that cannot be caught up, and the
only honest answer is "start again": it drops its cache, lists, and watches from
the new version. Answering with the current state instead is the dangerous
failure — the client carries on believing it missed nothing, and quietly holds
objects that were deleted while it was away. `Too old resource version` is the
most-seen line in controller logs for this reason, and it is not a bug.

**A restart is a compaction.** The objects were written down; the changes were
not. After a restart the oldest version this server can replay from is the one
it loaded, so anything older is `Expired`.

**A version ahead of this server is a `504`, not a 400.** The client may have
read it from another apiserver a moment ago. "Ask again" is the right answer —
the same rule as reads in stage 9.

## Go APIs

- Keep history as a slice of events with a floor version: append on publish,
  trim past a limit, and raise the floor to the last event you dropped.
- Register the watcher, check the floor, and slice the history **under one
  lock**, exactly as with the snapshot in the last stage.
- `errors.Is(err, errExpired)` at the handler, mapping to `410` and
  `reason: Expired` — client-go's `apierrors.IsResourceExpired` reads that field.
- The version parse can assume a valid number: the check that rejects garbage
  and futures already runs before it.

## Hints

<details><summary>Nudge</summary>

The store needs to remember recent events, not only broadcast them. The handler
needs one more branch: the client either has nothing or has a version.
</details>

<details><summary>Approach</summary>

Add `history []watchEvent` and `floor int64` to the store. `publish` appends and
trims; `load` sets `floor = version`, since nothing before this process can be
replayed. Change the watch-open method to take the version the client has: `-1`
means snapshot as before; otherwise refuse with `errExpired` when it is below the
floor, and collect the history entries newer than it. In the handler, read
`resourceVersion` (absent or `0` → `-1`), map `errExpired` to `410 Expired`, send
the replay through the same filters as the live events, then enter the loop.
</details>

## Further reading

- [API concepts: resource versions](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions)
- [client-go Reflector: `ListAndWatch`](https://github.com/kubernetes/client-go/blob/master/tools/cache/reflector.go)
- [etcd compaction](https://etcd.io/docs/latest/op-guide/maintenance/#history-compaction)
