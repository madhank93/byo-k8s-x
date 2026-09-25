---
title: Nothing happened, and here is proof
concepts: [bookmarks, watch-progress, thundering-herd, opt-in-protocol]
---

## Core concept

A watch on a busy resource keeps its client's `resourceVersion` current for
free: every event carries one. A watch on a **quiet** resource does not. The
cluster moves on, the history the server keeps rolls forward, and the version
this client is holding gets older and older — until it reconnects and is told
`410 Gone`, and has to list everything again.

**A bookmark is an event that carries no object and says only "you have seen
everything up to version N".**

```
GET …/configmaps?watch=true&allowWatchBookmarks=true

{"type":"BOOKMARK","object":{"apiVersion":"v1","kind":"ConfigMap",
                             "metadata":{"resourceVersion":"417"}}}
```

**The failure it prevents is collective.** One idle watcher relisting is
nothing. Every idle watcher in a cluster relisting at once, because they all
fell off the same history window, is an apiserver that stops answering anything
else. Bookmarks cost a few bytes per watch per minute and remove that mode
entirely.

**Opt-in, always.** Only a client that sent `allowWatchBookmarks=true` may be
sent one. A client whose decoder does not know the type receives an event it
cannot read, which is worse than no event — so the flag is the client saying it
understands the protocol.

**No object in it.** Just `apiVersion`, `kind` and `metadata.resourceVersion`.
There is no change here to react to; a controller that treats a bookmark as an
object update will reconcile something that has not moved.

**Between events, not instead of them.** The bookmark is extra traffic on an
idle watch, and it never replaces or reorders a real change.

**How often.** Here: within five seconds of the store moving past what this
watch has been sent. The real server is roughly a minute, jittered so that ten
thousand watches do not all get one on the same tick — and it only sends one
when there is something new to report.

## Go APIs

- `time.NewTicker` plus a third `case` in the `select`, and `defer
  ticker.Stop()`. A `nil` channel blocks for ever, which is exactly what you
  want when bookmarks were not asked for — no branch needed in the loop.
- Track the highest version actually sent to *this* client; the bookmark is
  worth sending only when the store is past it.
- `strconv.ParseBool` for the flag, as with `watch`.
- The event's `kind` is the resource's kind, not the list's — trim the `List`.

## Hints

<details><summary>Nudge</summary>

The store does not change at all in this stage. Everything is in the streaming
loop: one more channel to select on, and one number to remember.
</details>

<details><summary>Approach</summary>

Keep a `latest` in the handler, starting at the version the watch opened at and
updated on every event sent. If `allowWatchBookmarks` parses true, make a ticker
and put its channel in the `select`; otherwise leave that channel `nil`. On a
tick, read the store's current version, and if it is greater than `latest`, send
a `BOOKMARK` whose object is `apiVersion`, `kind` and a `metadata` holding only
that version — then set `latest` to it.
</details>

## Further reading

- [API concepts: watch bookmarks](https://kubernetes.io/docs/reference/using-api/api-concepts/#watch-bookmarks)
- [KEP-956: watch bookmarks](https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/956-watch-bookmark/README.md)
- [`ListOptions.AllowWatchBookmarks`](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#ListOptions)
