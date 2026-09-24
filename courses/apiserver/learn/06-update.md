---
title: The write that can lose a race
concepts: [update, optimistic-concurrency, conflict, immutable-identity]
---

## Core concept

A PUT replaces a stored object. That sentence hides the most important
mechanism in the API server, so it is worth being slow about.

```
PUT /api/v1/namespaces/default/configmaps/settings
{"apiVersion":"v1","kind":"ConfigMap",
 "metadata":{"name":"settings","resourceVersion":"1"},
 "data":{"colour":"green"}}

200 OK   → the stored object, with resourceVersion "2"
```

**200, not 201.** A client that asked to update an object it had just read would
otherwise have to work out which of the two things happened.

**Replace, not merge.** Everything the body does not carry is gone. `data` sent
without a key deletes that key. Merging is what the patch stages are for, and
the distinction is exactly why `kubectl edit` sends the whole object back.

**`resourceVersion` is optimistic concurrency, and this is what it is for.**
Two clients read the same object. Both change one field. Both write back. Without
a version check the second write silently erases the first, and nothing anywhere
reports it — you find out later that a field you set is not set. So:

- the body carries the `resourceVersion` the client read,
- the server compares it with what is stored,
- a mismatch is **409 `Conflict`**, and the store is left alone.

A controller hitting that 409 does not fail: it re-reads, re-applies its change
to what is there now, and tries again. `RetryOnConflict` in client-go is that
loop, and the whole reconcile model depends on this server refusing rather than
overwriting.

**An empty `resourceVersion` is an unconditional write.** The client is saying
"whatever is there, make it this" — that is `kubectl replace`, and the risk is
being taken on purpose rather than by accident. Allow it.

**Identity is yours, permanently.** `uid` and `creationTimestamp` are set once,
at create. A client is free to send back values it invented; the server is not
free to believe them. Keep the stored ones.

**The name in the URL wins nothing — a disagreement is a 400.** An update is not
a rename. If the body names a different object than the path does, there is no
right answer to pick: whichever the server chose, it would be writing to a URL
nobody asked about.

**A PUT to a name nothing is stored under is a 404.** (The real server can
create through a PUT for some resources; a client that sent an update still
expects to be told the thing it was updating is gone.)

## Go APIs

- `mux.HandleFunc("PUT /api/v1/namespaces/{namespace}/configmaps/{name}", …)`.
- `errors.Is` with two sentinel errors out of the store — not-found and
  conflict — so the handler maps each to its own code and reason. The store
  decides what happened; the handler decides how to say it over HTTP.
- The version comparison is string equality, and only equality.
  `resourceVersion` is opaque to everyone but the store that issued it: never
  parse it, never compare it for order, never do arithmetic on it. The real one
  is an etcd revision, and treating it as a number is a bug that survives
  testing for months.

## Hints

<details><summary>Nudge</summary>

The create and the update are nearly the same function: look up the key, refuse
for opposite reasons, bump the counter, stamp metadata, store. Write the update
as its own method rather than adding a flag to the create.
</details>

<details><summary>Approach</summary>

`update(namespace, name, obj) (object, error)` under the lock: fetch the old
object or return not-found; if the body names a resourceVersion and it differs
from the stored one, return conflict; bump the counter; carry the old `uid` and
`creationTimestamp` onto the new metadata with the fresh `resourceVersion`;
store and return. Both writes now share one decode helper — the JSON, and the
namespace in the body agreeing with the one in the path.
</details>

## Further reading

- [API concepts: concurrency control and consistency](https://kubernetes.io/docs/reference/using-api/api-concepts/#concurrency-control-and-consistency)
- [`RetryOnConflict`](https://pkg.go.dev/k8s.io/client-go/util/retry#RetryOnConflict)
