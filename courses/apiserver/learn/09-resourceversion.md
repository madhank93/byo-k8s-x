---
title: How fresh an answer has to be
concepts: [resourceversion, consistency, quorum-read, timeout]
---

## Core concept

`?resourceVersion=` on a read is the most misread parameter in the API. It looks
like a filter. It is a **freshness floor**.

```
GET /api/v1/namespaces/default/configmaps/settings?resourceVersion=7
```

This does **not** mean "the object as it was at version 7". It means "do not
answer me with a cluster older than version 7". What comes back is the object as
it is **now**. Nothing in the Kubernetes API can show you an old version of an
object — there is no time travel, and the history a watch replays is a stream of
changes, not a set of snapshots, and it expires.

Why would a client ever send one? Because a real cluster has several apiservers
in front of one etcd, and their caches are not in lockstep. A client that just
wrote version 7 and reads through another replica could be told its own write
never happened. Sending the version it already knows about turns that into an
error it can retry instead of a wrong answer it would believe.

The rules:

| sent | means |
|---|---|
| absent | the most recent, read through the store — the strongest and dearest |
| `0` | anything you already have; do not wait. The cheap read, and what an informer's first list uses |
| *n* ≤ current | fine: the server is already at least that fresh, answer with what is current |
| *n* > current | **504 `Timeout`**, `Too large resource version: n, current: m` |
| not a number | **400 `BadRequest`** |

**The 504 is the interesting one, and it is not a rejection.** The client is not
wrong — it may have read that number from a replica a moment ago — so the answer
is "ask me again", not "no such thing". That is why it is a timeout rather than a
404 or a 422, and why the real server sets `Retry-After` on it.

**Quietly ignoring what you cannot parse is the worst option.** A server that
drops a `resourceVersion` it does not understand answers a question nobody asked,
and the client has no way to find out. 400 is a kindness.

**Whatever a list says, the server must be able to answer.** A list reports its
own `resourceVersion`; a client's next request uses that number. A server that
reports a version it then refuses leaves that client nowhere to start.

One place decides all of this, and every read goes through it — that is what
makes the watch stages possible without repeating any of it.

## Go APIs

- `r.URL.Query().Get("resourceVersion")`, `strconv.ParseInt`.
- One helper returning a bool and writing the failure itself, called at the top
  of each read handler: `if !freshEnough(w, r, objects) { return }`.
- `http.StatusGatewayTimeout` (504) with reason `Timeout`;
  `http.StatusBadRequest` (400) with reason `BadRequest`.

## Hints

<details><summary>Nudge</summary>

The store needs one more read-only method — how far it has got — and the
comparison is against that.
</details>

<details><summary>Approach</summary>

`freshEnough(w, r, s)`: empty parameter → true. Parse it; a parse error or a
negative number → 400, false. Compare with `s.currentVersion()`; greater → 504
`Timeout` with the message the real server uses, false. Otherwise true, and the
handler answers with current state as it already did. Call it from the object
read and from both collection reads.
</details>

## Further reading

- [API concepts: resource versions](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions)
- [Semantics for get and list](https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-get-and-list)
