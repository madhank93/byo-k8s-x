---
title: A collection too big for one answer
concepts: [chunking, continue-token, cursors, remainingItemCount, compaction]
---

## Core concept

Every list so far has been the whole collection. On a real cluster that is how
an apiserver falls over: `kubectl get pods -A` on a big cluster means the server
loads every pod, serialises every pod, and sends every pod, in one response it
holds in memory the whole time. **Paging turns one enormous answer into a
sequence of bounded ones.**

```
GET  …/configmaps?limit=2                     → a, b   + metadata.continue, remainingItemCount: 3
GET  …/configmaps?limit=2&continue=<token>    → c, d   + metadata.continue, remainingItemCount: 1
GET  …/configmaps?limit=2&continue=<token>    → e      + no continue
```

The last page is told apart by the **absence** of `continue`, not by a short
page. A client that is always handed a cursor pages for ever.

**The token is opaque to the client and meaningful to the server.** It carries
where to resume — the last key sent — and the `resourceVersion` the list was
started at. The client stores it and sends it back untouched, which is what
leaves the server free to change its contents. The real one is base64 JSON of
exactly that pair.

**Resume *after* the last key, not at it**, or every page repeats one object.

**Every page of one list reports the same `resourceVersion`.** A paged list is
one answer delivered in instalments, taken at one point in the store's history —
so the version comes out of the cursor, not out of wherever the store has got to
by the time page three is asked for. This is what lets an informer list in
chunks and then watch from that one number.

**Selecting comes first, paging second.** `limit` bounds what is *sent*, not
what is *examined*. Filter then cut, or a selector matching 3 of 10,000 objects
sends a client through 5,000 pages of nothing. And the selectors travel with the
cursor: the client sends them again on every page.

**410 Gone is the one that surprises people.** A real server serves later pages
by reading etcd at the revision in the token, and etcd keeps only about five
minutes of history. A client that is slow between pages gets `410 Gone`,
`reason: Expired`, and has to start the list again. This server keeps no history
at all, so it serves later pages from the store as it is now and answers 410
only for a cursor from a version it has never reached — but the client contract
is the same one, which is why client-go restarts a chunked list on `Expired`.

**A cursor the server cannot read is a 400**, not "start again". Silently
restarting hands a paging client the first page repeatedly, which looks like the
collection being much larger than it is.

## Go APIs

- `base64.RawURLEncoding` — a cursor travels in a query string, so no `+`, `/`
  or `=` padding.
- `strconv.Atoi` for `limit`; reject negatives, and treat absent as "no limit"
  rather than zero-meaning-none-sent.
- The cursor can only point at something the list is ordered by, which here is
  the registry key — `registryKey(resource, namespace, name)` rebuilt from the
  object's own metadata.
- `remainingItemCount` is a number in the JSON; Go decodes it as `float64` on
  the other side, which is worth knowing when you assert on it.

## Hints

<details><summary>Nudge</summary>

Two query parameters, read before the store is touched, and three lines after
the items have been filtered. The store does not change in this stage either.
</details>

<details><summary>Approach</summary>

Define a token struct of `{rv, start}`, encode it as base64 JSON. In the list
handler: parse `limit` and `continue` (400 on either being unreadable); list and
apply the selectors as before; if there is a token, take its version as the
list's version and drop every item whose key is `<=` its start; then if `limit >
0` and more items are left than that, set `metadata.continue` from the last item
of the page and `metadata.remainingItemCount` to what is left over, and truncate.
No cursor otherwise.
</details>

## Further reading

- [API concepts: retrieving large results sets in chunks](https://kubernetes.io/docs/reference/using-api/api-concepts/#retrieving-large-results-sets-in-chunks)
- [etcd: compaction and revisions](https://etcd.io/docs/latest/op-guide/maintenance/)
- [KEP-365: paginated API lists](https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/365-paginated-api-lists/README.md)
