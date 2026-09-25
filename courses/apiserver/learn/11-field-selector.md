---
title: Only the ones that match
concepts: [field-selectors, server-side-filtering, indexes, status-errors]
---

## Core concept

A list is the most expensive answer this server gives, and until now the only
way to narrow one has been the namespace in the URL. **A field selector is the
client saying which of them it actually wants, and the server answering only
those.**

```
GET /api/v1/namespaces/default/configmaps?fieldSelector=metadata.name=alpha
GET /api/v1/configmaps?fieldSelector=metadata.namespace=kube-public
GET /api/v1/namespaces/default/configmaps?fieldSelector=metadata.name!=alpha
GET /api/v1/configmaps?fieldSelector=metadata.namespace=default,metadata.name=beta
```

**Why on the server at all.** The kubelet watches pods with
`spec.nodeName=<its own node>`. Filtering on the client would mean every pod in
the cluster being serialised, sent and decoded on every node, for the handful
that node runs. The selector is what makes that watch proportional to the node
rather than to the cluster.

**The syntax is deliberately small.** A term is a field, an operator and a
value; the operators are `=`, `==` and `!=`; the comma between terms is an
**AND**. There is no OR, no substring, no comparison — a client that wants a
union sends two requests. And there is no existence form: `metadata.name` on its
own is not a term, because every object has one.

**Only indexed fields are selectable.** This is the part worth keeping. A field
selector is answered out of what the registry indexes, not by opening every
object, so a field the server does not index is a **400**, not a scan and not a
shrug. `metadata.name` and `metadata.namespace` work on every resource; the rest
are declared per resource in the real server — `status.phase` and
`spec.nodeName` on pods, `type` on secrets — which is why
`--field-selector spec.replicas=3` on a Deployment fails and surprises people.

**Refusing is the whole point.** A server that ignored a selector it could not
answer would hand back the entire collection with a 200, and the client would
act on all of it: a controller told to reconcile one object reconciling
everything. Silently widening a filter is worse than failing.

**Filtering does not change the envelope.** The answer is still a
`ConfigMapList` with the list's own `resourceVersion`; only `items` is shorter.
And a selector nothing matches is `200` with an empty list — the collection
exists, the filter is the client's question.

## Go APIs

- `r.URL.Query().Get("fieldSelector")` — absent is `""`, which means match
  everything.
- Parse into a `func(object) bool` rather than filtering inline: the next stage
  adds a second selector, and both narrow the same list.
- Split operators longest-first (`!=`, then `==`, then `=`) or `!=` reads as `=`
  with a key ending in `!`.
- `http.ServeMux` returns `""` from `r.PathValue("namespace")` when the pattern
  has no such wildcard — which is already what the store reads as "every
  namespace", so one handler can serve both URL shapes.

## Hints

<details><summary>Nudge</summary>

One handler function for all three list URLs, and one parse step before the
store is touched. The store does not change at all in this stage.
</details>

<details><summary>Approach</summary>

Write `parseFieldSelector(raw) (func(object) bool, error)`: split on commas,
split each term into key/op/value, reject any key that is not `metadata.name` or
`metadata.namespace`, and build a test per term that compares `metaString(obj,
field)` and inverts it for `!=`. AND them. In the list handler, call it first and
answer `400 BadRequest` with the error as the message, then list and keep the
items the test accepts.
</details>

## Further reading

- [Field selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/)
- [API concepts: efficient detection of changes](https://kubernetes.io/docs/reference/using-api/api-concepts/)
- [kubectl: `--field-selector`](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_get/)
