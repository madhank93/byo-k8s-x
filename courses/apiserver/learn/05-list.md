---
title: All of them, in order
concepts: [list, listmeta, resourceversion, empty-collections]
---

## Core concept

A collection endpoint answers with a **List object**, not a JSON array.

```
GET /api/v1/namespaces/default/configmaps
200 OK
{"kind":"ConfigMapList","apiVersion":"v1",
 "metadata":{"resourceVersion":"7"},
 "items":[ …whole objects… ]}
```

Three things about that shape matter more than they look.

**`kind` is `<Kind>List`.** A client decides how to decode what came back by
reading the kind out of it. A bare `[…]` has nowhere to put that, which is why
no endpoint in Kubernetes answers with a naked array.

**The list has its own `resourceVersion`, and it is not any item's.** It is the
point in the store's history this answer describes. A client lists, remembers
that number, then opens a watch with `?resourceVersion=<it>` — and sees every
write that happened after the list and none that were already in it. That is the
difference between a watch and a poll, and it is the reason `resourceVersion`
lives on the store rather than on the objects. An informer's entire cache is
built from one list plus the watch that continues it.

**`items` is `[]` when there is nothing, never `null` and never missing.** A
namespace nothing has been stored in lists as an empty collection with a 200 —
there is no "that namespace does not exist yet" at a collection endpoint. In Go
this is the difference between `items := []object{}` and `var items []object`;
the second marshals to `null`, and the client on the other side is one
dereference away from a crash.

**The order is yours to decide, so decide it.** Sort by name. The real server
gets this for free — etcd hands back a key range already sorted — and anything
that prints a list depends on it being the same twice in a row. Go's map
iteration order is deliberately random, so a list built straight out of the map
is a different answer every request.

**A list is namespace-scoped here.** Objects in another namespace are not in
this answer. (`kubectl get cm -A` hits `/api/v1/configmaps`, without a namespace
in the path; that endpoint arrives with the `namespaces` stage.)

## Go APIs

- `sort.Slice` with a comparison on `metadata.name`. A tiny helper that reads one
  metadata string saves writing the same two type assertions in five places, and
  the later stages use it constantly.
- `strings.Cut(key, "/")` to split a stored key back into namespace and name.
- `mux.HandleFunc("GET /api/v1/namespaces/{namespace}/configmaps", …)` — the
  collection, one segment shorter than the object.

## Hints

<details><summary>Nudge</summary>

The list needs two things out of the store under one lock: the matching objects
and the version. Return both.
</details>

<details><summary>Approach</summary>

`list(namespace) ([]object, int64)`: start with an empty (not nil) slice, walk
the map keeping the keys whose namespace half matches, sort by name, and return
the slice with `s.version`. The handler wraps it in the List object and writes
200.
</details>

## Further reading

- [API concepts: resource versions](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions)
- [Efficient detection of changes](https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes)
