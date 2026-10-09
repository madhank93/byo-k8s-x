---
title: The first thing worth storing
concepts: [create, uid, resourceversion, alreadyexists, validation]
---

## Core concept

A create is not an echo. The client sends a name and some data; what comes back
is the *stored object*, carrying three things only the server could know.

```
POST /api/v1/namespaces/default/configmaps
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"settings"},"data":{"colour":"blue"}}

201 Created
{"apiVersion":"v1","kind":"ConfigMap",
 "metadata":{"name":"settings","namespace":"default",
             "uid":"5f2c…","creationTimestamp":"2026-09-24T09:12:03Z",
             "resourceVersion":"1"},
 "data":{"colour":"blue"}}
```

**`uid`** is identity. A name is a slot, and the slot can be emptied and
refilled; the uid is what makes the second `settings` a *different object* to
everything that referenced the first. Owner references are uid-based for this
reason — the controller course's "adopt an existing child" only works because a
name alone is not proof.

**`creationTimestamp`** is RFC 3339, to the second, in UTC. Clients parse it.

**`resourceVersion`** is the one that matters most and looks like the least. It
is not per-object — it is a counter over **every** write the server makes, taken
under the same lock as the write itself. The number on this object means "the
store as it was at this point in its history". That single fact is what makes
`?resourceVersion=` on a watch answerable, and it is why the field is opaque to
clients: they may compare it for equality, never for order, and never do
arithmetic on it.

**The namespace comes from the URL.** If the body names one too, they have to
agree — otherwise the URL a client was authorized against is not the one it
wrote to. Reject the mismatch rather than picking a winner.

**Two failures are worth getting exactly right**, because clients are written
against them:

- **409 `AlreadyExists`** when the name is taken. A create that silently
  overwrites has thrown away an object someone else is using; every controller
  that does "create, and treat conflict as fine" depends on this.
- **422 `Invalid`** when the object cannot be accepted — no `metadata.name`,
  say. Not a 400: the request was understood perfectly, and it is the object
  inside it that is wrong. 400 means *you sent me nonsense*, 422 means *I read
  it, and no*.

## Go APIs

- `mux.HandleFunc("POST /api/v1/namespaces/{namespace}/configmaps", …)` and
  `r.PathValue("namespace")`. The wildcard is Go 1.22+; no router needed.
- Decode into `map[string]any` rather than a struct. A typed scheme with
  conversion is the real server's answer and about a thousand lines of it;
  unstructured JSON is honest for this course and makes patching, in a later
  stage, straightforward instead of reflective.
- `crypto/rand` for the uid. Sixteen random bytes formatted 8-4-4-4-12 is a
  UUID v4 in every way that matters here.
- `time.Now().UTC().Format(time.RFC3339)`.
- One `sync.Mutex` around the map *and* the counter. They are one thing: a
  version handed out for a write that has not happened yet, or taken twice for
  two writes, breaks the ordering every reader depends on.

## Hints

<details><summary>Nudge</summary>

Write the store before the handler. It needs a map, a counter, and one method
that refuses a name it already holds.
</details>

<details><summary>Approach</summary>

`create(namespace, name, obj)` under the lock: refuse if the key is there,
bump the counter, stamp name, namespace, uid, creationTimestamp and
resourceVersion into `metadata`, store, return. The handler decodes, validates
the name and the namespace, calls it, and writes 201 with whatever came back.
</details>

## Further reading

- [API concepts: resource versions](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions)
- [Object names and uids](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/)
