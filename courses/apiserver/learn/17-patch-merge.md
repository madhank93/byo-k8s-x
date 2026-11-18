---
title: Saying only what changed
concepts: [patch, json-merge-patch, rfc-7386, lost-update, content-type]
---

## Core concept

A PUT carries the whole object, so it carries a copy of every field the client
did not mean to touch — as they were when it read them. That copy is what goes
stale. Stage 6 made the server refuse a stale PUT; this stage gives clients a
write that has nothing stale in it to begin with.

**A PATCH says only what changed, and the server applies it to what is stored
at the moment of the write.**

```
PATCH /api/v1/namespaces/default/configmaps/settings
Content-Type: application/merge-patch+json

{"data":{"colour":"green","size":null}}

200 OK   → the whole stored object, colour green, size gone, everything else kept
```

**The Content-Type is the dialect.** There are three, and the same body means
different things in each. This stage is `application/merge-patch+json`, RFC 7386;
the next two add JSON Patch and the strategic merge patch. Anything else —
`application/json` included — is **415 `UnsupportedMediaType`**. Guessing at a
dialect is guessing at what the client wanted changed.

**The merge rules, all of them:**

- An object in the patch is merged into the stored object key by key,
  recursively. Keys the patch does not mention are kept.
- `null` **removes** the key. It is never stored. It is the only way this dialect
  has to delete anything.
- Anything else — a string, a number, **a list** — replaces what was there,
  whole.

**Lists are replaced, and that is the dialect's limit.** A merge patch cannot
say "this element of the list"; it can only say "the list is now this". For
`metadata.finalizers`, where several controllers each own one entry, that means
a client adding its own removes everyone else's. The strategic merge patch, two
stages on, exists to fix exactly this.

**Apply it inside the write.** This is the part graded hardest. A server that
reads the object, merges, then writes back has rebuilt the stale-PUT problem on
the server side: two patches to *different* keys arrive together, both read the
same version, and one of them is lost — or refused with a 409 the client did
nothing to deserve. The merge has to happen under the same lock as the write,
against whatever is stored when the lock is taken. The real server does it with
a retry loop (`GuaranteedUpdate`) rather than a lock; the effect is the same.

**A `resourceVersion` in the patch is still a precondition.** Most patches leave
it out and mean "apply this to whatever is there". One that includes it is
asking for exactly that version, and a mismatch is a 409 as in stage 6.

**Everything else from update still holds.** The reply is the stored object,
the `resourceVersion` moves, `uid` and `creationTimestamp` are kept, a missing
object is a 404 — and watchers see a `MODIFIED`. Add `patch` to the verbs in
discovery, or kubectl will not send one.

## Go APIs

- `mime.ParseMediaType(r.Header.Get("Content-Type"))` — a client may add
  `; charset=utf-8`, and a string compare would refuse it.
- Decode the patch into `any`, not a struct: it is a partial object, and `nil`
  in the decoded map is the JSON `null` you need to see.
- The merge is a short recursive function over `map[string]any`.
- Work on a deep copy of the stored object. A marshal/unmarshal round trip is
  the simplest correct one; a shallow `maps.Clone` shares every nested map with
  the store, so a patch that is later refused has already changed it.

## Hints

<details><summary>Nudge</summary>

Your store's update is almost right already. What it needs is to take a
*change* — a function from the old object to the new one — instead of the new
object, and to call it while it holds the lock.
</details>

<details><summary>Approach</summary>

Give the store a `modify(resource, namespace, name, change func(old) (new,
error))` that looks the object up, calls `change` under the lock, then does
everything update did: the `resourceVersion` check, identity, the new version,
the write, the event. Update becomes `modify` with a change that ignores the old
object. The PATCH handler checks the media type, decodes the body, and passes a
change that deep-copies the old object and merges the patch into it.

The `resourceVersion` check needs nothing new: a merged object carries the
stored version unless the patch set a different one.
</details>

## Further reading

- [RFC 7386: JSON Merge Patch](https://www.rfc-editor.org/rfc/rfc7386)
- [API concepts: patch operations](https://kubernetes.io/docs/reference/using-api/api-concepts/#patch-and-apply)
- [Update API objects in place using kubectl patch](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/)
