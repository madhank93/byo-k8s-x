---
title: One owner per field
concepts: [server-side-apply, managed-fields, field-manager, declarative-config, idempotency]
---

## Core concept

Every patch so far has been an instruction: change this, remove that. To take a
field away with a merge patch you have to know it is there and send `null`. A
tool that keeps an object matching a file in git cannot work like that — when
someone deletes a line from the file, the tool has nothing to send `null` for,
because the line is gone.

**Server-side apply sends the whole of what one client wants, and the server
remembers, per field, which client wanted it.** The memory is
`metadata.managedFields`, and it is what lets the server work out what the
client has stopped asking for.

```
PATCH …/configmaps/settings?fieldManager=alpha
Content-Type: application/apply-patch+yaml

{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"settings"},
 "data":{"colour":"blue","size":"large"}}

201 Created  → the object did not exist, so apply created it
200 OK       → it did, so apply merged into it
```

**The manager is required.** `?fieldManager=` names who is applying; missing or
empty is **400 `BadRequest`**. So is a body that is not an object, one without
`apiVersion` or `kind`, or one whose `metadata.name` or `metadata.namespace`
is not the URL's. JSON is
YAML, so the body is JSON — it is what kubectl sends.

**managedFields: one entry per manager and operation.**

```json
{"manager":"alpha","operation":"Apply","apiVersion":"v1",
 "fieldsType":"FieldsV1","time":"2026-10-01T09:00:00Z",
 "fieldsV1":{"f:data":{"f:colour":{},"f:size":{}}}}
```

`fieldsV1` is the config's shape with every key written `f:<key>`. A map is
walked into; anything else — a string, a number, a whole list — is a leaf,
`{}`. What says *which* object this is rather than what it holds is never
owned: `apiVersion`, `kind`, and `metadata`'s `name`, `namespace`, `uid`,
`resourceVersion`, `creationTimestamp` and `managedFields`. A container left
empty after dropping those is left out.

**An apply, in four steps, all under the store's lock:**

1. Merge the config into the live object: maps key by key, everything else
   replaced whole — a merge patch without `null`.
2. Find the leaves this manager's entry owned last time that the new config
   does not set. Remove each one **unless another manager also owns it**.
3. Replace this manager's entry with exactly the new config's leaves. An entry
   that owns nothing is dropped.
4. Store it — **unless the result is identical to what is there**.

Step 2 is the reason for all of it. A field another client also set is one that
client still wants; your config forgetting it is not a vote to delete it.

**Applying the same thing twice is not a write.** Controllers and GitOps tools
apply on a loop, whether or not anything changed. If each of those moved the
`resourceVersion` and woke every watcher, the cluster would spend its time
telling itself nothing happened. Same object out, no new version, no event —
and `time` in managedFields does not move either.

**What still holds:** a `metadata.resourceVersion` in the config is a
precondition, 409 `Conflict` when stale. And a PUT or patch whose body has no
`managedFields` keeps the stored ones: most clients decode into types without
the field, and one of them writing back must not erase everyone's record.

## Go APIs

- `r.URL.Query().Get("fieldManager")` — and `Has` is not enough: empty is
  missing.
- A recursive `func leaves(obj map[string]any, prefix []string, out *[][]string)`
  gives the owned set; building `fieldsV1` back from it is the same walk
  inverted.
- `reflect.DeepEqual(old, new)` is the whole no-op test. Compute managedFields
  with the *old* `time` first, compare, and only stamp `time.Now()` if they
  differ.

## Hints

<details><summary>Nudge</summary>

Write apply as one more `change func(old) (new, error)` passed to the store's
`modify` from stage 17. Everything it needs — the old object, the old
managedFields, the config — is in hand inside that function.
</details>

<details><summary>Approach</summary>

Under the lock: if the object is missing, start from an empty one and remember
to answer 201. Collect the old leaf set for (manager, Apply) and the union of
every *other* entry's leaves. Merge the config in. For each old leaf not in the
new config and not in the others' union, delete it from the object. Rebuild the
entry from the config's leaves, drop it if empty, and set it on the result. If
the result equals the stored object, return it without writing. In PUT and the
other patches, copy the stored managedFields onto the new object when the body
had none.
</details>

## Further reading

- [Server-Side Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
- [KEP-555: Server-side apply](https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/555-server-side-apply)
- [structured-merge-diff](https://github.com/kubernetes-sigs/structured-merge-diff)
