---
title: Two writers, one field
concepts: [server-side-apply, field-manager, conflicts, force, managed-fields]
---

## Core concept

Last stage, every apply recorded which fields its manager set. Nothing used
that record yet except removal. This stage is what it is for. Picture a
Deployment whose `replicas` is applied from Git by one controller and scaled by
an autoscaler. Without ownership, each one quietly undoes the other on every
reconcile and neither ever finds out.

**An apply that would change a field someone else owns is refused, and the
refusal names them.**

```
alpha applies   {"data":{"colour":"blue"}}         → 201, alpha owns .data.colour
beta applies    {"data":{"colour":"red"}}          → 409 Conflict
  "Apply failed with 1 conflict(s): conflict with \"alpha\" using v1: .data.colour"
beta applies    ...?fieldManager=beta&force=true   → 200, beta owns .data.colour
```

**The rules, compared per leaf of the incoming config:**

- **Owned by another manager, different value** → conflict. Collect every
  conflicting leaf, then answer **409 `Conflict`** with nothing written: not
  the fields that did not conflict, and not managedFields. The message names
  each manager and field. `details.causes` holds one
  `{"type":"FieldManagerConflict","message":"conflict with \"alpha\"","field":".data.colour"}`
  per conflict.
- **Owned by another manager, same value** → no conflict. The field is now
  owned by both. If alpha later stops setting it, it stays, because beta still
  claims it.
- **`force=true`** → the applier's value wins, it owns the field, and the field
  is taken out of every other entry's `fieldsV1`. An entry left owning nothing
  is dropped.

**Ordinary writes own fields too.** A PUT or any of the three patch dialects
names its manager with `?fieldManager=`, or gets `unknown` if it does not.
Compare the object before and after: every leaf whose value changed, appeared,
or disappeared moves into that manager's entry, with `"operation": "Update"`.
A removed leaf is just dropped from every entry. Updates **never** conflict.
They are a person or an old client saying "this is the object now", and there
is nobody to ask. Compare old against new, not the body. A PUT sends every
field, and owning all of them would make every hand edit a takeover.

That is how an applier learns about a hand edit:

```
alpha applies colour=blue
PUT …?fieldManager=editor  colour=red     → 200, editor (Update) owns .data.colour
alpha applies colour=blue                 → 409, conflict with "editor"
```

`kubectl apply` stops there and tells you. `--force-conflicts` is `force=true`.

**Check under the lock.** The conflict check reads who owns what. If it reads
that before taking the lock, two appliers can both see a field nobody owns and
both succeed. The second then silently overwrites the first, which is the exact
thing this stage exists to prevent. Do the ownership check, the merge, and the
write in the same `modify` call you built for patches.

## Go APIs

- Turn `fieldsV1` into a `map[string]bool` of dotted paths (`.data.colour`) for
  the comparison, and back into the nested `f:` form to store it. Both are
  short recursive functions.
- Look up a leaf's current value by walking the path through the stored
  object, and compare with `reflect.DeepEqual`. A leaf can be a list.
- `metav1.Status` has `Details.Causes []StatusCause`, each with `Type`,
  `Message`, `Field`. Your Status type only needs the same JSON shape.
- `r.URL.Query().Get("force") == "true"`.

## Hints

<details><summary>Nudge</summary>

Before merging, walk the config's leaf paths. For each one, look through the
other entries for one that owns it. If the stored value differs, record a
conflict. Any conflicts and no force: return an error that your handler turns
into the 409, before anything is changed.
</details>

<details><summary>Approach</summary>

Inside `modify`, for an apply: compute the config's leaves; find conflicts
against every other entry, with any operation; refuse if there are some and no
force; with force, delete those leaves from the other entries. Then merge and
remove as in stage 20, and replace this manager's Apply entry. For an update:
diff the leaf sets and values of the old and new objects; delete every changed
leaf from every entry, then add the ones still present to this manager's
Update entry. Last, drop every entry whose `fieldsV1` is empty. Doing that in
one place catches both the forced loser and the overwritten applier.
</details>

## Further reading

- [Server-side apply: conflicts](https://kubernetes.io/docs/reference/using-api/server-side-apply/#conflicts)
- [Server-side apply: field management](https://kubernetes.io/docs/reference/using-api/server-side-apply/#field-management)
- [Transferring ownership](https://kubernetes.io/docs/reference/using-api/server-side-apply/#transferring-ownership)
