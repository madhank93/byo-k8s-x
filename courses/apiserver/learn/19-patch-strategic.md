---
title: Lists that belong to more than one writer
concepts: [strategic-merge-patch, patch-merge-key, finalizers, owner-references, directives]
---

## Core concept

Two stages ago, a merge patch replaced `metadata.finalizers` whole. Think about
who writes that list. A finalizer is one controller saying "do not delete this
until I have cleaned up"; three controllers can each have one on the same
object. If each of them adds its own with a merge patch, each one erases the
other two — and an object whose finalizers were erased gets deleted before the
cleanup ran.

**A strategic merge patch is a merge patch that knows, per field, whether a
list is a value, a set, or a list of things with identities.** It is what
`kubectl patch` sends when you do not say `--type`, and what client-side
`kubectl apply` is built on.

```
PATCH …/configmaps/settings
Content-Type: application/strategic-merge-patch+json

{"metadata":{"finalizers":["example.com/c"]}}

before: ["example.com/a","example.com/b"]
after:  ["example.com/a","example.com/b","example.com/c"]
```

**Three strategies, chosen by field:**

- **Replace** — the default, and exactly what a merge patch does. Every list
  not declared otherwise.
- **Merge as a set** — for lists of plain values. `metadata.finalizers` is one.
  Values the patch lists are added if absent; values it does not list stay.
- **Merge by key** — for lists of objects. `metadata.ownerReferences` is keyed
  by `uid`. An element whose `uid` is already there is merged into that
  element (recursively, with these same rules); a new `uid` is appended; the
  rest are left alone.

The real server reads the strategy from struct tags on each Go type —
`patchStrategy:"merge" patchMergeKey:"uid"`. That is why a strategic patch works
only on built-in resources: a custom resource has no Go type in the server, and
is refused with 415. A table of paths does the same job here.

**Maps are unchanged from a merge patch.** Keys merge, `null` removes.

**Merging means leaving a value out keeps it — so removal needs a directive.**
Directives are keys starting with `$`. They are instructions to the server, and
**never stored**:

- `{"uid":"…","$patch":"delete"}` inside a keyed list removes the element with
  that key.
- `"$deleteFromPrimitiveList/finalizers": ["example.com/a"]`, beside
  `finalizers` in the same object, removes those values from the set.
- `"$setElementOrder/finalizers": […]` says what order the client wants the
  merged list in. kubectl sends it on almost every patch. You may honour it or
  not; you may not store it.

**An element of a keyed list without its key is 422 `Invalid`.** There is no
telling which element it means, and appending it would be a guess.

## Go APIs

- A `map[string]string` from dotted path to merge key (`""` for a set) is the
  whole strategy table. Build the path as you recurse.
- `strings.HasPrefix(key, "$")` catches every directive; handle the two you
  act on first, and skip the rest.
- `slices.ContainsFunc`, `slices.IndexFunc`, `slices.DeleteFunc` with
  `reflect.DeepEqual`. Plain `==` on two `any` holding maps panics.

## Hints

<details><summary>Nudge</summary>

Start from your merge-patch function and add a path argument. The only new
behaviour is at a list value: look its path up, and pick one of three ways to
combine it with what is there.
</details>

<details><summary>Approach</summary>

In the loop over the patch's keys: a `$deleteFromPrimitiveList/<field>` key
filters that field's list; any other `$` key is skipped; `null` deletes; a list
whose path is declared as a set gets each missing value appended; a list
declared with a merge key walks the patch's elements, finds the stored element
with the same key, and either deletes it (`$patch: delete`), merges into it
with a recursive call, or appends; anything else recurses as before. A missing
key on an element is an error your handler turns into 422.
</details>

## Further reading

- [Strategic merge patch](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-api-machinery/strategic-merge-patch.md)
- [kubectl patch: notes on the strategic merge patch](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/#notes-on-the-strategic-merge-patch)
- [Finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
