---
title: One element of a list
concepts: [json-patch, rfc-6902, json-pointer, atomicity, test-precondition]
---

## Core concept

A merge patch says what the object should look like. A **JSON patch says what
to do to it**: a list of operations, run in order, each aimed at one location.

```
PATCH /api/v1/namespaces/default/configmaps/settings
Content-Type: application/json-patch+json

[{"op":"test",    "path":"/metadata/resourceVersion", "value":"41"},
 {"op":"remove",  "path":"/metadata/finalizers/1"},
 {"op":"replace", "path":"/metadata/labels/app.kubernetes.io~1name", "value":"api"}]
```

**Six operations.** `add`, `remove`, `replace`, `move`, `copy`, `test`. The RFC
defines the middle ones in terms of the others — replace is remove-then-add,
move is remove-then-add somewhere else, copy is read-then-add — and writing
them that way is both shorter and correct.

- `add` on an object key sets it, existing or not. On a list index it
  **inserts before** that index; `-` means one past the end, i.e. append.
- `remove`, `replace` and `test` need the target to exist. Only `add` creates.
- `test` compares the value at the path with the operand and fails the patch
  if they differ. It is a precondition on any field you like — "only if the
  `resourceVersion` is still this" is the common one, and it is what a merge
  patch has no way to say.

**The path is a JSON pointer, RFC 6901.** `/` separates the tokens, each a key
or a list index. A `/` *inside* a key is written `~1`, and a `~` is written `~0`.
Split on `/` first, **then** unescape each token — the other order splits
`app.kubernetes.io~1name` into two keys. This is not an edge case: every label
and annotation with a domain prefix goes through it.

**All or nothing.** If operation four fails, operations one to three did not
happen. Nothing is written, the `resourceVersion` does not move, no watcher
sees anything. Apply the whole list to a copy, and only store the copy if every
operation succeeded — the `modify` from the last stage already gives you that,
as long as the copy is deep.

**Two different failures:**

- A body that is not a list of operations — an object, say, which is a merge
  patch under the wrong Content-Type — is **400 `BadRequest`**. The server could
  not read the request.
- A list the server read and could not apply — a failed `test`, a `remove` of
  something absent, an index past the end — is **422 `Invalid`**. The request
  was understood; the result it asks for is the problem.

## Go APIs

- Decode the body into `[]struct{ Op, Path, From string; Value any }`. An
  object where the list should be fails right there, which is your 400.
- `strings.Split(path[1:], "/")`, then
  `strings.NewReplacer("~1", "/", "~0", "~")` on each token.
- `slices.Insert` and `slices.Delete` for the list cases. Both return a new
  slice header, so whatever held the old one has to be given the new one.
- `reflect.DeepEqual` for `test`: both sides came out of `encoding/json`, so a
  number is a `float64` on each.

## Hints

<details><summary>Nudge</summary>

Everything is one recursive walk: follow the tokens down to the container the
*last* token lives in, and do the operation there. Add, remove and get differ
only in what they do at the bottom.
</details>

<details><summary>Approach</summary>

Write `walk(doc, path, leaf)`, which descends through every token but the last
and calls `leaf(container, lastToken)`, putting whatever `leaf` returns back
where the container was (appending to a slice makes a new one). Build `add`,
`remove` and `get` as three small leaf functions over `map[string]any` and
`[]any`. Then each of the six ops is a line or two on top of those three. Run
the ops in a loop over a deep copy, returning on the first error wrapped as
something your handler turns into 422.
</details>

## Further reading

- [RFC 6902: JSON Patch](https://www.rfc-editor.org/rfc/rfc6902)
- [RFC 6901: JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901)
- [kubectl patch --type=json](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/#use-a-json-patch)
