---
title: Patch by operation
concepts: [JSON patch, RFC 6902, JSON pointer, patch targets]
---

## Core concept

A strategic merge patch says what the result should look like. A JSON patch
(RFC 6902) says what to *do*: a list of operations, each on a path.

```yaml
patches:
- target:
    kind: Deployment
    name: web
  patch: |-
    - op: replace
      path: /spec/replicas
      value: 3
    - op: add
      path: /spec/template/spec/containers/0/args/-
      value: --verbose
```

| op | does |
|---|---|
| `add` | sets a map key, or inserts into a list at an index (`-` is the end) |
| `remove` | deletes a key or a list item, which must be there |
| `replace` | swaps the value at a path |
| `move` | removes `from`, then adds its value at `path` |
| `copy` | adds a copy of `from` at `path` |
| `test` | fails the whole build unless the value at `path` equals `value` |

Paths are JSON pointers: `/`-separated keys, list indexes as numbers, and
inside a key `~1` stands for `/` and `~0` for `~` — so the annotation
`example.com/owner` is `/metadata/annotations/example.com~1owner`.

Operations run in order, each on the result of the last. Every step but the
last must already exist: `add` will not create `/spec/strategy` on the way to
`/spec/strategy/type`. kustomize's implementation lets `replace` add a missing
*map key*, but an index past the end of a list is an error for every
operation.

The patch body is YAML or JSON — a list either way. That list shape is how
kustomize tells a JSON patch from a strategic merge one in the same `patches`
field. A JSON patch has no `kind` or `name` of its own, so it must have a
`target`; for now a target lists `group`, `version`, `kind`, `name` and
`namespace`, and each field it sets must match. The name may be the
resource's first name or its current one, and a resource with no namespace
counts as being in `default`. A target that matches nothing is not an error.

`patchesJson6902` is the older field for the same thing. kustomize runs it
later than `patches` — after this kustomization's namespace, prefix, labels
and annotations.

## Go APIs

- `strconv.Atoi` for list indexes.
- `append(s[:i:i], append([]any{v}, s[i:]...)...)` inserts without aliasing
  the tail.

## Hints

<details><summary>Nudge</summary>

Walk the pointer to the *container* of its last token, and let each operation
decide what to do with that container and key.
</details>

<details><summary>Approach</summary>

Appending to a slice can return a new slice, so the walk must write each
container back into its parent on the way out. Copy deeply on `copy`, or
the two places will share one map.
</details>

## Further reading

- [RFC 6902: JSON Patch](https://www.rfc-editor.org/rfc/rfc6902)
- [RFC 6901: JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901)
- [Patches in kustomize](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/patches/)
