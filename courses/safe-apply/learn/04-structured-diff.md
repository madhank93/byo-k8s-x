---
title: Say what changes, field by field
concepts: [diff, merge patch, exit codes]
---

## Core concept

A patch says what to send. A reviewer wants to know what will *be different*,
and that is a comparison between two whole objects: live as it is now, and
live as it will be once the patch lands.

```
  live ──► apply the three-way patch locally ──► after
    └──────────────── compare, leaf by leaf ──────┘
```

Flatten both objects into `path → value` and the diff falls out: a path only
in *after* was added, a path only in *live* was removed, a path in both with a
different value changed. Lists are compared whole — telling which element
moved needs the list's merge key, and a whole-list line is never wrong.

Some fields change on every write and mean nothing: `metadata.resourceVersion`
and `metadata.managedFields`. Leave them out of both sides.

For this stage, add:

```
app diff -f FILE
```

For each object that would change, print its `Kind/name` on a line of its
own, then one indented line per changed field, sorted by path, with values as
JSON:

```
ConfigMap/web
  ~ .data.a: "1" -> "1x"
  - .data.b: "2"
  + .data.d: "4"
```

An object that would not change prints nothing; one that does not exist yet
has every field as `+`. Exit **0** when nothing would change, **1** when
something would, **2** when the diff cannot be computed.

Watch your test data: YAML 1.1 reads an unquoted key `n`, `y`, `no` or `on` as
a boolean, and so does the decoder kubectl uses.

## Go APIs

- `jsonpatch.MergePatch(current, patch)` from `gopkg.in/evanphx/json-patch.v4`
  applies a merge patch to a JSON document.
- `unstructured.RemoveNestedField` to drop the noisy fields.
- `json.Marshal` on a leaf gives the value's display form for free.

## Hints

<details><summary>Nudge</summary>

Diffing two nested maps is hard. Diffing two flat maps is a loop. Which one
can you turn the other into?
</details>

<details><summary>Approach</summary>

Walk each object recursively, building `.a.b.c` paths; a non-empty map
recurses, anything else is a leaf. Then two passes: over *after* for adds and
changes, over *live* for removals.
</details>

<details><summary>Implementation</summary>

```go
var walk func(path string, v any)
walk = func(path string, v any) {
    if m, ok := v.(map[string]any); ok && len(m) > 0 {
        for k, child := range m { walk(path+"."+k, child) }
        return
    }
    js, _ := json.Marshal(v)
    out[path] = string(js)
}
```
</details>

## Further reading

- [kubectl diff](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_diff/)
- [The Norway problem](https://hitchdev.com/strictyaml/why/implicit-typing-removed/)
