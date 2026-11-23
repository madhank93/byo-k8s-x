---
title: Copy a field into others
concepts: [replacements, field paths, delimiters, YAML types]
---

## Core concept

Name references (stage 12) cover the fields kustomize *knows* refer to
another object. Everything else — a hostname in an environment variable, a
port in an annotation, a tag that must match some other tag — is a
**replacement**: copy one field of one resource into fields of others.

```yaml
replacements:
- source:
    kind: ConfigMap
    name: endpoints
    fieldPath: data.host            # default: metadata.name
  targets:
  - select:
      kind: Deployment
    reject:
    - name: api
    fieldPaths:
    - spec.template.spec.containers.[name=web].env.[name=HOST].value
```

The **source** selects exactly one resource — none, or two, is an error —
using the same filters as a patch target, so it can name the resource as
written even after a prefix. Its `fieldPath` must exist.

Each **target** selects resources with `select`, minus any that match an
entry of `reject`, and writes the value at each of its `fieldPaths`.

Field paths are dotted. A segment is:

| segment | means |
|---|---|
| `replicas` | a map key |
| `[example.com/owner]` | a map key with dots in it |
| `0` | a list index |
| `[name=web]` | the list items whose `name` is `web` |
| `*` | every item of a list |

A target path that is not there is an error unless the target says
`options: {create: true}`, which makes the missing map keys.

Two options cut values into parts. On the source, `delimiter` and `index`
take one part (`redis.shop.svc`, `.`, `1` → `shop`), and an index past the end
is an error. On a target they replace one part of the existing value
(`shop/web:1.0`, `:`, `1` → `shop/web:shop`), and an index past the end adds a
part instead.

The target field **keeps its type**: kustomize keeps the field's YAML tag and
swaps only its text. Copying the string `"3"` into `spec.replicas` gives the
number 3; copying a number into a string field gives a string. A field that
did not exist takes the source's type.

Replacements are the last thing a kustomization does — after patches,
labels, images and replicas — so they see, and win over, everything else.

## Go APIs

- `strconv.FormatFloat(f, 'f', -1, 64)` writes a decoded YAML number back as
  `3`, not `3e+00`.

## Hints

<details><summary>Nudge</summary>

Split the path on dots that are not inside brackets, then walk it with a
callback that receives the container and the last segment, as in stage 14.
</details>

<details><summary>Approach</summary>

Read the source value once, then for every selected target and path either
set the value, retyped to the old value's type, or rebuild the delimited
string around it.
</details>

## Further reading

- [Replacements](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/replacements/)
