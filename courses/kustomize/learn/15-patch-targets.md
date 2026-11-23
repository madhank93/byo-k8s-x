---
title: Aim a patch at many resources
concepts: [patch targets, regular expressions, label selectors, output order]
---

## Core concept

A `target` is a filter, and a patch applies to *every* resource it lets
through — one patch to add a sidecar to every Deployment, or an annotation to
everything labelled `tier=front`:

```yaml
patches:
- target:
    group: apps
    kind: Deployment|StatefulSet
    labelSelector: tier in (front, back),canary!=true
  patch: |-
    - op: add
      path: /metadata/annotations/owner
      value: shop
```

`group`, `version`, `kind`, `name` and `namespace` are **regular expressions
matched against the whole field**: `web` does not select `web-canary`, and
`Deploy` does not select `Deployment`, but `web.*` and `Deployment|StatefulSet`
mean what they say. Wrap the expression in `^(?:…)$` and it behaves.

`labelSelector` and `annotationSelector` use Kubernetes selector syntax,
requirements joined by commas, all of which must hold:

| requirement | holds when |
|---|---|
| `k=v`, `k==v` | `k` is there with value `v` |
| `k!=v` | `k` is missing, or has another value |
| `k in (a, b)` | `k` is there with one of the values |
| `k notin (a, b)` | `k` is missing, or has none of them |
| `k` / `!k` | `k` is there / missing |

A strategic merge patch can have a target too. Then its own `kind` and
`name` are not consulted at all — the target chose — and every selected
resource keeps its apiVersion, kind, name and namespace. Such an entry may
hold only one patch document, and `$patch: delete` in it deletes everything
selected.

A strategic merge patch merges *into* what is there. A JSON patch needs its
path to exist — with one exception you will meet here: kustomize keeps
bookkeeping annotations on every resource while it builds, so
`/metadata/annotations` is always there to `add` under.

More same-kind resources means kustomize's output order matters more. Within
one group, version and kind it sorts by **namespace, then name**, and a
resource with no namespace sorts after all the namespaced ones.

## Go APIs

- `regexp.Compile("^(?:" + expr + ")$")` anchors without changing what `|`
  means.
- Splitting a selector on commas must skip the commas inside `( … )`.

## Hints

<details><summary>Nudge</summary>

One `selects(target, resource)` function serves both kinds of patch and the
legacy `patchesJson6902` field.
</details>

<details><summary>Approach</summary>

Parse each selector requirement with one regular expression — an optional
`!`, a key, then optionally an operator and value or `in`/`notin` and a
parenthesised list.
</details>

## Further reading

- [Patches with targets](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/patches/#patch-multiple-objects)
- [Label selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors)
