---
title: Build on a base
concepts: [bases, overlays, generator behavior, layering]
---

## Core concept

An **overlay** is nothing new: it is a kustomization whose `resources` lists
another kustomization directory, the **base**. What makes the pattern work is
the order of operations — the base is built completely, then everything the
overlay says is applied to the base's finished output:

```
  base/                       prod/
    deployment.yaml             kustomization.yaml
    service.yaml                  resources: [../base]
    kustomization.yaml            namespace: prod
                                  namePrefix: prod-
                                  replicas: [{name: web, count: 3}]
```

Every earlier stage composes this way. The prefixes stack (`top-mid-web`),
references follow every rename, and the overlay's patches, replicas and
generators can still name a resource the way the base wrote it, because a
resource answers to every name it has had.

Two overlays of one base can sit side by side in a third kustomization —
`resources: [../prod, ../dev]` — and each builds its own copy of the base.
They had better end up with different names or namespaces, or the copies
collide.

The one new thing is **generator behavior**. An overlay usually wants to
*change* the base's ConfigMap rather than make another:

| `behavior` | needs an earlier one? | data | labels, annotations |
|---|---|---|---|
| `create` (default) | must **not** exist | new | new |
| `merge` | must exist | old, overlaid by new | merged |
| `replace` | must exist | new only | merged |

A Secret's `type` becomes the overlay's. "Exists" means a ConfigMap (or
Secret) that has *ever* been called that name in that namespace — so `create`
still collides with a base's `settings` after a middle layer renamed it
`mid-settings`. The merged object keeps its place, its current name and its
hash flag; the hash is computed at the very end, so it reflects the merged
data and every reference follows it.

`bases` is the older field for listing bases. It works like `resources`.

## Hints

<details><summary>Nudge</summary>

Before appending a generated object, look for one of the same kind whose name
history includes the generator's name.
</details>

<details><summary>Approach</summary>

Merge into the existing object in place: labels and annotations as maps,
`data` and `binaryData` merged or replaced, and `type` overwritten.
</details>

## Further reading

- [Bases and overlays](https://kubectl.docs.kubernetes.io/references/kustomize/glossary/#overlay)
- [configMapGenerator behavior](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/configmapgenerator/)
