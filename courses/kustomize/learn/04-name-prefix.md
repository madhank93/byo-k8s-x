---
title: Prefix and suffix the names
concepts: [namePrefix, nameSuffix, name history]
---

## Core concept

`namePrefix: prod-` and `nameSuffix: -v2` wrap the name of every resource,
cluster-scoped ones included: `web` becomes `prod-web-v2`. That is how two
copies of one application can live side by side.

Three kinds keep their names, because the API server derives them from what
they define: `Namespace`, `CustomResourceDefinition` (named
`<plural>.<group>`) and `APIService` (named `<version>.<group>`). Renaming
them would break them.

Layers wrap from the inside out: an inner kustomization's `base-` is applied
first, and the outer `prod-` goes outside it — `prod-base-web`. Suffixes
stack the same way, outer last.

From here on a resource has a **history**: every name and namespace it has
had, the current one last. Nothing reads it yet, but replicas (stage 8),
references (stage 12) and patches (stage 13) all find a resource by a name it
used to have, so start keeping it now.

## Hints

<details><summary>Nudge</summary>

Wrap each object in a small struct that holds the map and its list of past
names, and rename through one method that appends to it.
</details>

## Further reading

- [namePrefix](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/nameprefix/)
- [nameSuffix](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/namesuffix/)
