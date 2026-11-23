---
title: Patch by example
concepts: [strategic merge patch, patch merge keys, $patch directives, patch matching]
---

## Core concept

A strategic merge patch is a fragment of the resource it changes. You write
the fields you want, and kustomize lays them over the original:

```yaml
patches:
- path: more-replicas.yaml        # a file…
- patch: |-                       # …or the patch inline
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: web
    spec:
      replicas: 3
```

The patch's `apiVersion`, `kind`, `metadata.name` and `metadata.namespace`
pick its target, and there must be exactly one. The name may be the one the
resource has now or any it had earlier in the build — so an overlay can patch
`web` even after its base renamed it `base-web`. A patch with no namespace only
matches a resource with none. The patch never renames its target.

The merge itself, field by field:

| patch holds | result |
|---|---|
| a map | merged key by key, recursively |
| `null` | the field is deleted |
| a scalar | replaces the field |
| a list with a **merge key** | merged item by item |
| any other list | replaces the list whole |

Merge keys come from the Kubernetes schema: `containers`, `volumes`, `env`
and `imagePullSecrets` items are keyed by `name`, `volumeMounts` by
`mountPath`, container `ports` by `containerPort` and a Service's `ports` by
`port`. A merged list holds the patch's items first — each merged into the
original item with the same key — then the original items the patch did not
mention. `args`, `command` and `tolerations` have no key, so a patch replaces
them. A custom resource has no schema kustomize knows, so *all* of its lists
are replaced, even one called `containers`.

Directives sit inside the patch as `$patch`:

- `$patch: delete` on a list item removes the original item with that key;
  at the top of a patch it removes the whole resource.
- `$patch: replace` in a map replaces the map instead of merging it; as an
  item of a keyed list it makes the patch's items the whole list.

Patches run before this kustomization's own namespace, prefix and labels, so
they see resources as they arrive. Generators run before patches, so a
generated ConfigMap can be patched — and its hash is of the patched data.
`patchesStrategicMerge`, the older field, lists files or inline patches and
runs before `patches`.

## Go APIs

- `reflect.DeepEqual` compares merge-key values whatever type the YAML decoder
  gave them.

## Hints

<details><summary>Nudge</summary>

Write `mergeMap` and `mergeList` as two mutually recursive functions, and
carry the slash path down so a list knows which field it is.
</details>

<details><summary>Approach</summary>

After the patch, put the target's name and namespace back: the patch may have
matched an earlier name, and merging would overwrite the current one.
</details>

## Further reading

- [Patches in kustomize](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/patches/)
- [Strategic merge patch](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-api-machinery/strategic-merge-patch.md)
