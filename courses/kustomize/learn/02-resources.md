---
title: Gather the resources
concepts: [resource identity, Lists, nested kustomizations, output order, load restrictions]
---

## Core concept

An entry of `resources` is one of two things:

- a **file**, holding any number of YAML documents. Empty and comment-only
  documents are skipped. A `List` — or any kind ending in `List` with an
  `items` field — is unwrapped into its items.
- a **directory**, which must hold its own kustomization. It is built on its
  own, completely, and its output is added in. This is the seed of bases and
  overlays.

A file must sit inside the kustomization's own directory; `../other/cm.yaml`
is refused. Only a directory may reach outside. A directory that includes
itself, directly or through others, is a cycle and is refused too.

Every resource has an **identity**: group, version, kind, namespace and name.
Two resources with the same identity are an error, wherever they came from.
The same name in two namespaces is two resources.

kustomize does not print resources in the order it read them. It sorts:

1. kinds it ranks first, in its order — `Namespace`, `ResourceQuota`,
   `StorageClass`, `CustomResourceDefinition`, `ServiceAccount`, …,
   `ConfigMap`, `Secret`, `Service`, …, `Deployment`, `StatefulSet`,
   `CronJob`, `PodDisruptionBudget`;
2. every other kind;
3. `MutatingWebhookConfiguration`, then `ValidatingWebhookConfiguration`.

Within a rank it orders by group, version and kind, then by namespace, then
name. Missing parts sort last, as if spelled with a `~`. The sort is on
`namespace|name`, so namespace `aa` comes before `a`.

## Go APIs

- `filepath.EvalSymlinks` gives each directory one spelling, so a cycle is
  recognised however the path was written.
- `filepath.Rel` tells you whether a file is below the root.
- `sort.SliceStable` with a rank, then a string key.

## Hints

<details><summary>Nudge</summary>

Make `build(dir)` return the resources, and call it recursively for a
directory entry. A set of the directories being built catches the cycle.
</details>

<details><summary>Approach</summary>

Build a sort key that encodes kustomize's placeholders — `~G` for no group,
`~X` for no namespace — so one string comparison does the work after the
rank.
</details>

## Further reading

- [resources](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/resource/)
- [kustomize's legacy resource order](https://github.com/kubernetes-sigs/kustomize/blob/master/api/resid/gvk.go)
