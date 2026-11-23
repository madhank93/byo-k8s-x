---
title: Put everything in one namespace
concepts: [namespace transformer, cluster-scoped kinds, layering]
---

## Core concept

`namespace: prod` moves every **namespaced** resource into `prod`, replacing
whatever namespace it already had. Cluster-scoped kinds — `Namespace`,
`ClusterRole`, `ClusterRoleBinding`, `PersistentVolume`, `StorageClass`,
`CustomResourceDefinition`, the webhook configurations and the rest — have no
namespace and get none.

A kind is judged *with its API group*: `rbac.authorization.k8s.io`'s
`ClusterRole` is cluster-scoped, but `example.com`'s `ClusterRole` is just a
custom resource that happens to share the name, and it is namespaced.

Two things follow the namespace besides `metadata.namespace`:

- a `Namespace` object in the resources is **renamed** to the namespace, so
  the build creates the namespace it puts everything in;
- in a `RoleBinding` or `ClusterRoleBinding`, a subject named `default`
  — the namespace's default ServiceAccount — gets the new namespace.

Each kustomization applies its own namespace to what it builds, so in a
nested build the outermost one has the last word, and an inner namespace
survives an outer kustomization that sets none.

Moving things can make two resources one identity — two `settings`
ConfigMaps from different namespaces, or two `Namespace` objects both renamed
`prod` — and that is an error.

## Hints

<details><summary>Nudge</summary>

Keep a table of cluster-scoped kinds keyed by `group/kind`, with `""` as the
core group.
</details>

<details><summary>Approach</summary>

Apply the namespace after all of this kustomization's resources are gathered,
then run the identity check again.
</details>

## Further reading

- [namespace](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/namespace/)
- [Namespaced and cluster-scoped resources](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/#not-all-objects-are-in-a-namespace)
