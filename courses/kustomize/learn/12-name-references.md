---
title: Keep the references wired up
concepts: [name references, nameReference config, generated names, namespaces]
---

## Core concept

Renaming an object is only safe if everything that points at it is renamed
too. A Deployment that mounts ConfigMap `settings` breaks the moment the
ConfigMap becomes `settings-t82mkhg8fd` — unless the Deployment's
`volumes[].configMap.name` follows it. kustomize keeps a table of every field
that names another object (its *nameReference* config) and, once all names
are final, rewrites each of those fields:

```
  referrer field                                   refers to
  <pod spec>/volumes/configMap/name                ConfigMap
  <pod spec>/containers/envFrom/secretRef/name     Secret
  <pod spec>/serviceAccountName                    ServiceAccount
  Ingress spec/rules/http/paths/backend/service    Service
  RoleBinding roleRef/name                         whatever roleRef.kind says
  HorizontalPodAutoscaler spec/scaleTargetRef      whatever its kind says
```

`<pod spec>` is wherever the workload keeps one: `spec` in a Pod,
`spec/template/spec` in a Deployment, StatefulSet, Job…, and
`spec/jobTemplate/spec/template/spec` in a CronJob.

Three rules decide what a reference resolves to:

- **History, not just the current name.** A resource remembers every name it
  has had — `settings`, `base-settings`, `prod-base-settings-…` — and a
  reference written against any of them finds it. That is how an overlay's own
  Pod can say `settings` and still land on the base's prefixed, hashed
  ConfigMap.
- **Kind matters.** A RoleBinding's `roleRef.name: reader` follows the Role
  or the ClusterRole called `reader` depending on `roleRef.kind`.
- **Namespace matters.** A namespaced object only answers references from its
  own namespace; a cluster-scoped one answers everyone.

A name nobody in the build ever had is not a reference kustomize owns: it is
left exactly as written.

For this stage, run the rewrite once at the end of `build`, after the hash
suffixes are on.

## Hints

<details><summary>Nudge</summary>

Store the reference table as data — referrer kind, slash path, target kind —
and reuse the path walker you wrote for labels.
</details>

<details><summary>Approach</summary>

For each resource, for each table row matching its kind, walk to the field;
then search every resource for one of the target kind, in the same namespace
(or cluster-scoped), whose name history contains the field's value, and write
its current name back.
</details>

## Further reading

- [kustomize's built-in nameReference config](https://github.com/kubernetes-sigs/kustomize/blob/master/api/internal/konfig/builtinpluginconsts/namereference.go)
- [Generators and name references](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/configmapgenerator/)
