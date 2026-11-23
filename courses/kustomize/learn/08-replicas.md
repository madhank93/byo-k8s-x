---
title: Set the replica counts
concepts: [replicas, scalable kinds, name history]
---

## Core concept

```yaml
replicas:
- name: web
  count: 3
```

sets `spec.replicas` to 3 on every **scalable** resource called `web`:
Deployments, ReplicaSets, StatefulSets and ReplicationControllers, in any API
group. An existing count is overwritten. A Service or a Job called `web` is
not scalable and is untouched.

The name is matched against the resource's **history**: a Deployment that a
prefix has renamed `prod-web` still answers to `web`, and so does one a base
renamed before the overlay saw it. This is the first stage that needs the
history you started keeping in stage 4.

A name that no scalable resource answers to is an error, not a silent no-op,
and `count` must be a number.

## Hints

<details><summary>Nudge</summary>

Give your resource a `hasName(name)` method that searches every name it has
had.
</details>

## Further reading

- [replicas](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/replicas/)
