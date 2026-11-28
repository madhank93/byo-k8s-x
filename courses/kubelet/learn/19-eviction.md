---
title: Evict what outgrows its limit
concepts: [eviction, ephemeral storage, emptyDir sizeLimit, DisruptionTarget, Evicted]
---

## Core concept

The kubelet is the node's last line of defence. When something on the node
runs short (memory, disk, inodes, process IDs), the scheduler cannot help,
because it only places pods. The kubelet **evicts**: it picks pods, kills
them, and leaves them **Failed** with reason **Evicted**, so their
controllers make replacements somewhere else.

Node-wide pressure is hard to stage on a shared kind node. The same machinery
also enforces a per-pod promise you can stage: an `emptyDir` with a
**sizeLimit**. Its contents count as the pod's local, ephemeral storage. When
they grow past the limit, the kubelet evicts the pod:

- its containers are killed at once, with no grace period, since this is a
  hard eviction;
- the pod's phase becomes `Failed`, its `reason` is `Evicted`, and its
  `message` names the volume and the limit;
- a condition **DisruptionTarget**=True, reason `TerminationByKubelet`, says
  the pod was ended by the node and not by its own failure. Jobs read it
  through `podFailurePolicy`.

An evicted pod is **not deleted**. It stays as a record of what happened, and
it is terminal, so its restart policy no longer applies: nothing runs in it
again.

## Go APIs

- `filepath.WalkDir`, summing `d.Info().Size()`, to measure a volume.
- `v.EmptyDir.SizeLimit.Value()`.
- `corev1.DisruptionTarget`.

## Hints

<details><summary>Nudge</summary>

Measure on every sync, before you start anything. A pod already over its
limit should be evicted, not given another container.
</details>

<details><summary>Approach</summary>

If an emptyDir with a size limit holds more than the limit, stop the pod's
containers with a zero grace period, build the status as usual, then
overwrite the phase, reason and message and append the DisruptionTarget
condition. Your terminal-phase check keeps the pod from running again.
</details>

## Further reading

- [Node-pressure eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/)
- [Local ephemeral storage](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#local-ephemeral-storage)
- [Pod disruption conditions](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/#pod-disruption-conditions)
