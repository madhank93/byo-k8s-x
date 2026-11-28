---
title: Admit what fits, limit what runs
concepts: [requests, limits, admission, cgroups, OOMKilled]
---

## Core concept

A container's `resources` hold two different promises:

- **requests** are what the pod is guaranteed. The scheduler places pods by
  their requests, and the kubelet checks them again before it runs anything,
  because a pod can be bound to the node with `spec.nodeName` and never meet
  the scheduler at all. If this pod's requests, added to those of the pods
  already running, are more than the node's allocatable, the kubelet
  **rejects** it. The pod goes straight to `Failed`, with reason
  `OutOfcpu` or `OutOfmemory` and a message that says what did not fit.
  Nothing is started.
- **limits** are what the container may not exceed, and the kernel enforces
  them through the container's **cgroup**:
  - `limits.memory` becomes `memory.max`. Swap is set to zero as well,
    because a pod that could swap past its limit would have no limit. A
    container that goes over is killed by the kernel's OOM killer. It exits
    137 (SIGKILL), and the kubelet reports reason **OOMKilled**, which it
    learns from the `oom_kill` count in the cgroup's `memory.events`.
  - `limits.cpu` becomes a CFS quota, `cpu.max`: 500m is 50ms of CPU in
    every 100ms period.
  - `requests.cpu` becomes the cgroup's weight, so a busy node shares CPU
    in proportion to what each pod asked for.

Read `memory.events` before you delete the task. The cgroup is removed along
with it.

## Go APIs

- `oci.WithMemoryLimit(bytes)`, `oci.WithMemorySwap(bytes)`,
  `oci.WithCPUCFS(quota, period)`, `oci.WithCPUShares(shares)`. A
  `func(ctx, client, *containers.Container, *oci.Spec) error` is an
  `oci.SpecOpts` of your own.
- `container.Spec(ctx)`: `Linux.CgroupsPath` is the cgroup under
  `/sys/fs/cgroup`.
- `resource.Quantity`: `Add`, `Cmp`, `Value`, `MilliValue`.

## Hints

<details><summary>Nudge</summary>

Admission happens once, on the first sync of a pod, before it has a
`startTime`. The pods already admitted are the ones that have a start time
and have not finished.
</details>

<details><summary>Approach</summary>

Sum the requests of the admitted pods, add this pod's requests, and compare
each resource against capacity. Write a Failed status with the reason, and
never call `startContainer`. When a task exits, record the OOM kill in a
label next to the exit code, and report OOMKilled ahead of Error.
</details>

## Further reading

- [Resource management for pods and containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [How pods with resource limits are run](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#how-pods-with-resource-limits-are-run)
- [cgroup v2: memory and cpu interface files](https://docs.kernel.org/admin-guide/cgroup-v2.html)
