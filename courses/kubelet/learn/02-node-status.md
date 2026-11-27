---
title: Report what the machine is
concepts: [NodeStatus, capacity, allocatable, conditions, status subresource]
---

## Core concept

A registered Node with no status is a name and nothing else. The scheduler
needs to know what fits on it, and the node lifecycle controller needs to know
it is alive. Both come from **status**, which only the kubelet writes:

- **capacity** — what the machine has: `cpu` (cores), `memory` (bytes; the
  `MemTotal` in `/proc/meminfo`), and `pods`, the most it will run (110 by
  default).
- **allocatable** — what is left for pods after the system's own reservations.
  This course reserves nothing yet, so it equals capacity.
- **conditions** — `Ready=True` (reason `KubeletReady`) is the one the
  scheduler and the lifecycle controller read. Its `lastHeartbeatTime` is
  when the kubelet last said so.
- **addresses** — `InternalIP`, what the node's hostname resolves to, and
  `Hostname`, the node's name.
- **nodeInfo** — `kernelVersion`, `osImage`, `operatingSystem`,
  `architecture`, read from the machine.

Status is written through the Node's `status` subresource, which ignores spec
and metadata. Post it on an interval: the lifecycle controller marks a node
`Unknown` when the heartbeat goes quiet for its grace period, 40 seconds.

This stage: post that status at start and every 10 seconds, with a fresh
heartbeat each time.

## Go APIs

- `runtime.NumCPU()`; `/proc/meminfo`, `/proc/sys/kernel/osrelease`,
  `/etc/os-release` (`PRETTY_NAME`).
- `os.Hostname()` and `net.LookupIP`.
- `resource.NewQuantity(n, resource.DecimalSI or BinarySI)`.
- `cs.CoreV1().Nodes().Patch(ctx, name, types.StrategicMergePatchType, data,
  opts, "status")` or `UpdateStatus`.

## Hints

<details><summary>Nudge</summary>

`MemTotal` is in KiB. Capacity is in bytes, or a quantity with a `Ki` suffix —
either compares equal.
</details>

<details><summary>Approach</summary>

Build a `corev1.NodeStatus`, marshal `{"status": …}` and send it as a strategic
merge patch to the status subresource: conditions and addresses merge by
`type`, so you do not clobber what another component wrote. Run it in a
goroutine on a ticker, logging failures rather than exiting on them.
</details>

## Further reading

- [Node status](https://kubernetes.io/docs/reference/node/node-status/)
- [Reserve compute resources for system daemons](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/)
- [kubelet: node status setters](https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/nodestatus/setters.go)
