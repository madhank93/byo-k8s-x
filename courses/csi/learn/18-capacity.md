---
title: Report and enforce capacity
concepts: [GetCapacity, storage capacity tracking, ResourceExhausted]
---

## Core concept

A node's disk is finite, and a scheduler that does not know how full it is
will keep sending pods with new claims to a node that cannot hold them.
`GetCapacity` answers how many bytes are still available. Kubernetes's
external-provisioner publishes the answer as CSIStorageCapacity objects, and
the scheduler skips nodes whose capacity is too small for a pod's claims.

Two halves, and both must agree:

- **Report**: available is the total, `--capacity`, less the capacity of
  every volume. It is derived from the records on disk, so it is right after
  a restart too.
- **Enforce**: a `CreateVolume` that does not fit is `ResourceExhausted`, and
  takes nothing. Retrying an existing volume does not count again — it is
  already in the sum.

`GetCapacity` may be asked for a particular topology or set of capabilities.
For a driver with one node and one kind of storage the answer is the same.

This stage: `--capacity BYTES`, `GET_CAPACITY`, and refusing what does not
fit.

## Go APIs

- `flag.Int64Var`.
- `csi.GetCapacityResponse{AvailableCapacity}`.

## Hints

<details><summary>Nudge</summary>

Count capacity from the volume records rather than keeping a running total:
there is then nothing to get out of step.
</details>

## Further reading

- [GetCapacity](https://github.com/container-storage-interface/spec/blob/master/spec.md#getcapacity)
- [Storage capacity tracking](https://kubernetes.io/docs/concepts/storage/storage-capacity/)
