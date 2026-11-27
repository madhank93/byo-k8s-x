---
title: Take a snapshot
concepts: [CreateSnapshot, ListSnapshots, DeleteSnapshot, ready_to_use]
---

## Core concept

A snapshot is a volume's contents at one instant, kept apart from the
volume. In Kubernetes a VolumeSnapshot object becomes `CreateSnapshot`
through the external-snapshotter, much as a claim becomes `CreateVolume`.

The calls mirror the volume ones:

- **`CreateSnapshot(name, source_volume_id)`** — `name` is again the
  idempotency key. The same name of the same source answers the same
  snapshot; the same name of a different source is `AlreadyExists`. A source
  that does not exist is `NotFound`.
- **`ListSnapshots`** — all of them, or filtered by `snapshot_id` or
  `source_volume_id`. A filter that matches nothing is an empty list, not an
  error.
- **`DeleteSnapshot`** — idempotent, like `DeleteVolume`.

A snapshot answers `creation_time` and `ready_to_use`. Cloud snapshots are
often cut instantly but uploaded for minutes; `ready_to_use: false` tells
the CO to wait before restoring. This driver's snapshot is a full copy taken
in the call, ready when it returns.

This stage: advertise `CREATE_DELETE_SNAPSHOT` and `LIST_SNAPSHOTS`, and keep
snapshots — records and copies — under `--root`.

## Go APIs

- `exec.Command("cp", "-a", "--sparse=always", src, dst)` copies a directory
  tree or an image without filling its holes.
- `timestamppb.New(t)` from `google.golang.org/protobuf/types/known/timestamppb`.

## Hints

<details><summary>Nudge</summary>

A copy that a crash cut short must not be found later as a finished
snapshot. Write the record only after the copy is complete.
</details>

## Further reading

- [CreateSnapshot](https://github.com/container-storage-interface/spec/blob/master/spec.md#createsnapshot)
- [Kubernetes CSI snapshots](https://kubernetes-csi.github.io/docs/snapshot-restore-feature.html)
