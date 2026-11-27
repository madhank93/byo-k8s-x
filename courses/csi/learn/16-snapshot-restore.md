---
title: Restore one
concepts: [volume_content_source, restore, snapshot independence]
---

## Core concept

Restoring is not a call of its own. It is `CreateVolume` with a
`volume_content_source` naming a snapshot: a new volume, created holding the
snapshot's contents. In Kubernetes that is a claim with a `dataSource` of
kind VolumeSnapshot.

What it must get right:

- **The contents of the instant the snapshot was taken.** Files changed or
  written after it are not in the restored volume.
- **Independence from the source.** The source volume may be long deleted;
  the snapshot is its own copy and still restores.
- **The answer says where it came from**: the new volume's `content_source`
  echoes the snapshot, which is how the CO knows the request was honoured
  rather than ignored.
- A snapshot that does not exist is `NotFound`, and a volume too small to
  hold it is `OutOfRange`.

The restored volume takes the snapshot's kind: an ext4 snapshot restores as
an ext4 volume, whatever the request's `fs_type` said.

This stage: `CreateVolume` from a snapshot.

## Go APIs

- `req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()`.
- `csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{...}}` in
  the answer.

## Hints

<details><summary>Nudge</summary>

Remember the source with the volume's record: a retried `CreateVolume` has
to answer the same `content_source` as the first.
</details>

## Further reading

- [CreateVolume: volume_content_source](https://github.com/container-storage-interface/spec/blob/master/spec.md#createvolume)
- [Restore a volume from a snapshot](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#volume-snapshot-and-restore-volume-from-snapshot-support)
