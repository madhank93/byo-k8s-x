---
title: Clone a volume
concepts: [CLONE_VOLUME, volume content source, copy semantics]
---

## Core concept

A clone is a restore with no snapshot in between: `CreateVolume` whose
`volume_content_source` names another **volume**. In Kubernetes, a claim
whose `dataSource` is another claim.

The rules are the restore's rules:

- The clone holds what the source held when it was cloned, and from then on
  the two are separate — writes to either do not reach the other.
- The source must exist (`NotFound`), and the clone must be at least as large
  (`OutOfRange`).
- The answer's `content_source` names the source volume.

Advertise it with the controller capability `CLONE_VOLUME`. Without it, the
external-provisioner refuses claims with a claim as `dataSource` before the
driver hears of them.

This stage: clone a volume.

## Go APIs

- `req.GetVolumeContentSource().GetVolume().GetVolumeId()`.

## Hints

<details><summary>Nudge</summary>

You already copy a volume's storage into a snapshot, and a snapshot's into a
new volume. A clone is that copy straight from one volume to the other.
</details>

## Further reading

- [Kubernetes CSI volume cloning](https://kubernetes-csi.github.io/docs/volume-cloning.html)
- [Volume cloning](https://kubernetes.io/docs/concepts/storage/volume-pvc-datasource/)
