---
title: Attach before mount
concepts: [ControllerPublishVolume, attach, VolumeAttachment, FailedPrecondition]
---

## Core concept

Between creating a volume and staging it on a node there is often a step only
the storage system can do: attach the disk to that machine. That is
`ControllerPublishVolume(volume_id, node_id)`, and its undo
`ControllerUnpublishVolume`. In Kubernetes they run from the
external-attacher, which watches VolumeAttachment objects the
attach/detach controller creates when a pod is scheduled.

The calls are in the controller service because attaching is a control-plane
act: it is the cloud API that hangs a disk on a VM, not anything the VM does.
What a driver owes:

- Publishing to the node it is already published to is success.
- A node or volume the driver does not know is `NotFound`.
- While a volume is published somewhere, `DeleteVolume` is
  `FailedPrecondition`. A disk still attached to a running machine must not
  disappear under it.
- And as ever, publishing is remembered across restarts.

This driver's volumes are on its node's disk already, so "attached" is only a
fact it records with the volume. That is enough for the contract above.

This stage: advertise `PUBLISH_UNPUBLISH_VOLUME`, record which nodes hold a
volume, and refuse to delete one that is still published.

## Go APIs

- `slices.Contains`, `slices.DeleteFunc` on the record's node list.

## Hints

<details><summary>Nudge</summary>

`ControllerUnpublishVolume` with no `node_id` means "from every node".
</details>

## Further reading

- [ControllerPublishVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#controllerpublishvolume)
- [external-attacher](https://github.com/kubernetes-csi/external-attacher)
- [CSIDriver: attachRequired](https://kubernetes-csi.github.io/docs/csi-driver-object.html)
