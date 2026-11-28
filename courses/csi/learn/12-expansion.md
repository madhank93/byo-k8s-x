---
title: Grow a volume in place
concepts: [ControllerExpandVolume, NodeExpandVolume, online expansion, resize2fs]
---

## Core concept

Growing a volume is two jobs in two places, and the spec splits it the same
way:

- **`ControllerExpandVolume`** grows the storage: the cloud disk, or here the
  image file. It answers the new size and `node_expansion_required` — true
  when there is a filesystem on the volume that has not grown with it.
- **`NodeExpandVolume`** runs on the node where the volume is mounted and
  grows the filesystem into the new space.

For an image behind a loop device there is one step between them: the loop
device sized itself when it was attached and does not notice the file grew
until told — `losetup -c`. After that, `resize2fs` grows a mounted ext4
filesystem in place. Growing while mounted is **online** expansion, the
`VolumeExpansion` plugin capability that lets the kubelet expand a volume
under a running pod.

Volumes only grow. A size below the current one is `OutOfRange`; the size it
already has is success, since the CO retries.

This stage: advertise `EXPAND_VOLUME` in both services and `ONLINE`
expansion, and grow an ext4 volume's filesystem while it stays published.

## Go APIs

- `os.Truncate(img, size)` grows a sparse file.
- `losetup -j IMG -n -O NAME` finds the device an image is attached to.
- `losetup -c DEV`, `resize2fs DEV`.
- `csi.PluginCapability_VolumeExpansion_ONLINE`.

## Hints

<details><summary>Nudge</summary>

`resize2fs` with no size grows the filesystem to the device's size — which is
still the old size until the loop device is refreshed.
</details>

<details><summary>Approach</summary>

Controller: validate, refuse shrinking, truncate the image and save the new
capacity, and say `node_expansion_required` for image volumes. Node: find the
loop device, refresh it, `resize2fs` it. A directory volume has nothing to
grow on the node.
</details>

## Further reading

- [ControllerExpandVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#controllerexpandvolume)
- [NodeExpandVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#nodeexpandvolume)
- [Kubernetes CSI volume expansion](https://kubernetes-csi.github.io/docs/volume-expansion.html)
