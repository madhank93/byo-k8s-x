---
title: Take it back down
concepts: [NodeUnpublishVolume, NodeUnstageVolume, idempotent teardown]
---

## Core concept

Teardown runs the other way: when a pod goes, the kubelet calls
`NodeUnpublishVolume` on its target; when no pod on the node uses the volume
any more, `NodeUnstageVolume` on the staging path.

Each one is an unmount, and each one must be safe to repeat. A target that is
already unmounted, or already removed, is success — the kubelet retries
teardown until it hears yes, and a driver that errors on "already done"
leaves a pod stuck terminating forever.

What goes and what stays:

- Unpublish removes the target directory, which the driver created.
- Unstage leaves the staging directory, which the CO created.
- Neither touches the volume's data. Unmounting is not deleting: publish the
  volume again and its files are there. Only `DeleteVolume` destroys data.

This stage: `NodeUnpublishVolume` and `NodeUnstageVolume`, idempotent, with
`InvalidArgument` for a missing id or path.

## Go APIs

- `syscall.Unmount(path, 0)`.
- `os.Remove` for the target, ignoring `os.IsNotExist`.

## Hints

<details><summary>Nudge</summary>

Check for a mount before unmounting; `umount` of a plain directory is an
error, and of a missing one too.
</details>

## Further reading

- [NodeUnpublishVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#nodeunpublishvolume)
- [NodeUnstageVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#nodeunstagevolume)
