---
title: Format and mount a block device
concepts: [fs_type, loop device, mkfs, format once, autoclear]
---

## Core concept

Real volumes are block devices — a cloud disk, an iSCSI LUN — and a block
device holds no files until it has a filesystem. Staging is where that
happens: the node attaches the device, makes a filesystem on it **if it has
none yet**, and mounts it at the staging path.

A file can stand in for a disk. A **loop device** (`/dev/loopN`) is a block
device whose blocks are a regular file's bytes, so this stage's volumes are
sparse image files under `--root`, as large as the volume's capacity:

1. `CreateVolume` with a mount capability whose `fs_type` is `ext4` makes the
   image. An `fs_type` the driver cannot make is `InvalidArgument`, at create
   time rather than later on a node.
2. `NodeStageVolume` formats the image if no filesystem is on it, attaches it
   and mounts it. `mount -o loop` does both: it picks a free loop device,
   attaches the image, and marks the device *autoclear*, so it detaches
   itself when the last mount of it goes.
3. Publish is still a bind mount of the staging path.

"If it has none yet" is the line that matters. A volume is staged every time
a pod lands on a new node, or after every reboot; a driver that formats on
each stage erases the volume each time. `blkid` reports what is on a device,
and exits 2 when it finds nothing. Format only on that answer — any other
failure is a reason to stop, not to format.

This stage: `fs_type: ext4` volumes, formatted once, mounted through a loop
device that is gone once the volume is unstaged. An empty `fs_type` stays a
directory volume.

## Go APIs

- `os.OpenFile` and `(*os.File).Truncate` for a sparse image.
- `exec.Command("blkid", "-p", img)`, `exec.Command("mkfs.ext4", "-q", "-F", img)`,
  `exec.Command("mount", "-t", "ext4", "-o", "loop", img, path)`.
- `errors.As(err, &exitErr)` and `ExitCode()` to tell "no filesystem" from a
  failure.

## Hints

<details><summary>Nudge</summary>

`syscall.Unmount` of the staging path is enough to detach the device if it
was mounted with `-o loop`. A device you attached yourself with `losetup`
needs `losetup -d` after the unmount.
</details>

<details><summary>Approach</summary>

Record the filesystem type with the volume. In stage: directory volumes bind
as before; image volumes run `blkid`, then `mkfs` on exit status 2, then
`mount -o loop` — all behind the same "already mounted?" check.
</details>

## Further reading

- [loop(4)](https://man7.org/linux/man-pages/man4/loop.4.html)
- [mount(8): loop devices](https://man7.org/linux/man-pages/man8/mount.8.html#LOOP-DEVICE_SUPPORT)
- [NodeStageVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#nodestagevolume)
