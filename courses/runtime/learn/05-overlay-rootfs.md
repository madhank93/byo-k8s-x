---
title: Stack the layers with overlayfs
concepts: [overlayfs, lowerdir, upperdir, copy-up, whiteout device]
---

## Core concept

Unpacking every layer into a fresh directory per container works, and costs a
full copy of the image each time. Real runtimes extract each layer **once**,
into a directory of its own, and give every container a union of them through
**overlayfs**:

```
mount -t overlay overlay \
  -o lowerdir=L2:L1,upperdir=U,workdir=W  MERGED
```

- `lowerdir` is a colon-separated list of read-only layers, **top first** —
  the reverse of the manifest's order.
- `upperdir` is the container's own writable layer. Any write lands here; a
  file from a lower layer is copied up before it is modified, so the layers
  themselves never change and every container can share them.
- `workdir` is scratch space overlayfs needs, on the same filesystem as
  `upperdir`, empty, and never shared.
- `MERGED` is the view: the container's root filesystem.

A layer extracted on its own cannot delete anything, because there is nothing
under it yet. So whiteouts are recorded for overlayfs to apply at mount time,
in its own format: a deleted file is a **character device numbered 0/0**, and
an opaque directory carries the extended attribute
`trusted.overlay.opaque=y`. This is the one place the unpacking code differs
from stage 4.

A half-extracted layer directory would be shared by every later container, so
extract into a temporary directory and rename it into place, as you did for
blobs.

This stage: `mount [--root DIR] REF ID` prints the rootfs path of a new
container `ID`; `umount [--root DIR] ID` unmounts it and removes the
container's directory, writable layer included. A layout that works:

```
<root>/layers/sha256/<hex>/          one per layer, shared
<root>/containers/<id>/{upper,work,rootfs}
```

## Go APIs

- `syscall.Mount("overlay", target, "overlay", 0, "lowerdir=…,upperdir=…,workdir=…")`.
- `syscall.Unmount(target, syscall.MNT_DETACH)`.
- `syscall.Mknod(path, syscall.S_IFCHR, 0)` for a whiteout device.
- `syscall.Setxattr(dir, "trusted.overlay.opaque", []byte("y"), 0)`.

## Hints

<details><summary>Nudge</summary>

If the merged view shows files that should be gone, look at the order of
`lowerdir`: leftmost wins.
</details>

<details><summary>Approach</summary>

Give the layer applier a switch: onto a full tree, whiteouts delete; into a
layer directory, they become a 0/0 device or the opaque xattr. `mount` then
extracts any layer it lacks, makes the container's three directories, and
mounts with `lowerdir` built from the manifest's layers reversed.
</details>

<details><summary>Implementation</summary>

`umount` should tolerate a rootfs that is not mounted (`EINVAL`) or already
gone (`ENOENT`) and still remove the directory, so a half-created container
can always be cleaned up.
</details>

## Further reading

- [Overlay filesystem (kernel docs)](https://docs.kernel.org/filesystems/overlayfs.html)
- [containerd's overlay snapshotter](https://github.com/containerd/containerd/tree/main/plugins/snapshots/overlay)
- [mount(2)](https://man7.org/linux/man-pages/man2/mount.2.html)
