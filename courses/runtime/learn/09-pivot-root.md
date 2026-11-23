---
title: Make the rootfs its root
concepts: [pivot_root, chroot escape, MNT_DETACH, proc filesystem, /dev]
---

## Core concept

chroot changes one process's idea of `/`. It does not change the mount table,
and it does not stop a root process from leaving: chroot into a subdirectory,
then `chdir("..")` enough times, and the kernel lets you walk up past the old
root because you are no longer under the one it checks against. Then
`chroot(".")` and you are on the node.

**pivot_root** moves the root of the whole mount namespace instead:

```
pivot_root(new_root, put_old)
```

makes `new_root` the namespace's `/` and moves the old root to `put_old`
underneath it. Then unmount `put_old` with `MNT_DETACH` and remove the
directory. Now the node's filesystem is not hidden — it is not mounted in this
namespace at all, and no `..` reaches it.

`pivot_root` has preconditions, and its `EINVAL` does not say which failed:
`new_root` must be a mount point (the overlay is), and neither it nor its
parent may be a shared mount (stage 8 made everything private).

A real root also needs the kernel filesystems programs expect:

- **/proc**, mounted fresh (`mount -t proc proc /proc`). Without it `ps`,
  `/proc/self` and much of the Go runtime's introspection fail.
- **/dev**, as a small tmpfs holding only the standard character devices —
  `null`, `zero`, `full`, `random`, `urandom`, `tty` — never the node's own
  `/dev`, which holds its disks.

This stage: the same `run`, with the container's `/` its rootfs, the old root
gone from its mount table, `/proc` and `/dev` mounted, and the process started
in the image's `WorkingDir`.

## Go APIs

- `syscall.PivotRoot(newRoot, putOld)`, then `syscall.Chdir("/")`.
- `syscall.Unmount("/.oldroot", syscall.MNT_DETACH)`, `os.Remove`.
- `syscall.Mount("proc", "/proc", "proc", flags, "")`.
- `syscall.Mount("tmpfs", "/dev", "tmpfs", syscall.MS_NOSUID, "mode=755")` and
  `syscall.Mknod("/dev/null", syscall.S_IFCHR|0o666, 1<<8|3)`.

## Hints

<details><summary>Nudge</summary>

After `pivot_root` your current directory still points into the old root.
`chdir("/")` before you unmount it, and use paths relative to the new root from
then on.
</details>

<details><summary>Approach</summary>

In `init`, replace the chroot: make `<rootfs>/.oldroot`, pivot, chdir `/`,
detach and remove `/.oldroot`, then mount `/proc` and populate `/dev`, then
chdir to the working directory and exec.
</details>

<details><summary>Implementation</summary>

mknod's mode is filtered by the umask, so the device nodes come out 0644 rather
than 0666 unless you chmod them after creating them. A program that opens
`/dev/null` for writing as a non-root user cares.
</details>

## Further reading

- [pivot_root(2)](https://man7.org/linux/man-pages/man2/pivot_root.2.html)
- [chroot(2): the escape it documents](https://man7.org/linux/man-pages/man2/chroot.2.html)
- [OCI runtime spec: default devices](https://github.com/opencontainers/runtime-spec/blob/main/config-linux.md#default-devices)
