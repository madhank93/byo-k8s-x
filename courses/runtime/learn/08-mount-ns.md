---
title: Give it its own mount table
concepts: [mount namespace, CLONE_NEWNS, mount propagation, re-exec init]
---

## Core concept

In stage 7 the container's overlay is mounted in the node's mount table, where
every process on the node can see it, and it stays there if the runtime dies
before cleaning up. A **mount namespace** fixes both: a process cloned with
`CLONE_NEWNS` gets a private copy of the mount table, mounts made inside it
are invisible outside, and the kernel tears them all down when the last
process in the namespace exits.

Two things make this harder than one flag.

**The mounts have to be made from inside.** The overlay must be mounted after
the clone, by a process already in the new namespace, and before the image's
program starts. Go cannot run code in a child between fork and exec, so
runtimes re-execute themselves: `run` starts `/proc/self/exe init …` in the
new namespaces, and `init` does the setup, then `exec`s the image's program,
which replaces it. runc works exactly this way (`runc init`).

**Propagation.** A new mount namespace starts as a copy, and on a systemd
host — a kind node included — `/` is a **shared** mount: mounts inside a peer
propagate back out to the node. Before mounting anything, `init` must make its
copy private:

```
mount --make-rprivate /      # MS_REC | MS_PRIVATE on "/"
```

Skip that and the "private" mount table leaks every mount into the node's.

This stage: same `run` as before, with the rootfs mounted inside a new mount
namespace. While the container runs, the node's mount table shows nothing of
it, and when it exits there is nothing to unmount.

## Go APIs

- `exec.Command("/proc/self/exe", "init", spec)` with
  `SysProcAttr{Cloneflags: syscall.CLONE_NEWNS}`.
- `syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, "")`.
- `syscall.Chroot`, `syscall.Chdir`, then `syscall.Exec(path, argv, env)` —
  exec replaces `init`, so the image's process inherits the namespace.
- `encoding/json` to hand `init` the rootfs, overlay options and process.

## Hints

<details><summary>Nudge</summary>

Split stage 5's mount into two halves: preparing the directories (which the
parent can do) and the `mount` call itself (which `init` must do).
</details>

<details><summary>Approach</summary>

Add a hidden `init` subcommand. `run` prepares the layers and container
directory, marshals what `init` needs, and starts itself with `CLONE_NEWNS`.
`init` makes `/` recursively private, mounts the overlay, chroots, changes to
the working directory and execs. The parent's cleanup no longer needs to
unmount — the namespace took the mount with it — but must still remove the
directory.
</details>

<details><summary>Implementation</summary>

From here on `run`'s child is your own program, not the image's, so stdio
must be passed through to it explicitly, and its exit status is the image
program's because `exec` kept the same process.
</details>

## Further reading

- [mount_namespaces(7)](https://man7.org/linux/man-pages/man7/mount_namespaces.7.html)
- [Shared subtrees (kernel docs)](https://docs.kernel.org/filesystems/sharedsubtree.html)
- [runc's init: libcontainer/nsenter](https://github.com/opencontainers/runc/tree/main/libcontainer/nsenter)
