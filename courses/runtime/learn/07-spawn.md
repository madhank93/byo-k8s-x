---
title: Run the image's process
concepts: [chroot, fork and exec, exit status, environment, cleanup]
---

## Core concept

Everything so far was preparation. `run` puts it together: pull, mount a fresh
rootfs for a new container, start the image's process inside it, wait, and
clean up.

The simplest way to make a process see the rootfs as `/` is **chroot**: the
child changes its root directory before exec, so `/bin/probe`, the working
directory and every path the program opens resolve inside the image. It is
not isolation — the next stages show how many ways out a chroot leaves — but
it is enough to run the image.

What a runtime owes the caller:

- **The image's environment, and nothing else.** The runtime's own variables
  (its PATH, its HOME, whatever the shell had) must not leak in.
- **The process's stdio.** Its output is the container's output.
- **Its exit code.** `run` exits with whatever the container exited with —
  that is how a kubelet, or a shell script, knows it failed.
- **No trace once it is gone.** The overlay is unmounted and the container's
  directory, writable layer included, removed. Only the shared layers and
  blobs stay.

This stage: `run [--root DIR] REF [ARG…]` runs the image's process as `config`
described it.

## Go APIs

- `exec.Cmd{Path, Args, Env, Dir, Stdin, Stdout, Stderr}` — set `Path` and
  `Args` yourself; `exec.Command` would look the name up on the node.
- `SysProcAttr: &syscall.SysProcAttr{Chroot: rootfs}`: the child chroots, then
  changes to `Dir`, then execs — so `Dir` and `Path` are inside the image.
- `*exec.ExitError` and its `ExitCode()`.
- `crypto/rand` for a container id.

## Hints

<details><summary>Nudge</summary>

`os.Exit` skips deferred calls. Clean up before you exit with the container's
code, not in a defer around the exit.
</details>

<details><summary>Approach</summary>

Generate an id, mount the rootfs, `defer` the removal, build the `exec.Cmd`
with the chroot, run it, and turn an `ExitError` into a code. Return that code
from the function that holds the defer, and call `os.Exit` with it one level up.
</details>

## Further reading

- [chroot(2)](https://man7.org/linux/man-pages/man2/chroot.2.html)
- [syscall.SysProcAttr](https://pkg.go.dev/syscall#SysProcAttr)
- [runc: what it means to create and start a container](https://github.com/opencontainers/runtime-spec/blob/main/runtime.md)
