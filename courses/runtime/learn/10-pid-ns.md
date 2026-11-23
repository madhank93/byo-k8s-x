---
title: Make it pid 1
concepts: [pid namespace, CLONE_NEWPID, pid 1, init signals, /proc]
---

## Core concept

The container can still see every process on the node in its `/proc`, and
signal them if it is root. A **pid namespace** gives it a process tree of its
own: the first process cloned into it with `CLONE_NEWPID` is **pid 1** there,
its children are 2, 3, …, and nothing outside is visible. From the node, the
same processes have ordinary pids, and the node can still signal and kill them.

Your design already makes this a one-flag change, and the reasons are worth
seeing:

- `init` is the process cloned into the namespace, so it is pid 1, and `exec`
  keeps the pid — the image's program *becomes* pid 1. Had `init` forked the
  program instead, it would be pid 2 under a runtime process of yours.
- `/proc` shows the pid namespace of whoever mounted it. `init` mounts it after
  the clone, so it lists the container's processes only.

Being pid 1 comes with duties a normal process lacks. The kernel delivers no
signal to pid 1 that it has not installed a handler for (except `SIGKILL` and
`SIGSTOP` from outside), so a program that ignores `SIGTERM` cannot be politely
stopped. Pid 1 also inherits every orphan in the namespace and must reap them,
and when it exits the kernel kills everything else in the namespace. That is
why many images run a tiny init (`tini`, `dumb-init`) as their entrypoint.

This stage: the image's process is pid 1 in a namespace of its own, sees only
itself in `/proc`, and its exit code is still `run`'s.

## Go APIs

- `syscall.CLONE_NEWNS | syscall.CLONE_NEWPID` in `Cloneflags`.
- `os.Getpid()` inside the container returns 1.

## Hints

<details><summary>Nudge</summary>

If the container reports pid 1 but still counts the node's processes, look at
when and by whom `/proc` was mounted.
</details>

## Further reading

- [pid_namespaces(7)](https://man7.org/linux/man-pages/man7/pid_namespaces.7.html)
- [tini: why a container wants an init](https://github.com/krallin/tini)
- [Share process namespace between containers in a pod](https://kubernetes.io/docs/tasks/configure-pod-container/share-process-namespace/)
