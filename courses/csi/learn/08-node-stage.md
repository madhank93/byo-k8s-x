---
title: Stage a volume on the node
concepts: [NodeStageVolume, staging path, bind mount, mountinfo]
---

## Core concept

A volume reaches a pod in two steps on the node, and this is the first.
`NodeStageVolume` makes the volume available **once per node**, at a
`staging_target_path` the CO chose: for a network disk, this is where it is
attached and its filesystem mounted. Every pod on the node that uses the
volume is then published from that one mount.

For this driver's directory volumes, staging is a **bind mount**: the
volume's directory under `--root` mounted again at the staging path, so the
same files are reachable from both places. A file written through one
staging path is there through any other.

The CO created the staging directory; the driver only mounts on it. And as
always the call can come twice, so staging a path that already has the
volume on it succeeds without mounting again — stacked mounts are how a node
ends up with volumes that never fully unmount. The kernel's own list of
mounts, `/proc/self/mountinfo`, says what is mounted where; its fifth field
is the mount point.

This stage: advertise `STAGE_UNSTAGE_VOLUME` and implement
`NodeStageVolume`: `NotFound` for a volume you never made, `InvalidArgument`
for a missing path or capability.

## Go APIs

- `syscall.Mount(src, dst, "", syscall.MS_BIND, "")`.
- `os.ReadFile("/proc/self/mountinfo")`, `strings.Fields`.

## Hints

<details><summary>Nudge</summary>

`mount` on a path that is already a mount point does not fail; it stacks. The
check has to be yours.
</details>

<details><summary>Approach</summary>

Validate, load the record (`NotFound` if it is missing), and if nothing is
mounted exactly on the staging path, bind the data directory onto it.
</details>

## Further reading

- [NodeStageVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#nodestagevolume)
- [proc_pid_mountinfo(5)](https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html)
- [mount(2): bind mounts](https://man7.org/linux/man-pages/man2/mount.2.html)
