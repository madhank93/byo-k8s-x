---
title: Publish it into a pod's path
concepts: [NodePublishVolume, target path, read-only bind mount]
---

## Core concept

The second step: `NodePublishVolume` makes a staged volume appear at a
`target_path`, one per pod. The kubelet bind-mounts that path into the pod's
container, so whatever is mounted at the target is what the pod sees at its
`mountPath`.

Publishing is a bind mount of the staging path. Two details:

- **The target is the driver's to create.** The CO makes its parent
  directory; the driver makes the target itself, and so removes it again on
  unpublish.
- **`readonly` is per publish.** Two pods may share a volume, one writing and
  one only reading. A bind mount inherits its source's flags, so read-only is
  a second call: remount the new bind with `MS_RDONLY`. That changes only the
  new mount — the staging path and the writer's target stay writable.

This stage: implement `NodePublishVolume`, idempotent on a target that is
already published, honouring `readonly`.

## Go APIs

- `os.MkdirAll` for the target.
- `syscall.Mount("", dst, "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_RDONLY, "")`.

## Hints

<details><summary>Nudge</summary>

Mounting with `MS_BIND|MS_RDONLY` in one call is silently read-write. Linux
only applies flags to a bind mount on a remount.
</details>

<details><summary>Approach</summary>

The staging helper from the last stage, with a `readonly` flag: bind if
nothing is mounted at the target, then remount read-only if asked.
</details>

## Further reading

- [NodePublishVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#nodepublishvolume)
- [mount(2): MS_REMOUNT with MS_BIND](https://man7.org/linux/man-pages/man2/mount.2.html)
