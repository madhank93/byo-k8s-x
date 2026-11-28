---
title: Mount emptyDir and hostPath volumes
concepts: [volumes, volumeMounts, emptyDir, hostPath, bind mounts, tmpfs]
---

## Core concept

A container's filesystem lives and dies with the container. A restart starts
from a fresh snapshot, and two containers in one pod never see each other's
files. **Volumes** are the pod's storage. They are declared once in
`spec.volumes`, and each container mounts the ones it wants with
`volumeMounts`.

The kubelet makes each volume a directory on the node, then **bind-mounts**
it into the container at `mountPath`:

- **emptyDir**: a new empty directory under the pod's own directory,
  `ROOT/pods/UID/volumes/kubernetes.io~empty-dir/NAME`. Every container that
  mounts it sees the same files, and the files outlive container restarts.
  It is removed when the pod is. With `medium: Memory` it is a **tmpfs**,
  held in RAM.
- **hostPath**: a path on the node itself. The kubelet creates nothing and
  removes nothing, except that `type: DirectoryOrCreate` makes the
  directory when it is missing. This is the node's own data, so it outlives
  every pod.

A mount with `readOnly: true` binds read-only. `subPath` mounts one entry
inside the volume instead of the whole volume.

The order matters on the way out. Unmount the tmpfs volumes first, then
remove the pod's directory. Never follow a hostPath into a pod's directory
to delete it.

## Go APIs

- `oci.WithMounts([]specs.Mount{{Destination, Type: "bind", Source,
  Options: []string{"rbind", "ro" or "rw"}}})`.
- `syscall.Mount("tmpfs", dir, "tmpfs", 0, "")`,
  `syscall.Unmount(dir, syscall.MNT_DETACH)`.
- A directory is a mount point when `stat` gives it a different device from
  its parent.

## Hints

<details><summary>Nudge</summary>

Your kubelet runs in the node's mount namespace, so a tmpfs it mounts on the
node is the one the bind mount picks up.
</details>

<details><summary>Approach</summary>

Before you create a container, work out a host path for every volume,
making emptyDirs (and mounting the tmpfs ones) as you go. Turn the
container's `volumeMounts` into bind mounts from those paths. When a pod is
gone, unmount anything mounted under its directory, then remove the
directory.
</details>

## Further reading

- [Volumes](https://kubernetes.io/docs/concepts/storage/volumes/)
- [emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir)
- [hostPath](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath)
