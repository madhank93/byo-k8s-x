---
title: Let Kubernetes drive it
concepts: [external-provisioner, StorageClass, WaitForFirstConsumer, PersistentVolume lifecycle]
---

## Core concept

Until now the tester played the CO. This stage hands the whole driver to
Kubernetes:

1. A **StorageClass** names your driver as its `provisioner`, with
   `volumeBindingMode: WaitForFirstConsumer`.
2. A **PersistentVolumeClaim** of that class waits until a pod uses it; the
   scheduler picks the pod's node and marks the claim with it.
3. The **external-provisioner**, a sidecar the tester runs on your node with
   your socket mounted, sees the claim and calls `CreateVolume` — name
   `pvc-<uid>`, the claim's size, and accessibility requirements that put the
   chosen node first. From the answer it creates a **PersistentVolume** whose
   node affinity comes from the volume's `accessible_topology`, and binds it.
4. The **kubelet** stages the volume under
   `/var/lib/kubelet/plugins/kubernetes.io/csi/<driver>/…/globalmount`,
   publishes it into the pod's directory, and starts the container.
5. Deleting the pod unpublishes and unstages; deleting the claim makes the
   provisioner call `DeleteVolume` and remove the PersistentVolume.

Almost nothing here is new to your driver. What is new is that every call
comes from real Kubernetes code, with the arguments and retries it really
uses — the point of a spec is that a driver written against it works with a
CO it has never met.

The one new argument: the StorageClass sets `mountOptions: [noexec]`. The
PersistentVolume inherits them, and the kubelet sends them on every stage and
publish as the capability's `mount_flags`. A driver that drops them mounts
the volume with options nobody asked for.

This stage: honour `mount_flags`, and a pod with a claim of your
StorageClass runs, writes, and leaves nothing behind when it and its claim
are deleted.

## Hints

<details><summary>Nudge</summary>

When a pod is stuck in `ContainerCreating`, `kubectl describe pod` shows the
kubelet's mount errors, and the provisioner's log shows what `CreateVolume`
was sent and answered.
</details>

<details><summary>Approach</summary>

`syscall.Mount` takes flags as bits and options as a string, and a name like
`noexec` has to be translated into one or the other. `mount -o
remount,bind,noexec,ro DST` does that parsing for you, so a bind mount can
take the CO's flags and `readonly` in one remount.
</details>

## Further reading

- [external-provisioner](https://github.com/kubernetes-csi/external-provisioner)
- [Kubernetes CSI: deploying a driver](https://kubernetes-csi.github.io/docs/deploying.html)
- [Volume binding mode](https://kubernetes.io/docs/concepts/storage/storage-classes/#volume-binding-mode)
