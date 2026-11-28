---
title: Build your own CSI driver — primer
concepts: [CSI, gRPC, controller service, node service, kubelet plugin, bind mount]
---

## What a CSI driver actually is

Kubernetes does not know how to make a disk. Every PersistentVolume that is
not a hostPath or an NFS share comes from a **CSI driver**: a gRPC server that
speaks the Container Storage Interface, a spec written so that any container
orchestrator (the spec says *CO*) can drive any storage system through the
same dozen calls.

The calls come in three services:

- **Identity** — who the driver is and what it can do. Every caller starts
  here.
- **Controller** — volumes as objects: create, delete, list, expand, attach
  to a node, snapshot. Nothing in it touches a node's mount table; in a real
  deployment it usually runs once per cluster.
- **Node** — volumes as mounts, on the node where a pod runs: stage a volume
  once per node, publish it into each pod that uses it, take both down again.

Kubernetes never calls the controller service itself. Small sidecars do —
the external-provisioner turns a PersistentVolumeClaim into `CreateVolume`,
the external-attacher turns a VolumeAttachment into `ControllerPublishVolume`
— while the kubelet calls the node service directly.

## Where your driver runs

Mounts are Linux, so your driver is built with `GOOS=linux` and copied into the
first worker, `byok8s-worker`, where it runs as root, in the node's own mount
namespace, beside the kubelet. Set `GOOS=linux` in your editor.

Every stage starts it the same way:

```
app --endpoint unix:///var/lib/kubelet/plugins/NAME/csi.sock \
    --name NAME --node-id byok8s-worker --root DIR [flags the stage adds]
```

`--name` is a fresh driver name per run, `--root` a directory of the stage's
own where your driver keeps everything: its records of volumes and snapshots,
and the storage behind them. The tester plays the CO: it dials your socket
from the host, makes calls, and then looks at the node — what is mounted where,
which files exist — to judge what the calls did. From `registration` on it
also hands your driver to the real kubelet, and in the last stage to the real
external-provisioner.

## The shape of the course

One driver, one file, grown service by service:

```
identity → capabilities → create → delete → node info     the API, no mounts
registration → topology                                    the kubelet finds you
node-stage → node-publish → node-unpublish                 directories, bind mounts
filesystem → expansion                                     a loop device, ext4
controller-publish → ephemeral-inline                      attach, inline volumes
snapshot-create → snapshot-restore → clone → capacity      data management
idempotency → sidecars-drive-it                            a CO that retries
```

The first volumes are plain directories under `--root`, bind-mounted into
place. `filesystem` adds volumes that are real block devices: an image file,
attached as a loop device and formatted ext4.

## Two things worth knowing before you start

**Every call can come twice.** A CO retries anything that timed out, after a
restart, or just because. The spec makes nearly every call idempotent: the
same `CreateVolume` name returns the same volume, staging what is already
staged succeeds, deleting what is gone succeeds. Most stages grade a repeat.

**The driver forgets nothing it was told.** A driver restarts far more often
than volumes go away. Whatever it knows about a volume lives on disk under
`--root`, never only in memory, and several stages restart your driver halfway
through to check.
