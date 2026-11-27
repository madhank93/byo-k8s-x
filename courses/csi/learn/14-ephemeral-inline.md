---
title: A volume that lives with its pod
concepts: [CSI ephemeral volumes, CSIDriver, volume_context, kubelet]
---

## Core concept

Not every volume is a PersistentVolume. A pod can name a CSI driver straight
in its spec:

```yaml
volumes:
  - name: data
    csi:
      driver: NAME
      volumeAttributes: {size: 4Mi}
```

That is a **CSI ephemeral inline volume**: it exists for exactly as long as
the pod does. Nothing provisions it and nothing deletes it — no
`CreateVolume`, no staging, no `DeleteVolume`. The kubelet only calls
`NodePublishVolume` when the pod starts, with a `volume_id` it made up, and
`NodeUnpublishVolume` when the pod goes.

The driver has to know which kind of publish it is getting. The CSIDriver
object says which lifecycles a driver serves (`Ephemeral` here), and with
`podInfoOnMount` the kubelet adds keys to `volume_context`, among them
`csi.storage.k8s.io/ephemeral: "true"`. On that publish the driver creates
the volume and mounts it; on the unpublish of the same id, it unmounts and
**deletes** it — data included.

From this stage on the tester runs pods on the real kubelet.

This stage: serve ephemeral publishes, and delete an ephemeral volume when it
is unpublished.

## Go APIs

- `req.VolumeContext["csi.storage.k8s.io/ephemeral"]`.
- `req.StagingTargetPath` is empty: an ephemeral volume is never staged.

## Hints

<details><summary>Nudge</summary>

`NodeUnpublishVolume` gets no `volume_context`. Whether a volume is ephemeral
has to be something the driver remembers from the publish.
</details>

<details><summary>Approach</summary>

On an ephemeral publish, make a directory volume with the kubelet's id and a
record marked ephemeral, and bind it at the target. On unpublish, after the
unmount, load the record: if it is ephemeral, remove its data and record.
</details>

## Further reading

- [Kubernetes CSI ephemeral local volumes](https://kubernetes-csi.github.io/docs/ephemeral-local-volumes.html)
- [Ephemeral volumes](https://kubernetes.io/docs/concepts/storage/ephemeral-volumes/#csi-ephemeral-volumes)
