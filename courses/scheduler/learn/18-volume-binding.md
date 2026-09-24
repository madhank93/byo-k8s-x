---
title: Where the data already is
concepts: [persistentvolume, persistentvolumeclaim, nodeaffinity, filtering]
---

## Core concept

A pod that mounts a network disk can run anywhere: the disk comes to the node.
A pod that mounts a *local* disk cannot. The disk is in one machine, and the
pod has to go to the machine.

Kubernetes writes that down on the volume, not on the pod:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata: {name: data-a}
spec:
  capacity: {storage: 64Mi}
  accessModes: [ReadWriteOnce]
  local: {path: /mnt/disks/a}
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - {key: kubernetes.io/hostname, operator: In, values: [worker-3]}
```

The pod never mentions `worker-3`. It mentions a claim; the claim names a
volume; the volume names the node. Your filter walks that chain:

```
pod.Spec.Volumes[].PersistentVolumeClaim.ClaimName
  → PVC.Spec.VolumeName
    → PV.Spec.NodeAffinity.Required
```

**It is the same `NodeSelector` type as stage 7.** `PV.Spec.NodeAffinity.Required`
is a `*corev1.NodeSelector`, with terms ORed and expressions within a term
ANDed — so the matcher you wrote for node affinity does this job unchanged. The
only new work is finding the terms.

**Required, so it is a filter, not a score.** A pod bound to a volume on the
fullest node goes to the fullest node. Room, balance and image locality all
still score, but they score one candidate.

**A volume with no node affinity constrains nothing.** That is the network-disk
case, and it is the common one: skip it and let the other filters decide.

**An unresolvable claim means wait, not place.** A claim that does not exist,
or one still `Pending` with no volume, gives you no node to check — so there is
no node you may bind to. Placing the pod anyway is the bug this stage is built
to catch: it succeeds on a cluster where every node happens to be able to
mount, and strands the pod forever on one where they cannot.

That waiting is where the real scheduler does much more than this stage asks.
A `StorageClass` with `volumeBindingMode: WaitForFirstConsumer` deliberately
leaves the claim unbound *until a pod needs it*, and then the scheduler picks
the node **first** and tells the provisioner to make the volume there. Your
scheduler leaves those pods to the binder; the `VolumeBinding` plugin in the
real one runs that whole negotiation, including reserving the volume before the
bind and undoing the reservation when the bind fails.

## Go APIs

- `pod.Spec.Volumes` is a `[]corev1.Volume`. Only the entries with a non-nil
  `.PersistentVolumeClaim` matter; a `configMap`, `emptyDir` or `secret` volume
  pins nothing.
- `corelisters.PersistentVolumeClaimLister` is namespaced —
  `claims.PersistentVolumeClaims(pod.Namespace).Get(name)`. The volume lister is
  not: PersistentVolumes are cluster scoped.
- `claim.Spec.VolumeName` is empty until the claim is bound. `claim.Status.Phase`
  says the same thing in words.
- `pv.Spec.NodeAffinity` is a `*corev1.VolumeNodeAffinity` — one field,
  `Required`. Both it and `Required` can be nil.
- Both informers come off the factory without a field selector, the way the node
  and all-pods informers do, and both belong in your `WaitForCacheSync` call.

## Hints

<details><summary>Nudge</summary>

You already wrote the hard part in stage 7. Find the `NodeSelector` the volume
carries and hand it to the matcher you have.
</details>

<details><summary>Approach</summary>

One helper, looping over the pod's volumes: no claim, skip; claim missing or
unbound, return false; volume with no node affinity, skip; otherwise require one
term to match the node's labels.
</details>

## Further reading

- [Local volumes](https://kubernetes.io/docs/concepts/storage/volumes/#local)
- [Volume binding mode](https://kubernetes.io/docs/concepts/storage/storage-classes/#volume-binding-mode)
- [VolumeBinding plugin](https://github.com/kubernetes/kubernetes/tree/master/pkg/scheduler/framework/plugins/volumebinding)
