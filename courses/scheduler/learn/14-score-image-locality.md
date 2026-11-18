---
title: Where it can start now
concepts: [scoring, imagelocality, node status, thresholds]
---

## Core concept

Three workers, all equally empty, and a pod whose image is 500MB. Two of them
have to download it before the pod can start; one already has it. Your
scheduler, which scores only on room, tosses a coin.

A node's cache is part of what makes it a good answer, and the node reports it:

```
$ kubectl get node byok8s-worker3 -o jsonpath='{.status.images[*]}'
{"names":["registry.k8s.io/e2e-test-images/agnhost:2.53"],"sizeBytes":52732205}
```

`status.images` is the kubelet's list of what it has pulled, with sizes. Score a
node up for already holding what the pod needs and the pod starts in seconds
instead of minutes.

**Size is the score, not count.** Ten 2MB images already present are worth
nothing; one 800MB image is worth everything. What matters is bytes not
downloaded.

**Below a floor, do not care.** The default scheduler ignores anything under
23MB and caps the score at 1000MB:

```
score = 0                                       when bytes ≤ 23MB
      = 100                                     when bytes ≥ 1000MB
      = (bytes - 23MB) / (1000MB - 23MB) * 100  in between
```

The floor is the interesting half. A small image pulls so fast that preferring
a node for it would be trading a real imbalance for an imaginary saving — and
every pod in this course so far runs a 268KB pause image, which is exactly the
case the floor exists to ignore. Skip the floor and your scheduler starts
herding pods toward whichever node happens to have a common base layer.

Add it to the total, as before:

```
node = argmax( leastAllocated + balancedAllocation + imageLocality )
```

Notice what a small score does when the others tie: every empty worker scores
identically on room and evenness, so a locality score of 2 decides it outright.
A score does not have to be large to be the deciding one — it has to be the
only thing that differs.

## Go APIs

- `node.Status.Images` is `[]corev1.ContainerImage`, each with `Names []string`
  and `SizeBytes int64`. A single image is listed under several names, usually
  a tag and a digest, so match the pod's `Image` against all of them.
- `slices.Contains(img.Names, c.Image)` is the whole lookup. Build a map from
  name to size if you prefer to do it once per node.
- Compare like with like: `Image` on a container is whatever the pod author
  wrote, so `nginx` and `docker.io/library/nginx:latest` are the same image
  named two ways. The real scheduler normalizes; matching what is written is
  enough here.

## Hints

<details><summary>Nudge</summary>

The pod cannot start until its image is on the node. Which node can start it
now?
</details>

<details><summary>Approach</summary>

Sum the sizes of the pod's images that the node already reports, scale that
between the 23MB floor and the 1000MB cap to a score out of 100, and add it to
the total.
</details>

## Further reading

- [Scheduler configuration: ImageLocality](https://kubernetes.io/docs/reference/scheduling/config/)
- [Images: pull policy and the node cache](https://kubernetes.io/docs/concepts/containers/images/)
