---
title: Only where it fits
concepts: [requests, allocatable, kubelet-admission, outofcpu]
---

## Core concept

A pod can say what it needs. Each container's `resources.requests` names an
amount of CPU and memory, and a node publishes how much it has to give in
`status.allocatable`. A pod fits a node when its requests, added to the
requests of **every pod already on that node**, stay within what the node
allocates.

"Every pod already on that node" is the part that changes your program. Until
now you only needed the pods waiting for you. To judge a fit you need all of
them — including pods your scheduler never saw, placed by the default
scheduler, a DaemonSet, or bound by hand. A node running system pods is not
empty just because you have not put anything on it yet. So you need a second
view of pods: not just the waiting ones, but every pod, so you can add up what
each node already carries. A pod that has finished (`Succeeded` or `Failed`)
holds nothing.

Binding still checks nothing, but this time something downstream does. Bind a
pod to a node it does not fit and the API server accepts it — and then the
kubelet refuses to run it:

```
Pod was rejected: Node didn't have enough resource: cpu,
requested: 6000, used: 6100, capacity: 11000
```

The pod goes to `Failed` with reason `OutOfcpu`, and it stays there. A failed
pod is never handed back to a scheduler. Overcommitting a node does not delay
the pod; it loses it.

When no node fits, bind nothing. Leave the pod waiting — it is not an error to
have more work than room. (Noticing when room appears and trying again is a
later stage.)

For this stage, fit CPU and memory. The tester asks for more than half a node
per pod, so each node takes exactly one, and the last pod has nowhere to go.
It creates pods one at a time, waiting for each to be placed, so your view of
the cluster is always current; what happens when it is not is a later stage.

The full rule also counts init containers and the pod's overhead. The tester's
pods have neither, so the sum of the containers' requests is enough here.

## Go APIs

- A second pod informer with no field selector — on the node factory, which has
  none — and its lister, to find the pods on a node.
- `resource.Quantity` methods `Add` and `Cmp`; `corev1.ResourceCPU`,
  `corev1.ResourceMemory`.
- `node.Status.Allocatable[name]`, `container.Resources.Requests[name]`.

## Hints

<details><summary>Nudge</summary>

A node you have never placed a pod on — is it empty?
</details>

<details><summary>Approach</summary>

Add up requests per node from the all-pods lister, skipping finished pods. In
`pick`, keep only nodes that are Ready *and* where the pod's requests plus the
node's current total stay within allocatable. If nothing is left, print that
the pod is waiting and bind nothing.
</details>

## Further reading

- [Resource management for pods and containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Node allocatable](https://kubernetes.io/docs/tasks/administer-cluster/reserve-compute-resources/#node-allocatable)
