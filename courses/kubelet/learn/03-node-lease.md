---
title: Hold the node's lease
concepts: [Lease, node heartbeat, kube-node-lease, ownerReferences]
---

## Core concept

Posting the whole NodeStatus every 10 seconds is expensive at scale: every
write is a full object, sent to every watcher of Nodes. So the heartbeat
moved to a small object of its own, a **Lease** in the `kube-node-lease`
namespace, named after the node:

```yaml
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata: {name: NAME, namespace: kube-node-lease, ownerReferences: [Node NAME]}
spec: {holderIdentity: NAME, leaseDurationSeconds: 40, renewTime: …}
```

The kubelet renews `renewTime` every quarter of the duration — 10 seconds —
and the node lifecycle controller counts a node alive while either the lease
or the Ready heartbeat is fresh. With a lease, a real kubelet posts full
status only when something changes, or every five minutes.

The Lease is **owned by the Node**: an `ownerReference` with the Node's name
and UID means deleting the Node lets the garbage collector delete its Lease.
That is why registration has to hand back the Node it created or found — you
need its UID.

This stage: create the Lease if it is missing, then renew it every 10
seconds.

## Go APIs

- `cs.CoordinationV1().Leases("kube-node-lease")`: `Get`, `Create`, `Update`.
- `metav1.NewMicroTime(time.Now())` — `renewTime` is a MicroTime.
- `metav1.OwnerReference{APIVersion: "v1", Kind: "Node", Name, UID}`.

## Hints

<details><summary>Nudge</summary>

Keep the Node that `Create` (or, on AlreadyExists, `Get`) returns. Its UID is
what the ownerReference needs.
</details>

<details><summary>Approach</summary>

On each tick: Get the Lease; on NotFound create it with holder, duration,
renew time and owner; otherwise set `renewTime` and Update. A conflict just
means try again next tick.
</details>

## Further reading

- [Leases: node heartbeats](https://kubernetes.io/docs/concepts/architecture/leases/#node-heart-beats)
- [Node heartbeats](https://kubernetes.io/docs/concepts/architecture/nodes/#heartbeats)
- [KEP-589: efficient node heartbeats](https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/589-efficient-node-heartbeats)
