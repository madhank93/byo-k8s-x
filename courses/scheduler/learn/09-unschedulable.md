---
title: A node closed for new work
concepts: [cordon, unschedulable, tolerations, daemonset]
---

## Core concept

`kubectl cordon` is how an operator says "finish what you are running, but take
nothing new" — before a kernel upgrade, or while chasing a fault. It sets one
field:

```yaml
spec:
  unschedulable: true
```

and the node controller then adds a taint to match:

```
node.kubernetes.io/unschedulable:NoSchedule
```

Two things follow from the pair. The field is true the instant the operator
runs the command; the taint arrives a moment later, put there by a controller.
So the field is what you check — it is what is true first, and it is the
statement of intent. And because the intent is expressed *also* as a taint, a
pod can answer it: a pod that tolerates `node.kubernetes.io/unschedulable` is
saying it does not mind a closed node, which is exactly how a DaemonSet keeps
placing its pods on a node being drained.

That is the whole rule for this stage:

- A node with `spec.unschedulable` is not a candidate…
- …unless the pod tolerates the unschedulable taint, in which case it is.

Cordon evicts nothing. A pod already running on the node stays running — the
taint's effect is `NoSchedule`, not `NoExecute` — and, as ever, nothing
downstream enforces any of it: bind a pod to a cordoned node yourself and the
kubelet runs it without complaint.

You may notice your scheduler already avoids cordoned nodes, because stage 8
taught it to avoid tainted ones. The field check is still worth writing: it is
true before the taint exists, and it is what `kubectl uncordon` clears first.

## Go APIs

- `node.Spec.Unschedulable`.
- The taint key `node.kubernetes.io/unschedulable`, matched against
  `pod.Spec.Tolerations` with the same toleration logic you wrote in stage 8.

## Hints

<details><summary>Nudge</summary>

If cordon is also a taint, what does a pod tolerating that taint mean the
operator has to accept?
</details>

<details><summary>Approach</summary>

One small function beside `tolerates`: a cordoned node is a candidate only for
a pod whose tolerations answer a `node.kubernetes.io/unschedulable:NoSchedule`
taint. Reuse the toleration matcher rather than writing a second one.
</details>

## Further reading

- [Safely drain a node](https://kubernetes.io/docs/tasks/administer-cluster/safely-drain-node/)
- [Taints and tolerations: built-in taints](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/#taint-nodes-by-condition)
