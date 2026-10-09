---
title: Place a pod
concepts: [binding, pods/binding, nodename, taints]
---

## Core concept

Finding a pod is half a scheduler. The other half is recording where it goes,
and there is exactly one way to do that: create a **Binding**.

A Binding names a pod and a target node. You post it to the pod's `binding`
subresource, and the API server does the rest — it sets `spec.nodeName` on the
pod for you. You never edit the pod yourself; `spec.nodeName` is not a field a
scheduler writes directly.

Two properties of that write shape everything after it:

- **It happens once.** A pod that already has a node cannot be bound again. The
  second attempt fails with a conflict — `pod … is already assigned to node …` —
  which is what stops two schedulers, or one scheduler twice, from both placing
  the same pod.
- **It is not checked against the node.** The API server does not ask whether
  the pod fits, whether the node is ready, or whether its taints allow it. Bind
  a pod to the control plane, which is tainted `NoSchedule`, and the binding is
  accepted and the kubelet runs the pod. Every rule about where a pod may go is
  the scheduler's to enforce; nothing downstream enforces it for you.

Once bound, the pod is the kubelet's. Your informer, which only asks for pods
with no node, sees it leave the set — delivered as a delete — and the pod never
reaches you again.

For this stage the node is given: bind every waiting pod to the node named by
`--node`, and print

```
bound <namespace>/<name> to <node>
```

Choosing the node yourself is the next stage. Keep printing the `unscheduled`
line from stage 1 as well.

## Go APIs

- `cs.CoreV1().Pods(ns).Bind(ctx, &corev1.Binding{…}, metav1.CreateOptions{})`.
- `corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: …, Namespace: …}, Target:
  corev1.ObjectReference{Kind: "Node", Name: …}}`.
- `flag.String` for `--node`.

## Hints

<details><summary>Nudge</summary>

Where does `spec.nodeName` come from, if not from you writing it?
</details>

<details><summary>Approach</summary>

In the add handler, after printing `unscheduled`, create a Binding for the pod
naming the `--node` node, and print `bound` once it succeeds. Report a failed
binding on stderr rather than exiting — one pod you could not place should not
stop the others.
</details>

## Further reading

- [Binding (API reference)](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/binding-v1/)
- [Assigning pods to nodes](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)
