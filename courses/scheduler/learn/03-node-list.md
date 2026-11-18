---
title: Choose a node that is there
concepts: [nodes, ready-condition, informers, sharedinformerfactory]
---

## Core concept

Until now the node was handed to you. From this stage on your program chooses,
and the first thing to learn about choosing is that the node list is not a
list of places a pod can run.

A Node object is a record the API server keeps. The machine behind it may have
gone away, may be restarting, or may never have existed at all — the object
stays until someone deletes it. What tells you whether a node can run a pod
right now is its **`Ready` condition**, which the kubelet keeps refreshing:
`True` means a kubelet is there and healthy; `False` or `Unknown` means a pod
bound there will sit `Pending`, because nothing will ever start it.

Remember that binding is not checked against the node. The API server accepts
a binding to a node that is not Ready exactly as it accepts one to a node that
is. The only thing between a pod and a dead node is your check.

To choose without asking the API server on every pod, keep your own copy of the
nodes: a second informer. It needs a factory of its own. The field selector you
set on the pod factory with `WithTweakListOptions` is applied to *every*
informer that factory makes — and nodes have no `spec.schedulerName`, so a node
list sent with it is refused outright and that informer never syncs. Sync the
node informer before you start handling pods, or the first pods you see find no
nodes at all.

For this stage: with no `--node`, choose among the nodes whose `Ready`
condition is `True`, taking them in turn. The tester adds a node that is listed
but not Ready; none of your pods may land on it. The control plane is Ready too,
and for now your pods may land there — its taint says to keep off, and honouring
taints is a later stage. Keep `--node` working: when it is given, it still wins.

## Go APIs

- `informers.NewSharedInformerFactory(cs, 0)` for nodes, separate from the
  field-selected pod factory.
- `factory.Core().V1().Nodes().Lister()` and `.List(labels.Everything())`.
- `corev1.NodeReady` in `node.Status.Conditions`, compared with
  `corev1.ConditionTrue`.
- `cache.WaitForCacheSync` on the node informer before starting the pod one.

## Hints

<details><summary>Nudge</summary>

What does a node look like, from the API, when the machine behind it is gone?
</details>

<details><summary>Approach</summary>

A node informer on its own factory, synced first. A small `isReady` that finds
the `Ready` condition. In the add handler, when there is no `--node`, list the
nodes, keep the ready ones, sort them, and take the next in turn.
</details>

## Further reading

- [Nodes: node status and conditions](https://kubernetes.io/docs/reference/node/node-status/#condition)
- [client-go informers](https://pkg.go.dev/k8s.io/client-go/informers)
