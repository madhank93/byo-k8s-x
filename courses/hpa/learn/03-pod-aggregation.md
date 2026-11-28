---
title: One number per pod, from every node
concepts: [aggregation, concurrency, per-pod usage]
---

## Core concept

A container is the unit the kubelet measures; a pod is the unit everyone asks
about. A pod's usage is the **sum** of its containers' — CPU rates and working
sets alike.

Pods live on every node, so the program now scrapes every node. A scrape takes
as long as the kubelet takes to resample — ten seconds or more — and doing
nodes one after another multiplies that by the node count. Scrape them
concurrently, each node's two samples on its own goroutine, and merge.

The program prints only the pods in its kubeconfig's namespace, by bare name.
The namespace comes from the kubeconfig's current context, exactly as
`kubectl` finds it.

## Go APIs

- `CoreV1().Nodes().List` for the node names.
- `sync.WaitGroup` and a `sync.Mutex` around the merged map.
- `clientcmd.ClientConfig.Namespace()` returns the context's namespace.

## Hints

<details><summary>Nudge</summary>

Key each container `namespace/pod/container`; cutting the last segment off
gives you the pod's key.
</details>

<details><summary>Approach</summary>

Wrap stage 2 into `collect(nodes)`: one goroutine per node runs both scrapes
and writes rates into a shared map under a lock. Fold that map by pod, then
print the keys that start with your namespace.
</details>

## Further reading

- [Go: sync.WaitGroup](https://pkg.go.dev/sync#WaitGroup)
- [kubeconfig namespaces](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/#context)
