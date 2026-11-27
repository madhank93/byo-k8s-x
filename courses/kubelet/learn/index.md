---
title: Build your own kubelet — primer
concepts: [kubelet, Node, node lease, CRI, pod lifecycle]
---

## What a kubelet actually does

Nothing in the control plane runs a container. The API server stores what
should exist, the scheduler writes a node name into each pod, and that is
where the control plane stops. On every node a **kubelet** picks up from
there: it tells the cluster the machine exists, watches for the pods bound to
it, asks the container runtime to run them, and writes back what happened.

So a kubelet faces two ways. Upward it is an API client like any controller —
it owns a Node object, a Lease, and the status of every pod on its node.
Downward it drives the container runtime over CRI, the gRPC interface
containerd and CRI-O both serve. This course builds both halves, in that
order: first the node's place in the cluster, then the pods.

## Where your program runs

Your program is built with `GOOS=linux` and runs as root inside the kind
worker `byok8s-worker`, next to that node's own kubelet. It does not replace
it. Each stage starts yours as the kubelet for a Node of its own, with a name
the stage picks:

```
app --kubeconfig PATH --hostname-override NAME --register-with-taints KEY=VALUE:EFFECT
```

The flags are the real kubelet's. The kubeconfig reaches the API server over
the kind network. The taint keeps the scheduler from sending other pods to a
node that only lives for one stage; the stage deletes the Node when it ends.

Leave out the `kubernetes.io/os` label your Node would normally carry: the
kube-system DaemonSets select on it, and they would put pods on your node that
nothing will ever run.

## The shape of the course

One long-running program. Stages 1–3 make the Node real: registered,
reporting the machine, and alive through its lease. The pod half comes after
— watch the pods bound to you, run them through CRI, and report their status
back.

## Two things worth knowing before you start

A Node is not created by the cluster; it is claimed by whoever says they are
it. The control plane learns that a machine exists only because its kubelet
registered, and that it is still alive only because the kubelet keeps saying
so. Stop saying so and, about forty seconds later, the node lifecycle
controller marks the node `Unknown` and starts evicting its pods.

Status belongs to the kubelet. Spec is what others want; status is what the
kubelet has seen. Write status through the `status` subresource and leave the
spec to the people who own it.
