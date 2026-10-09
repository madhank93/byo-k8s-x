---
title: A port only one pod can hold
concepts: [hostport, nodeports, kubelet-admission, fit]
---

## Core concept

Most of what a pod asks for can be shared. A node with 11 CPUs can split them
among many pods. A **host port** cannot be split. A container that declares
`hostPort: 8080` claims port 8080 on the node itself — on its real network
interface, not inside the pod — and there is only one of those per node.

So a second kind of fit sits beside resources: a pod fits a node only if none
of the ports it asks for on the host is already held there, by any pod,
whoever placed it. The same "every pod already on the node" view you built for
CPU and memory answers this one too.

Get it wrong and the pattern from the last stage repeats. The API server
accepts the binding, and the kubelet refuses to run the pod:

```
Pod was rejected: Predicate NodePorts failed: node(s) didn't have free ports
for the requested pod ports
```

The pod goes to `Failed` with reason `NodePorts` and never comes back to you.

A host port is identified by its number **and** its protocol: TCP 8080 and
UDP 8080 are different ports, and a pod asking for TCP 8081 does not collide
with one holding TCP 8080. A container that declares a `containerPort` with no
`hostPort` claims nothing on the node at all. (The full rule also considers
`hostIP`: two pods can share a port number when each binds a different
specific address. The tester's pods leave `hostIP` empty, which means every
address, so number and protocol are enough here.)

For this stage, keep the CPU and memory check and add this one. The tester
asks for host port 8080 on one pod at a time until one has nowhere to go —
and then asks for a different port, which must still find a node.

## Go APIs

- `container.Ports[i].HostPort` (zero means none) and `.Protocol` (empty means
  `TCP`).
- `corev1.ProtocolTCP` for the default.

## Hints

<details><summary>Nudge</summary>

Is "this node already runs a pod with a host port" the same question as "this
node already holds port 8080"?
</details>

<details><summary>Approach</summary>

Collect the `(protocol, hostPort)` pairs held by the pods on the node, skipping
finished pods and ports with no `hostPort`. The pod fits only if none of its
own pairs is already in that set. Treat an empty protocol as TCP on both sides.
</details>

## Further reading

- [Configuration best practices: hostPort](https://kubernetes.io/docs/concepts/configuration/overview/)
- [kube-scheduler plugins, including NodePorts](https://kubernetes.io/docs/reference/scheduling/config/#scheduling-plugins)
