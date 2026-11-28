---
title: Pin volumes to a node
concepts: [VOLUME_ACCESSIBILITY_CONSTRAINTS, accessibility requirements, ResourceExhausted]
---

## Core concept

A volume on a node's disk can only be used on that node, and the driver has
to say so. Advertising the plugin capability `VOLUME_ACCESSIBILITY_CONSTRAINTS`
turns on two halves of the conversation:

- In `CreateVolume`, the CO sends `accessibility_requirements`: `requisite`
  topologies the volume must be reachable from, and `preferred` ones in
  order. Kubernetes builds them from the nodes that run the driver and, with
  `WaitForFirstConsumer`, puts the node the pod was scheduled to first.
- In the answer, the volume carries `accessible_topology`: where it can
  actually be used. Kubernetes turns that into the PersistentVolume's node
  affinity, and from then on the scheduler only sends the volume's pods there.

This driver can only make volumes on its own node. If the requirements leave
that node out, no volume it could make would satisfy them:
`ResourceExhausted`, the code the spec gives for "cannot be provisioned
where asked".

This stage: advertise the capability, answer every volume with
`[{topology.byok8s.dev/node: <node id>}]`, and refuse requisites that exclude
this node.

## Go APIs

- `csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS`.
- `req.GetAccessibilityRequirements().GetRequisite()`, each a
  `*csi.Topology` with a `Segments` map.

## Hints

<details><summary>Nudge</summary>

No requirements at all means anywhere will do.
</details>

## Further reading

- [CreateVolume: accessibility_requirements](https://github.com/container-storage-interface/spec/blob/master/spec.md#createvolume)
- [Kubernetes CSI topology](https://kubernetes-csi.github.io/docs/topology.html)
