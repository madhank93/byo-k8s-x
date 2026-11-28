---
title: Say which node you are
concepts: [NodeGetInfo, node id, accessible topology]
---

## Core concept

The node service runs on every node, and each copy has to say which node it
is on. `NodeGetInfo` answers two things:

- `node_id` — how the controller service will name this node, in
  `ControllerPublishVolume` for instance. Kubernetes stores it in the node's
  CSINode object. It comes from `--node-id`.
- `accessible_topology` — the segments that locate this node, as key/value
  pairs. A cloud driver says `zone: us-east-1a`; a driver whose volumes live
  on a node's own disk names the node itself. This driver uses one key,
  `topology.byok8s.dev/node`, set to the node id.

Topology is how a volume and a pod end up on the same machine: the scheduler
only places a pod where its volume is reachable, and reachable means "the
same segments". This stage only reports them; `topology` puts them on the
volumes.

This stage: `NodeGetInfo`, and a `NodeGetCapabilities` that answers (an empty
list is fine).

## Go APIs

- `csi.NodeGetInfoResponse{NodeId, AccessibleTopology: &csi.Topology{Segments: ...}}`.

## Hints

<details><summary>Nudge</summary>

`NodeGetCapabilities` with an empty list is different from not implementing
it: the first says "nothing optional", the second is an error.
</details>

## Further reading

- [NodeGetInfo](https://github.com/container-storage-interface/spec/blob/master/spec.md#nodegetinfo)
- [Kubernetes CSI topology](https://kubernetes-csi.github.io/docs/topology.html)
