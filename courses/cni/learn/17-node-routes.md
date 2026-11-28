---
title: Route between nodes
concepts: [pod CIDR per node, host-gw, direct routing]
---

## Core concept

Every node hands out addresses from **its own slice** of the pod network,
so an address says which node a pod is on. Give each node a route to the
others' slices, through the other node's address, and pods talk across nodes
with their own addresses and no encapsulation. This is flannel's `host-gw`
backend, and what kindnet does in this very cluster.

```json
"peers": [{"subnet": "10.217.17.128/25", "via": "172.18.0.4"}]
```

On ADD, the node gets `10.217.17.128/25 via 172.18.0.4`. A packet from a
container goes to its gateway (the bridge), the node routes it to the peer,
the peer's bridge delivers it.

It only works when the nodes share an L2 network — the next hop must be
directly reachable. Every ADD repeats the routes, so replace rather than add.

## Go APIs

- `netlink.RouteReplace(&netlink.Route{Dst: subnet, Gw: via})`.

## Further reading

- [flannel backends: host-gw](https://github.com/flannel-io/flannel/blob/master/Documentation/backends.md#host-gw)
