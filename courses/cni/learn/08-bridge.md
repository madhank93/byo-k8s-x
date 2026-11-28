---
title: Join containers on a bridge
concepts: [Linux bridge, gateway address, L2 switching]
---

## Core concept

So far each host end of a veth is a loose wire. A **Linux bridge** is a
software switch: attach every container's host end to it and the containers
share one L2 segment, reaching each other directly by ARP.

Give the bridge the **gateway address** (`10.217.8.1/24`) and it becomes the
containers' router too: their default route points at it, and the node
reaches them through it. This is exactly what the reference `bridge` plugin
and Docker's `docker0` do.

```json
{"type": "byok8s", "bridge": "bk8sbr8", "ipam": {…}}
```

The first ADD creates the bridge; every later one finds it. Concurrent ADDs
race to create it, so "already exists" is success — but a link by that name
that is not a bridge is an error.

## Go APIs

- `netlink.LinkAdd(&netlink.Bridge{…})`, `errors.Is(err, syscall.EEXIST)`.
- `netlink.AddrAdd(bridge, gw)`, `netlink.LinkSetMaster(host, bridge)`.

## Hints

<details><summary>Nudge</summary>

`ip -o link show master BRIDGE` lists what is attached.
</details>

## Further reading

- [The bridge plugin](https://www.cni.dev/plugins/current/main/bridge/)
- [Linux bridge (kernel docs)](https://wiki.linuxfoundation.org/networking/bridge)
