---
title: Build an overlay
concepts: [VXLAN, VNI, VTEP, FDB, encapsulation, MTU]
---

## Core concept

Direct routes need the nodes on one L2 segment. An **overlay** removes that
limit: each node wraps pod traffic in UDP addressed to the other node, so it
crosses any IP network. **VXLAN** is the common one: an Ethernet frame inside
UDP, tagged with a 24-bit **VNI** that says which overlay it belongs to.

Each node gets a VXLAN device — its **VTEP** — with an address on an
overlay subnet:

```json
"backend": {"type": "vxlan", "vni": 4218, "port": 4799,
            "device": "bk8svx18", "address": "10.217.218.1/24"},
"peers": [{"subnet": "10.217.18.128/25", "via": "172.18.0.4", "vtep": "10.217.218.2"}]
```

Then, per peer:

- a route: `10.217.18.128/25 via 10.217.218.2 dev bk8svx18`;
- a **forwarding database** entry telling the device where to send frames it
  does not know: `00:00:00:00:00:00 dst 172.18.0.4`. That carries the ARP for
  the peer's VTEP, and the kernel learns its MAC from the reply.

Encapsulation costs 50 bytes. Real overlays lower the pods' MTU to match;
the probe traffic here is small enough not to care.

## Go APIs

- `netlink.Vxlan{VxlanId: vni, Port: port}`.
- `netlink.NeighAppend(&netlink.Neigh{Family: syscall.AF_BRIDGE, Flags: netlink.NTF_SELF, IP: via, HardwareAddr: zero mac})`.

## Further reading

- [RFC 7348: VXLAN](https://datatracker.ietf.org/doc/html/rfc7348)
- [flannel backends: vxlan](https://github.com/flannel-io/flannel/blob/master/Documentation/backends.md#vxlan)
