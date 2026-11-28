---
title: Assign a static address
concepts: [IPAM, static addresses, result ips, interface index]
---

## Core concept

Deciding which address a container gets is **IPAM**, and CNI treats it as a
separate concern: the config carries an `ipam` section, and in the reference
plugins a second binary (`host-local`, `static`, `dhcp`) answers it. Here it
is part of your plugin, chosen by `ipam.type`.

The simplest is a list given in the config:

```json
"ipam": {"type": "static",
         "addresses": [{"address": "10.217.5.10/24", "gateway": "10.217.5.1"}]}
```

The address goes on the container's interface, prefix and all — the prefix
is what gives the container its on-link route to the rest of the subnet. It
is reported in the result:

```json
"ips": [{"address": "10.217.5.10/24", "gateway": "10.217.5.1", "interface": 1}]
```

`interface` is an **index into `interfaces`**, pointing at the container's
end.

Decide addresses **before** creating anything: a bad address (error 7) must
fail ADD with nothing left behind.

## Go APIs

- `net.ParseCIDR` to validate; `netlink.ParseAddr` to apply.
- `h.AddrAdd(link, addr)` on the handle inside the namespace.

## Further reading

- [CNI spec: IP allocation](https://www.cni.dev/docs/spec/#ip-allocation)
- [The static IPAM plugin](https://www.cni.dev/plugins/current/ipam/static/)
