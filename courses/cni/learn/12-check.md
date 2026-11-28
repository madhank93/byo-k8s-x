---
title: Verify a container's network
concepts: [CHECK, prevResult, drift]
---

## Core concept

CHECK asks: is the container's network still what ADD built? The runtime
passes the result of the ADD back as `prevResult` in the config, and the
plugin compares it with the namespace as it is now:

- the interface `CNI_IFNAME` still exists;
- it still holds every address in `prevResult.ips`;
- every route in `prevResult.routes` is still there.

Any difference is an error. So is a CHECK with no `prevResult`: there is
nothing to check against.

CHECK changes nothing. It is how a runtime notices that something on the
node — a careless script, another agent — has broken a running pod.

## Go APIs

- `h.AddrList(link, netlink.FAMILY_V4)`, `h.RouteList(link, netlink.FAMILY_V4)`.
- A default route's `Dst` may be `nil` or `0.0.0.0/0`, depending on version.

## Further reading

- [CNI spec: CHECK](https://www.cni.dev/docs/spec/#check-check-containers-networking-is-as-expected)
