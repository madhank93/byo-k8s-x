---
title: Install the container's routes
concepts: [default route, gateway, on-link, result routes]
---

## Core concept

An address with a prefix gives the container a route to its own subnet and
nothing else. To reach anything beyond, it needs routes, and IPAM supplies
them:

```json
"routes": [{"dst": "0.0.0.0/0"},
           {"dst": "10.99.0.0/16", "gw": "10.217.7.254"}]
```

A route with no `gw` goes through the address's gateway. The gateway must
be on-link — inside the container's subnet — or the kernel refuses the route,
which is why the address goes on first.

Routes are added **inside the namespace**, on the container's interface, and
listed in the result's `routes`.

## Go APIs

- `h.RouteAdd(&netlink.Route{LinkIndex: ctr.Attrs().Index, Dst: dst, Gw: gw})`.

## Hints

<details><summary>Nudge</summary>

`ip -n NS route` is the routing table the tester reads.
</details>

## Further reading

- [ip-route(8)](https://man7.org/linux/man-pages/man8/ip-route.8.html)
