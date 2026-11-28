---
title: Plumb a veth pair
concepts: [veth, interface naming, result interfaces, sandbox]
---

## Core concept

A **veth pair** is a virtual cable: two interfaces, and whatever goes in one
comes out the other. Put one end in the container's namespace and keep the
other on the node, and the container has a way out.

Two details make it robust:

- **Create the peer straight inside the container.** Netlink lets the pair
  be created with its peer already in another namespace. Creating both on the
  node and moving one means `CNI_IFNAME` (`eth0`) has to be free on the node,
  which it is not.
- **Derive the host end's name** from the container ID and interface, e.g. a
  hash. Names are limited to 15 characters, must be unique per node, and DEL
  must be able to find the interface again without being told.

Both ends must be **up**. The result now lists them:

```json
"interfaces": [
  {"name": "veth1a2b3c4d5e6", "mac": "…"},
  {"name": "eth0", "mac": "…", "sandbox": "/run/netns/…"}
]
```

`sandbox` marks the interface that is inside the container.

This stage: make the pair, name the inside end `CNI_IFNAME`, report both.

## Go APIs

- `netlink.Veth{LinkAttrs: …{Name: host}, PeerName: ifname, PeerNamespace: netlink.NsFd(ns)}`.
- `netlink.LinkAdd`, `netlink.LinkByName`, `LinkSetUp`.

## Hints

<details><summary>Nudge</summary>

Two containers with the same `CNI_IFNAME` must not collide on the node.
</details>

## Further reading

- [veth(4)](https://man7.org/linux/man-pages/man4/veth.4.html)
- [CNI spec: result interfaces](https://www.cni.dev/docs/spec/#add-success)
