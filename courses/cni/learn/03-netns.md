---
title: Enter the container's network namespace
concepts: [network namespace, setns, netlink handle, loopback]
---

## Core concept

A **network namespace** is a complete copy of the network stack: its own
interfaces, addresses, routes, firewall and sysctls. A new one holds a single
interface, `lo`, and it is down. `CNI_NETNS` is a path to that namespace — a
file under `/run/netns` or `/proc/PID/ns/net` — and the plugin's job is to
make it useful.

There are two ways to act inside it:

- **Move a thread in**: `setns(fd, CLONE_NEWNET)`. In Go that means
  `runtime.LockOSThread()` first, because the scheduler moves goroutines
  between threads, and moving back before unlocking.
- **Open a netlink socket in it**: a netlink socket belongs to the namespace
  it was created in, so a handle created there configures that namespace
  from any thread.

The second is safer and is what the reference uses for everything except
sysctls.

A result is printed even when there is little to say yet:
`{"cniVersion":"1.0.0"}`, in the config's version.

This stage: open `CNI_NETNS`, bring its `lo` up, print a result. A path that
names no namespace is an error.

## Go APIs

- `netns.GetFromPath(path)` → a handle (an fd).
- `netlink.NewHandleAt(ns)`, then `h.LinkByName("lo")`, `h.LinkSetUp(l)`.

## Hints

<details><summary>Nudge</summary>

`ip -n NS link` shows what the tester sees. If `lo` is still down, your
handle is pointing at the node's namespace.
</details>

## Further reading

- [network_namespaces(7)](https://man7.org/linux/man-pages/man7/network_namespaces.7.html)
- [setns(2)](https://man7.org/linux/man-pages/man2/setns.2.html)
