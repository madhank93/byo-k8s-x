---
title: Tune the container's network stack
concepts: [sysctl, per-netns sysctls, setns, LockOSThread]
---

## Core concept

Most of `/proc/sys/net` is **per network namespace**: a container can have
its own `ip_unprivileged_port_start`, its own `ping_group_range`, its own
TCP tuning. Kubernetes exposes this as pod `securityContext.sysctls`, and the
runtime passes them to the plugin:

```json
"sysctl": {"net.ipv4.ip_unprivileged_port_start": "80"}
```

`net.ipv4.ping_group_range` becomes `/proc/sys/net/ipv4/ping_group_range`.

The catch is which namespace you write to. `/proc/sys/net` shows the
namespace of **the thread that opens it**, and there is no netlink call for
sysctls, so this is the one place the plugin has to move a thread: lock the
goroutine to its OS thread, `setns` into the container, write, `setns` back.
Forget the lock and the Go scheduler can run the write on another thread —
in the node's namespace — changing it for every pod on the node.

Accept only `net.*` keys. Everything else (`kernel.*`, `vm.*`) is global to
the node, and a plugin is not the place to change it; refuse the config
before creating anything.

## Go APIs

- `runtime.LockOSThread()` / `UnlockOSThread()`.
- `netns.Get()` for the current namespace, `netns.Set(ns)` to switch.
- `os.WriteFile("/proc/sys/...", value, 0o644)`.

## Hints

<details><summary>Nudge</summary>

If the container still reads the default but the node changed, your write
ran in the wrong namespace.
</details>

## Further reading

- [Using sysctls in a Kubernetes cluster](https://kubernetes.io/docs/tasks/administer-cluster/sysctl-cluster/)
- [The tuning plugin](https://www.cni.dev/plugins/current/meta/tuning/)
