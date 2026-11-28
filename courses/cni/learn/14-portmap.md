---
title: Forward a host port
concepts: [hostPort, DNAT, nftables, prerouting, output hook]
---

## Core concept

A pod with `hostPort: 8080` is reachable on the node's own address. That is
not the main plugin's job; it is a **chained** plugin that runs after it,
reads the container's address from `prevResult`, and installs forwarding.
The runtime passes the mappings as `runtimeConfig`:

```json
{"type": "byok8s-portmap", "prevResult": {…},
 "runtimeConfig": {"portMappings": [
   {"hostPort": 40123, "containerPort": 8080, "protocol": "tcp"}]}}
```

Forwarding is **DNAT**: rewrite the destination of packets for
`NODE_IP:hostPort` to `CONTAINER_IP:containerPort`, and let connection
tracking rewrite the replies back. Packets arrive two ways, and each needs a
rule:

- from **other machines**, through the `prerouting` hook;
- from the **node itself**, through the `output` hook — locally generated
  packets never pass prerouting.

`fib daddr type local` matches "addressed to this node" whatever its
addresses are. Keep the rules in a table of your own:

```
table ip byok8s-cni-portmap {
  chain pre { type nat hook prerouting priority -100; }
  chain out { type nat hook output priority -100; }
}
```

A chained plugin prints the `prevResult` it was given, so the next plugin
and the runtime still see the container's network.

## Go APIs

- `exec.Command("nft", "-f", "-")` with a script on stdin — or
  `github.com/google/nftables`.

## Further reading

- [The portmap plugin](https://www.cni.dev/plugins/current/meta/portmap/)
- [nftables NAT](https://wiki.nftables.org/wiki-nftables/index.php/Performing_Network_Address_Translation_(NAT))
