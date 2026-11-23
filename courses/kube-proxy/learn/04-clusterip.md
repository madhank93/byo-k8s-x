---
title: An address with nothing behind it
concepts: [clusterIP, dnat, verdict map, conntrack, targetPort]
---

## Core concept

Now the rules do something. A packet from a pod to `10.96.12.34:80` arrives at
the node, passes `prerouting`, and your nat chain rewrites its destination to
a pod's address and port — destination NAT. The kernel then routes the packet
by its *new* destination, across the cluster network to whichever node that
pod is on. Conntrack records the translation, so the reply, which comes back
from `10.244.2.7:8080`, is rewritten to look as if it came from
`10.96.12.34:80` before the client sees it.

The port changes too. A Service port (`port: 80`) forwards to a **target
port** on the pod (`targetPort: 8080`). For now the target is a number on the
Service; a later stage handles the name form.

One chain per service port keeps the rules readable, and one lookup finds the
chain. nftables can key a **verdict map** on several fields at once, so a
single rule in `services` dispatches every packet:

```
map service-ips {
	type ipv4_addr . inet_proto . inet_service : verdict
	elements = { 10.96.12.34 . tcp . 80 : goto svc-ns/web/tcp/80 }
}
chain services {
	ip daddr . meta l4proto . th dport vmap @service-ips
}
chain svc-ns/web/tcp/80 {
	meta l4proto tcp dnat to 10.244.2.7:8080
}
```

A packet whose destination is not in the map falls through, untouched, to
whatever the real kube-proxy or anyone else has to say about it. A lookup
costs the same for one Service or ten thousand, which is the whole reason
kube-proxy's nftables mode exists.

`dnat to address:port` must know the transport protocol; nft insists on a
protocol match before a port mapping. For this stage, send every connection
to the first ready endpoint.

The tester sends requests from a pod on `byok8s-worker2` and from a
host-network pod — the node itself — to a Service whose one pod is on another
worker.

## Go APIs

- `service.Spec.Ports` — each has `Port`, `Protocol` and `TargetPort`, an
  `intstr.IntOrString`; `TargetPort.IntValue()` for the number.
- `add element ip byok8s service-ips { … }` can follow the table in the same
  `nft -f` file, inside the same transaction.

## Hints

<details><summary>Nudge</summary>

A rule in `nat-prerouting` and `nat-output` already jumps to `services`. What
must `services` hold for a packet to `clusterIP:port` to reach a chain that
rewrites it?
</details>

<details><summary>Approach</summary>

Collect one record per Service port with its endpoints. Render a chain per
record with a single `dnat`, then one map element per record pointing at it.
Skip records with no endpoints for now.
</details>

## Further reading

- [Virtual IPs and service proxies](https://kubernetes.io/docs/reference/networking/virtual-ips/)
- [nftables wiki: Performing NAT](https://wiki.nftables.org/wiki-nftables/index.php/Performing_Network_Address_Translation_(NAT))
- [nftables wiki: Verdict maps](https://wiki.nftables.org/wiki-nftables/index.php/Verdict_Maps_(vmaps))
