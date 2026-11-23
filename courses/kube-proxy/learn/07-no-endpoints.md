---
title: Refuse, do not hang
concepts: [reject, filter chain, forward hook, output hook]
---

## Core concept

A Service with no ready pods still has a cluster IP, and clients still
connect to it. If your rules simply leave it out of the map, the packet is
never rewritten: the node routes `10.96.12.34` like any other address, out of
the default route, where nothing answers. The client waits for its whole
connect timeout — seconds to minutes — before failing. Every caller of that
Service hangs.

The kind thing is to fail fast: answer the connection attempt with a
**reject**, an ICMP "port unreachable" (or a TCP reset), so the client gets
*connection refused* at once and can retry, fall back, or report.

`reject` is not allowed in a nat chain; it belongs to a **filter** chain. An
un-rewritten packet from a pod passes the `forward` hook on its way out of the
node, and one the node sends itself passes `output` — after the nat chains
have had their chance, so only Services that were not DNATed are left to
match. A second map, keyed like the first, sends them to a chain that
rejects:

```
map no-endpoint-services {
	type ipv4_addr . inet_proto . inet_service : verdict
	elements = { 10.96.12.34 . tcp . 80 : goto reject-chain }
}
chain filter-forward {
	type filter hook forward priority filter; policy accept;
	jump no-endpoints
}
chain filter-output {
	type filter hook output priority filter; policy accept;
	jump no-endpoints
}
chain no-endpoints {
	ip daddr . meta l4proto . th dport vmap @no-endpoint-services
}
chain reject-chain {
	reject
}
```

A service port is in exactly one of the two maps: `service-ips` while it has
endpoints, `no-endpoint-services` while it has none.

The tester connects to an empty Service from a pod and from the node, adds a
pod and expects it reached, then deletes it and expects refusals again.

## Go APIs

Nothing new: two more chains and a second `add element` per empty service
port.

## Hints

<details><summary>Nudge</summary>

At which hook does a pod's packet to an address the node does not own pass,
once routing has decided to send it out again?
</details>

<details><summary>Approach</summary>

Add the map and chains above to the table. In the element loop, a service
port with no endpoints gets an element in `no-endpoint-services` instead of
`service-ips`.
</details>

## Further reading

- [nftables wiki: Rejecting traffic](https://wiki.nftables.org/wiki-nftables/index.php/Rejecting_traffic)
- [kube-proxy: Services with no endpoints](https://kubernetes.io/docs/reference/networking/virtual-ips/#traffic-to-services-with-no-endpoints)
