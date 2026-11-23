---
title: More than one backend
concepts: [load balancing, numgen, verdict map, conntrack]
---

## Core concept

A Service with three pods should share its connections between them. The
decision is made once per connection, on its first packet, because that is
the only packet a nat chain sees; conntrack then keeps every later packet of
that connection on the same pod. So "load balancing" here means: pick an
endpoint at random for each new connection.

nftables has a random number generator as an expression, `numgen random mod
N`, which yields 0 to N-1. Feed it to an anonymous verdict map and each value
jumps to a chain for one endpoint:

```
chain svc-ns/web/tcp/80 {
	numgen random mod 3 vmap { 0 : goto ep-ns/web/tcp/80/10.244.1.5,
	                           1 : goto ep-ns/web/tcp/80/10.244.2.7,
	                           2 : goto ep-ns/web/tcp/80/10.244.4.9 }
}
chain ep-ns/web/tcp/80/10.244.1.5 {
	meta l4proto tcp dnat to 10.244.1.5:8080
}
```

One chain per endpoint looks like more than this stage needs, and it is: it
is where the per-endpoint work of later stages goes — marking a packet for
masquerade when a pod talks to itself, remembering a client for session
affinity.

This is not how the iptables mode does it. iptables has no map, so it walks a
list of rules with falling probabilities — 1/3, then 1/2, then 1 — and every
packet of a Service with many endpoints pays for the walk. A map lookup costs
the same however many there are.

The set of endpoints is never still. A pod that is running but not ready is
in the slice marked not ready and must get nothing; when it turns ready it
joins; a deleted pod leaves. Your sync already rereads the slices on every
change, so every one of those is just a new table — as long as the table
really is rebuilt from the current slices each time.

The tester sends 60 new connections to a Service with three pods, one on each
worker, and expects each pod to answer at least six. Then, on a second
Service, it holds a pod serving but not ready, lets it in, deletes another,
and adds a third, checking the traffic follows each change.

## Go APIs

- `strings.Join` over `"%d : goto %s"` entries builds the map.
- `goto` rather than `jump`: nothing needs to come back to the service chain
  once an endpoint is chosen.

## Hints

<details><summary>Nudge</summary>

`numgen random mod N` is an expression; what turns a number into a chain to
run?
</details>

<details><summary>Approach</summary>

Give each endpoint its own chain holding the `dnat`, and make the service
chain a single `numgen random mod N vmap { … }` rule over them.
</details>

## Further reading

- [nftables wiki: Load balancing](https://wiki.nftables.org/wiki-nftables/index.php/Load_balancing)
- [KEP-3866: the nftables kube-proxy backend](https://github.com/kubernetes/enhancements/tree/master/keps/sig-network/3866-nftables-proxy)
