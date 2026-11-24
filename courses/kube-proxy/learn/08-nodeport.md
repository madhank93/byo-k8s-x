---
title: A port on every node
concepts: [nodePort, fib, local address, external traffic]
---

## Core concept

A cluster IP only means something inside the cluster. To reach a Service from
outside, a `NodePort` Service reserves a port — `30000-32767` by default — on
**every node**: a connection to any node's address on that port reaches the
Service. The API server allocates the number and writes it into
`spec.ports[].nodePort`; the proxy on each node makes it work.

"Any node's address" is the subtle part. A node has several — its main
interface, maybe others, loopback — and the rule should not hard-code one.
nftables can ask the routing table whether the destination is one of this
host's own addresses:

```
fib daddr type local
```

So one more map, keyed only on protocol and port, catches node-port traffic,
and it can send it to the very same service chain the cluster IP uses:

```
map service-nodeports {
	type inet_proto . inet_service : verdict
	elements = { tcp . 31234 : goto svc-ns/web/tcp/80 }
}
chain services {
	ip daddr . meta l4proto . th dport vmap @service-ips
	fib daddr type local meta l4proto . th dport vmap @service-nodeports
}
```

Order matters a little: the cluster IP lookup comes first, and only a packet
it did not claim is tested as node-port traffic.

A NodePort Service is still a ClusterIP Service; the cluster IP must keep
working.

The tester puts the only pod on `byok8s-worker2`, the node your proxy runs on,
and connects to that node's address and port from a pod on another worker.
With the pod local to the proxy, the reply passes back through the node that
rewrote the request, so the rewrite can be undone. The next stage is what
happens when it does not.

## Go APIs

- `servicePort.NodePort` — zero unless the Service is `NodePort` or
  `LoadBalancer`.

## Hints

<details><summary>Nudge</summary>

How does a rule know a packet is addressed to *this* node, without knowing the
node's address?
</details>

<details><summary>Approach</summary>

Add the map and the `fib` rule to `services`, and for every service port with
endpoints and a non-zero node port, an element pointing at its service chain.
</details>

## Further reading

- [Service type NodePort](https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport)
- [nftables wiki: Matching routing information (fib)](https://wiki.nftables.org/wiki-nftables/index.php/Matching_routing_information)
