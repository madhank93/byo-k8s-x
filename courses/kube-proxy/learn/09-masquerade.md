---
title: Replies that find their way back
concepts: [masquerade, SNAT, packet marks, hairpin, postrouting]
---

## Core concept

DNAT rewrites a request's destination; conntrack remembers, and undoes the
rewrite on the reply — but only if the reply passes back through the node that
made it. Two cases break that.

**A node port whose pod is on another node.** A client on node A connects to
node B's address; B rewrites the destination to a pod on node C. The pod
replies to the client's own address, so the reply goes C → A directly, from
an address A never talked to, and A drops it. Rewriting the *source* to B as
well — masquerading — makes the pod reply to B, which undoes both rewrites.

**A pod reaching its own Service and being picked.** The request leaves the
pod, is rewritten to come back to the same pod, and the pod answers itself
directly, never through the node's rewrite. This is the "hairpin" case, and
masquerading fixes it the same way.

Masquerading everything would work, and would throw away the client's address
for every backend. kube-proxy does it only where it is needed, by **marking**
packets as they are routed and masquerading marked packets on the way out:

```
chain mark-for-masquerade {
	meta mark set meta mark | 0x10000
}
chain nat-postrouting {
	type nat hook postrouting priority srcnat; policy accept;
	meta mark & 0x10000 != 0 masquerade
}
```

Node-port traffic goes through a chain of its own that marks, then carries on
to the usual service chain:

```
chain ext-ns/web/tcp/80 {
	jump mark-for-masquerade
	goto svc-ns/web/tcp/80
}
```

And each endpoint chain marks traffic from the endpoint itself:

```
chain sep-ns/web/tcp/80/10.244.1.5 {
	ip saddr 10.244.1.5 jump mark-for-masquerade
	meta l4proto tcp dnat to 10.244.1.5:8080
}
```

The tester checks all three: a node port reaching a pod on another node sees
the request come from the proxy node's address, a pod reaching itself through
its Service gets an answer, and a pod reaching a cluster IP still shows its own
address to the backend.

The mark bit is yours to choose. kube-proxy uses `0x4000`; picking another
keeps your decisions apart from its.

## Hints

<details><summary>Nudge</summary>

Which packets need their source rewritten, and where in the ruleset do you
still know which ones they are?
</details>

<details><summary>Approach</summary>

Add the postrouting chain and the marking chain; point node-port map elements
at a new per-port chain that marks and jumps on; add an `ip saddr <endpoint>`
mark rule to each endpoint chain, ahead of the DNAT.
</details>

## Further reading

- [nftables wiki: Performing NAT](https://wiki.nftables.org/wiki-nftables/index.php/Performing_Network_Address_Translation_(NAT))
- [Kubernetes: Source IP for Services with type NodePort](https://kubernetes.io/docs/tutorials/services/source-ip/#source-ip-for-services-with-type-nodeport)
