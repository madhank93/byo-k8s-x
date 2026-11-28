---
title: Isolate namespaces
concepts: [network policy, CNI_ARGS, nftables bridge family, sets, verdict maps]
---

## Core concept

Kubernetes NetworkPolicy is enforced by the CNI plugin, not by Kubernetes.
This stage builds the simplest useful policy: a container accepts traffic
only from containers in **its own Kubernetes namespace**, plus the node.

The runtime tells the plugin who the pod is through `CNI_ARGS`:

```
CNI_ARGS=IgnoreUnknown=1;K8S_POD_NAMESPACE=blue;K8S_POD_NAME=a
```

Containers on one bridge talk at L2, so filter where frames are switched:
the **bridge** family's `forward` hook sees every frame between two ports of
a bridge. Traffic from the node is routed out of the bridge, not forwarded
through it, so it never meets that hook and stays allowed.

One workable layout:

```
table bridge byok8s-cni-netpol {
  map dst { type ipv4_addr : verdict; }    container → jump its namespace
  set ns-blue { type ipv4_addr; }           members of blue
  chain fwd { type filter hook forward priority 0; ip daddr vmap @dst }
  chain ns-blue { ip saddr != @ns-blue drop }
}
```

ADD adds the container's address to its namespace's set and to `dst`. DEL
must take it out again: the next container to lease that address may be in
another namespace, and must not inherit the old one's peers.

## Go APIs

- `strings.Split(os.Getenv("CNI_ARGS"), ";")`, `strings.Cut(kv, "=")`.
- `nft -f -`: `add` of a table, chain, set or map is idempotent; `flush chain`
  then `add rule` keeps a chain's rules from piling up.

## Further reading

- [Network policies](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
- [nftables: bridge filtering](https://wiki.nftables.org/wiki-nftables/index.php/Bridge_filtering)
- [nftables: verdict maps](https://wiki.nftables.org/wiki-nftables/index.php/Verdict_Maps_(vmaps))
