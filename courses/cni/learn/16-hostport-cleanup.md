---
title: Remove only your own rules
concepts: [rule ownership, nft comments, handles, DEL without prevResult]
---

## Core concept

Portmap's DEL has to remove one container's forwarding from a table every
other container on the node shares. Flushing the table breaks everyone;
guessing from the ports breaks whoever reused one. Every rule needs an
**owner** it can be found by.

nftables gives each rule a **comment** and a **handle**. Write the
container ID as the comment on ADD; on DEL, list the table with handles,
delete the rules whose comment is this container's, and leave the rest.

```
nft -j -a list table ip byok8s-cni-portmap
delete rule ip byok8s-cni-portmap pre handle 7
```

DEL must not need `prevResult`: the runtime may have lost it. Nor may it
fail when there is nothing to delete, or when it runs twice.

## Go APIs

- `nft -j` prints JSON: `{"nftables":[{"rule":{"chain":…,"handle":…,"comment":…}}]}`.

## Further reading

- [nftables: simple rule management](https://wiki.nftables.org/wiki-nftables/index.php/Simple_rule_management)
