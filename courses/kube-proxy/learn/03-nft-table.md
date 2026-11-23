---
title: A table of your own
concepts: [nftables, table, base chain, hook, priority, atomic replace]
---

## Core concept

The kernel's packet filter, netfilter, runs code at fixed points on a packet's
path — **hooks**. A packet arriving from a pod passes `prerouting` before the
kernel decides where it goes; a packet the node itself sends passes `output`.
A proxy rewrites destinations, so it needs to run at both, and early: before
routing, so the rewritten address is the one that gets routed.

nftables organises rules into **tables**, each holding **chains**. A chain with
a `type`, a `hook` and a `priority` is a **base chain**: the kernel calls it.
Any other chain runs only when a rule jumps to it. Every table is independent,
which is what lets your rules live beside the real kube-proxy's, kindnet's and
Docker's without any of you knowing about the others:

```
table ip byok8s {
	chain nat-prerouting {
		type nat hook prerouting priority dstnat; policy accept;
		jump services
	}
	chain nat-output {
		type nat hook output priority -100; policy accept;
		jump services
	}
	chain services {
	}
}
```

`type nat` matters: only nat chains may rewrite addresses, and they see only
the first packet of each connection — conntrack carries the rewrite to the
rest. Priority `-100` (`dstnat`) is where destination NAT conventionally runs.

Write the whole table on every sync, and write it **atomically**. `nft -f`
applies a file as one transaction, so this replaces the table in one step,
whether or not it existed:

```
add table ip byok8s
delete table ip byok8s
table ip byok8s { … }
```

That also settles what to do about a table a crashed earlier run left behind:
it is replaced, not merged with. And it is the only table you ever touch.
`nft flush ruleset` would take down every other program's rules on the node.

The tester plants a stale `table ip byok8s` and someone else's table before
your program starts, then checks yours has replaced the first and left the
second alone.

## Go APIs

- `exec.Command("nft", "-f", "-")` with `cmd.Stdin = strings.NewReader(rules)`
  — `nft` is on the node's `PATH`, and the program runs as root.
- `cmd.CombinedOutput()` — nft reports syntax errors with a caret under the
  offending token; print them.
- `nft list table ip byok8s` and `nft -j list table ip byok8s` in the node show
  what you wrote: `docker exec byok8s-worker2 nft list table ip byok8s`.

## Hints

<details><summary>Nudge</summary>

Statements in nft's file format end at a newline or a semicolon, and some
priority names only make sense at some hooks. If `nft` rejects your file, its
error points at the token.
</details>

<details><summary>Approach</summary>

At the end of every sync, build the table as text — add, delete, then the
table body — and pipe it to `nft -f -`. The `services` chain is empty for now;
the next stage fills it.
</details>

## Further reading

- [nftables wiki: Quick reference](https://wiki.nftables.org/wiki-nftables/index.php/Quick_reference-nftables_in_10_minutes)
- [nftables wiki: Netfilter hooks](https://wiki.nftables.org/wiki-nftables/index.php/Netfilter_hooks)
- [nft(8)](https://www.netfilter.org/projects/nftables/manpage.html)
