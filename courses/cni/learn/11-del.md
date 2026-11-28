---
title: Tear it down
concepts: [DEL, idempotency, address release, missing netns]
---

## Core concept

DEL undoes ADD, and the spec asks one thing above all: **it must succeed
however little is left**. The runtime calls DEL

- after a failed ADD, to clean up whatever half of it happened;
- more than once, because it retries until DEL succeeds;
- with the netns already gone, or with no `CNI_NETNS` at all, when the
  sandbox died first;
- for containers the plugin never saw.

A DEL that fails on any of these keeps the pod stuck in `Terminating` and the
address leased forever.

So each step tolerates "not found": delete the interface in the namespace
if both still exist (deleting one end of a veth deletes the pair), delete
the host end by its derived name if it survived, and release every lease the
container (`CNI_CONTAINERID`, `CNI_IFNAME`) holds — which needs nothing but
the config, so it works with the netns gone.

DEL prints nothing on success.

## Go APIs

- `h.LinkDel(link)`; `netlink.LinkByName` returns `LinkNotFoundError`.
- `os.ReadDir(dataDir)` to find the container's leases.

## Hints

<details><summary>Nudge</summary>

Release the address even when the netns cannot be opened.
</details>

## Further reading

- [CNI spec: DEL](https://www.cni.dev/docs/spec/#del-remove-container-from-network-or-un-apply-modifications)
