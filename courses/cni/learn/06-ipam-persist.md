---
title: Allocate from a pool
concepts: [host-local IPAM, leases on disk, flock, concurrent plugins]
---

## Core concept

Static addresses do not scale past a handful of containers. A pool hands out
addresses from a subnet, and since every plugin call is a fresh process, the
pool's state has to live **on disk**. The layout the reference `host-local`
plugin uses works well:

```
<dataDir>/lock           an flock everyone takes first
<dataDir>/10.217.6.2     a lease: the file's content is its owner
<dataDir>/10.217.6.3
```

A lease is free when its file does not exist. Allocation is: take the lock,
skip the network address, the gateway (default: the first host) and the
broadcast address, pick the lowest free one, write its file, release.

The lock is not optional. Runtimes start pods in parallel, so several copies
of your plugin run at once, and two of them that read the directory before
either writes will hand out the same address. This stage holds
`<dataDir>/lock` itself and checks your ADD waits for it.

Make allocation **idempotent**: if this container (`CNI_CONTAINERID` and
`CNI_IFNAME`) already owns a lease, return it. An empty pool is an error.

```json
"ipam": {"type": "pool", "subnet": "10.217.6.0/29", "dataDir": "…"}
```

## Go APIs

- `syscall.Flock(fd, syscall.LOCK_EX)`; closing the file releases it.
- `os.ReadFile`, `os.WriteFile`, `errors.Is(err, os.ErrNotExist)`.

## Hints

<details><summary>Nudge</summary>

In a /29 the usable addresses are .2–.6: .0 is the network, .1 the
gateway, .7 the broadcast.
</details>

<details><summary>Approach</summary>

An IPv4 address is four bytes; incrementing the last one with carry walks
the subnet. Stop at the broadcast address.
</details>

## Further reading

- [host-local IPAM](https://www.cni.dev/plugins/current/ipam/host-local/)
- [flock(2)](https://man7.org/linux/man-pages/man2/flock.2.html)
