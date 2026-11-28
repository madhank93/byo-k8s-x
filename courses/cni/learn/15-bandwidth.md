---
title: Shape a container's traffic
concepts: [traffic shaping, tbf, qdisc, ingress via host veth]
---

## Core concept

The `kubernetes.io/ingress-bandwidth` annotation becomes a chained plugin's
`runtimeConfig`:

```json
"runtimeConfig": {"bandwidth": {"ingressRate": 8000000, "ingressBurst": 800000}}
```

Rates are bits per second, bursts bits.

Linux shapes what an interface **sends**, with a queueing discipline. What
the container **receives** is what the host end of its veth sends, so
ingress shaping goes on the host end, found in `prevResult.interfaces` as
the one with no `sandbox`. A **token bucket filter** fits: tokens arrive at
the rate, a packet leaves only with tokens to spend, and the bucket's size
is the burst.

```
tc qdisc replace dev vethXXXX root tbf rate 8000000bit burst 100000 latency 25ms
```

`tc` reports the rate in bytes per second. Without a `prevResult` there is
no way to know which interface to shape: that is an error.

## Go APIs

- `exec.Command("tc", …)`, or `netlink.QdiscReplace(&netlink.Tbf{…})`.

## Further reading

- [The bandwidth plugin](https://www.cni.dev/plugins/current/meta/bandwidth/)
- [tc-tbf(8)](https://man7.org/linux/man-pages/man8/tc-tbf.8.html)
