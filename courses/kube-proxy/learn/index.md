---
title: Build your own kube-proxy — primer
concepts: [services, endpointslices, nftables, dnat, conntrack]
---

## What kube-proxy actually does

A Service's cluster IP is not on any machine. No interface holds it, nothing
listens on it, and a packet sent to it would wander out of the default route
and die. It works because every node rewrites that packet before it leaves:
the destination `10.96.12.34:80` becomes `10.244.2.7:8080`, the address of
one pod behind the Service, and the kernel's connection tracking remembers
the rewrite so the replies are rewritten back. kube-proxy is the program that
keeps those rewrite rules in step with the cluster.

It never carries a packet itself. It watches two kinds of object — Services,
which say what addresses and ports exist, and EndpointSlices, which say which
pods are behind each one right now — and turns them into kernel rules. The
kernel does the forwarding; the proxy only writes the rules down and keeps
rewriting them as pods come and go.

## Your proxy and the real one

The cluster already runs kube-proxy on every node, and yours runs beside it on
one worker, `byok8s-worker2`. Two programs writing rules for the same
addresses would fight, so the work is split by a label Kubernetes defines for
exactly this: a Service labelled

```
service.kubernetes.io/service-proxy-name: byok8s
```

is skipped by the real kube-proxy and is yours. Every Service the tester
creates for you carries it. The EndpointSlice controller copies a Service's
labels onto its slices, so the same label finds those too.

Your rules go in an nftables table of your own, `table ip byok8s`. The real
kube-proxy's rules live in the iptables tables beside it, and other programs
on the node have tables of their own. Never flush the ruleset, and never
touch a table you did not create.

## How it runs, and how it is graded

The program is built for Linux and copied into the worker, where it runs as
root in the node's own network namespace, so `nft` and `conntrack` are on its
`PATH`. `KUBECONFIG` points at the cluster, and the node's hostname is its node
name. It runs until it is sent `SIGTERM`.

The verdict is traffic. A stage starts your program, creates pods and
Services, and sends requests from a pod — usually one on `byok8s-worker2`,
since that is the only node whose rules you write. Every backend answers with
its own pod name, the address the request came from and the port it arrived
on, so a stage can tell which pod you sent each request to, and whether the
client's address survived the trip.

## The shape of the course

One program, grown stage by stage: find your Services and their endpoints,
claim a table, make a cluster IP reach one pod and then many, follow pods as
they come and go, refuse quickly when nothing is there. Then the node itself:
node ports, masquerading so replies find their way back, session affinity,
endpoints that are terminating, the two traffic policies, load balancer
addresses and the health check a load balancer asks. Then the parts that make
it production-shaped: UDP and stale conntrack entries, changing only the rules
that changed, cleaning up on exit — and finally, side by side with the real
kube-proxy, the same answers for the same Services.

## Two things worth knowing before you start

NAT happens once per connection. The rules see only the first packet; the
kernel's conntrack table applies the same rewrite to the rest of the
connection, in both directions. So a rule change never moves a connection that
is already open — and a stale conntrack entry can keep sending traffic to a
pod that is gone.

The rules are the state. If your program dies, the kernel keeps forwarding
exactly as it was told until someone changes the table. A proxy has to start
from whatever it finds, and has to leave the node clean when it is told to
stop.
