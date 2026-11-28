---
title: Build your own CNI plugin — primer
concepts: [CNI, network namespace, veth, bridge, IPAM, overlay]
---

## What a CNI plugin actually does

A pod's network is not something Kubernetes builds. The kubelet asks the
container runtime for a sandbox; the runtime makes an empty network namespace
for it, holding nothing but a loopback that is down; and then it runs a
**CNI plugin** to fill that namespace in. Whatever the plugin leaves there —
an interface, an address, routes — is the pod's network.

The contract is tiny and deliberately dumb. The runtime runs a binary with
the request in environment variables and the network config on stdin:

```
CNI_COMMAND=ADD           ADD, DEL, CHECK or VERSION
CNI_CONTAINERID=…         which container
CNI_NETNS=/run/netns/…    the namespace to configure
CNI_IFNAME=eth0           what to call the interface inside it
CNI_ARGS=K=V;K=V          extra, e.g. K8S_POD_NAMESPACE
CNI_PATH=/opt/cni/bin     where other plugins live
```

and reads a JSON result, or a JSON error, from stdout. No daemon, no API: a
process per call. Flannel, Calico, Cilium and the reference `bridge` plugin
all start here.

## Where your program runs

Namespaces, links and routes are Linux, so your plugin is built with
`GOOS=linux` and copied into the first two kind workers, where it runs as
root. The tester plays the container runtime: it makes namespaces with
`ip netns add`, invokes your plugin the way containerd would, and then judges
what you built by looking — `ip link`, `ip route`, `nft`, `tc` — and by
sending TCP traffic through it. In the last stage the real containerd calls
your plugin for a real pod.

Netlink by hand is a chore, so the reference uses
[`github.com/vishvananda/netlink`](https://github.com/vishvananda/netlink)
and its sibling `netns` (`go get` them into `app/`). Anything that drives the
kernel is fine, including shelling out to `ip`.

## The shape of the course

One binary, two roles. Called with `CNI_COMMAND` set it is a plugin, and its
config's `type` says which: `byok8s` builds the container's network,
`byok8s-portmap` and `byok8s-bandwidth` are chained after it. Called as
`app conflist --cache DIR CMD FILE` it plays the runtime for a plugin list.

Stages 1–2 are the protocol; 3–9 build a single container's network piece by
piece; 10–12 finish the lifecycle; 13–16 are chaining; 17–19 go across nodes
and add policy; 20 hands the plugin to the kubelet.

## Two things worth knowing before you start

Every stage's addresses come from `10.217.N.0/24` for stage N, and every
namespace and link name the tester hands you starts with `bk8s`, so a stage
can always clean up after you.

DEL is the call that matters most and is graded least by accident. Runtimes
retry it until it succeeds, call it for containers whose ADD failed halfway,
and call it after the namespace is long gone. A plugin that leaks an address
on any of those paths runs a node out of IPs in a week.
