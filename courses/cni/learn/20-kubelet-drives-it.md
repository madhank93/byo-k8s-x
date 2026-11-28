---
title: Let the kubelet drive it
concepts: [/etc/cni/net.d, containerd CRI, pod sandbox, RunPodSandbox]
---

## Core concept

Everything so far, the tester invoked. In a cluster it is containerd: when
the kubelet asks for a pod sandbox, containerd makes a netns, reads the
**first** config in `/etc/cni/net.d` by file name, finds each plugin in
`/opt/cni/bin`, and runs the list with `CNI_ARGS` naming the pod. The pod IP
the kubelet reports is the `ips[0]` of your result. When the pod is
deleted, containerd runs DEL.

This stage installs your binary as `/opt/cni/bin/byok8s` and a conflist as
`/etc/cni/net.d/00-byok8s.conflist` on the first worker. `00-` sorts before
kindnet's `10-kindnet.conflist`, so containerd picks it up. It then runs a
pod there and checks:

- the pod runs, with an IP from the stage's subnet;
- its host veth is attached to the bridge;
- after the pod is deleted, DEL has detached it.

A failed ADD surfaces as a pod event:
`Failed to create pod sandbox: … failed to setup network for sandbox …`,
carrying your error's `msg`.

**While this stage runs, every new pod on that worker gets your network**,
not just the tester's. The tester removes the config however the stage ends,
and pods started in that window keep the address you gave them until
they are recreated.

## Hints

<details><summary>Nudge</summary>

`crictl pods` and `journalctl -u containerd` on the node show what containerd
did with your plugin.
</details>

## Further reading

- [containerd CRI: CNI config](https://github.com/containerd/containerd/blob/main/docs/cri/config.md)
- [Network plugins (Kubernetes)](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/network-plugins/)
