---
title: Register with the kubelet
concepts: [kubelet plugin watcher, registration API, CSINode]
---

## Core concept

The kubelet does not read a list of drivers from anywhere. It watches one
directory, `/var/lib/kubelet/plugins_registry`, and every unix socket that
appears there is a plugin asking to be known. It dials the socket and speaks
the kubelet's own **registration API**, which is not CSI:

1. `GetInfo` — the plugin says what it is (`CSIPlugin`), its name, the path
   of its real endpoint, and the API versions it speaks.
2. The kubelet dials that endpoint and calls CSI's `NodeGetInfo`, then writes
   the driver, its node id and its topology keys into the node's **CSINode**
   object.
3. `NotifyRegistrationStatus` — the kubelet says whether it worked.

When the socket goes, the kubelet drops the driver from CSINode. Real drivers
leave this to a sidecar, node-driver-registrar; here the driver serves both
sockets itself.

This stage: with `--registration-socket PATH`, serve the registration API
there, and remove it on SIGTERM.

## Go APIs

- `k8s.io/kubelet/pkg/apis/pluginregistration/v1` — `RegisterRegistrationServer`,
  `UnimplementedRegistrationServer`, `PluginInfo`, the `CSIPlugin` constant.
- A second `grpc.NewServer()` on a second listener.

## Hints

<details><summary>Nudge</summary>

`Endpoint` is a path in the node's filesystem, not a URL: the kubelet runs on
the same machine.
</details>

<details><summary>Approach</summary>

Listen on the registration socket the same way as the CSI one, register a
small type that embeds `UnimplementedRegistrationServer`, serve it in a
goroutine, and stop both servers on the signal. `SupportedVersions` of
`["1.0.0"]` is what node-driver-registrar sends.
</details>

## Further reading

- [Kubelet plugin registration](https://github.com/kubernetes/kubernetes/tree/master/pkg/kubelet/pluginmanager)
- [node-driver-registrar](https://github.com/kubernetes-csi/node-driver-registrar)
- [CSINode](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/csi-node-v1/)
