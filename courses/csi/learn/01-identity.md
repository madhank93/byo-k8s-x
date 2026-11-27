---
title: Serve the Identity service
concepts: [gRPC, unix socket, Identity service, plugin capabilities, graceful shutdown]
---

## Core concept

A CSI driver is a gRPC server on a unix socket. The CO is told the socket's
path, dials it, and the first thing it calls is the **Identity** service:

- `GetPluginInfo` — the driver's name and a `vendor_version`. The name is how
  Kubernetes refers to the driver everywhere: in a StorageClass's
  `provisioner`, in a PersistentVolume's `csi.driver`. It comes from
  `--name`.
- `GetPluginCapabilities` — which of the other services the driver offers. A
  driver that creates volumes has `CONTROLLER_SERVICE`; without it a CO never
  calls the controller service at all.
- `Probe` — is the driver healthy. An empty response means yes.

The socket outlives a crash: a driver that died leaves a file behind, and
`listen` on an existing path fails. Remove whatever is there before you
listen. On SIGTERM stop serving, let in-flight calls finish, and leave no
socket behind.

This stage: serve Identity on `--endpoint unix://PATH`, report `--name`,
advertise `CONTROLLER_SERVICE`, and exit 0 on SIGTERM.

## Go APIs

- `github.com/container-storage-interface/spec/lib/go/csi` — the generated
  types and `csi.RegisterIdentityServer`. Embed
  `csi.UnimplementedIdentityServer` (and the controller and node ones) so
  everything you have not written yet answers `Unimplemented`.
- `google.golang.org/grpc` — `grpc.NewServer`, `srv.Serve(lis)`,
  `srv.GracefulStop()`.
- `net.Listen("unix", path)`, `os.Remove`.
- `signal.NotifyContext(ctx, syscall.SIGTERM)`.

## Hints

<details><summary>Nudge</summary>

`GracefulStop` makes `Serve` return. Call it from a goroutine that waits on
the signal context, and let `main` return when `Serve` does.
</details>

<details><summary>Approach</summary>

Strip `unix://`, `MkdirAll` the socket's directory, remove a stale file,
listen, register a driver struct that embeds the three `Unimplemented*Server`
types, and serve. A unix listener Go created removes its socket when it is
closed.
</details>

## Further reading

- [The CSI spec: Identity service](https://github.com/container-storage-interface/spec/blob/master/spec.md#identity-service-rpc)
- [Kubernetes CSI developer docs](https://kubernetes-csi.github.io/docs/developing.html)
