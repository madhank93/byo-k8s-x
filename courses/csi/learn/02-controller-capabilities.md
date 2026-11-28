---
title: Advertise what you implement
concepts: [controller capabilities, node capabilities, Unimplemented]
---

## Core concept

The controller and node services each have a capabilities call:
`ControllerGetCapabilities` and `NodeGetCapabilities`. A CO reads them once
and from then on only calls what they list. Advertise `LIST_VOLUMES` and the
CO may page through your volumes; leave out `STAGE_UNSTAGE_VOLUME` and the
kubelet skips staging and publishes directly.

So the list is a contract in both directions. Everything advertised must be
implemented, and everything that is not must answer gRPC `Unimplemented` — a
driver that half-implements an RPC it does not advertise is lying to a CO
that might, one day, call it anyway.

The tester calls every RPC a capability stands for with an empty request. An
advertised one may reject the request — `InvalidArgument` is fine — but must
not say `Unimplemented`. One that is not advertised must.

This stage: advertise `CREATE_DELETE_VOLUME`, and keep the list honest as the
course adds RPCs. Every later stage re-checks it.

## Go APIs

- `csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{...}}`
  — the nesting is the spec's `oneof`.
- `csi.NodeGetCapabilitiesResponse`, built the same way.
- `codes.Unimplemented`, which the embedded `Unimplemented*Server` already
  returns.

## Hints

<details><summary>Nudge</summary>

Keep one slice of capability types per service and build both the response
and your own sense of what is implemented from it.
</details>

<details><summary>Approach</summary>

A package-level `var controllerCaps = []csi.ControllerServiceCapability_RPC_Type{...}`
and a loop that wraps each one. `CreateVolume` with no name must answer
`InvalidArgument` rather than `Unimplemented` from here on, so a stub that
validates its arguments is enough for now.
</details>

## Further reading

- [ControllerGetCapabilities](https://github.com/container-storage-interface/spec/blob/master/spec.md#controllergetcapabilities)
- [gRPC status codes](https://grpc.io/docs/guides/status-codes/)
