---
title: Provision a volume
concepts: [CreateVolume, idempotency key, capacity range, volume capabilities]
---

## Core concept

`CreateVolume` makes a volume and returns its `volume_id`, the handle every
later call uses. Its `name` is chosen by the CO — Kubernetes uses `pvc-<uid>`
— and it is the **idempotency key**: the same name twice must answer the same
volume, because the CO cannot tell a call that failed from one whose answer
was lost.

What a request carries and what the spec makes of it:

- `capacity_range` — `required_bytes` is the floor, `limit_bytes` the
  ceiling, either may be 0. Pick a size inside it, or a default if both are
  0. A range no size satisfies is `OutOfRange`.
- `volume_capabilities` — how the volume will be used: as a mounted
  filesystem or a raw block device, and with which access mode. Required.
  This driver offers filesystems only, so a block request is
  `InvalidArgument`.
- the same name again with a size the existing volume does not fit is
  `AlreadyExists`.

The volumes in this stage are directories under `--root`. Remember each
volume in a small file there too: the next stage restarts the driver.

This stage: `CreateVolume`, idempotent by name, with the spec's error codes.

## Go APIs

- `status.Error(codes.InvalidArgument, ...)` from `google.golang.org/grpc/status`.
- `crypto/sha256` — a volume id derived from the name makes a retry find the
  volume it already made.
- `encoding/json`, `os.MkdirAll`, `os.WriteFile`.

## Hints

<details><summary>Nudge</summary>

If the id is a hash of the name, "have I made this already?" is a single file
lookup, and no table of names to ids is needed.
</details>

<details><summary>Approach</summary>

Validate the name and capabilities, compute the size, derive the id, and load
its record: if it exists and fits the range, return it; if it exists and does
not, `AlreadyExists`. Otherwise make the directory and save the record.
</details>

## Further reading

- [CreateVolume](https://github.com/container-storage-interface/spec/blob/master/spec.md#createvolume)
- [CreateVolume errors](https://github.com/container-storage-interface/spec/blob/master/spec.md#createvolume-errors)
