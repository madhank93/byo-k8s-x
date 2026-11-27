---
title: Survive a CO that retries
concepts: [concurrent calls, idempotency, Aborted, serialisation]
---

## Core concept

Every stage so far sent one call at a time. A real CO does not. The
external-provisioner times out a slow `CreateVolume` and sends it again while
the first is still running; the kubelet's reconciler can stage a volume from
two goroutines at once. The calls arrive on separate gRPC streams, which
grpc-go serves on separate goroutines, and nothing makes them take turns.

Each check-then-act in the driver is a race:

- two `CreateVolume`s of one name both find no record, and both make one;
- two `NodeStageVolume`s both see nothing mounted, and both mount — stacked
  mounts, and one unstage later still leaves one behind;
- two writers of different records step on the same temporary file.

The spec gives a driver two ways out. It may make concurrent calls take
turns, so the second one finds the first one's work done and succeeds as a
repeat. Or it may refuse a call while the same operation on the same volume
is in flight, with `Aborted`, and let the CO retry. Either is correct; what
is not is two calls both doing the work.

This stage: eight copies of each call at once must leave exactly the state
one call would have.

## Go APIs

- `sync.Mutex` — the simplest correct answer is one lock around every call
  that changes state.
- `codes.Aborted`, for the other answer.

## Hints

<details><summary>Nudge</summary>

Calls that only read — capabilities, `ListVolumes`, `GetCapacity` — do not
need the lock.
</details>

<details><summary>Approach</summary>

A mutex on the driver, taken at the top of every create, delete, publish,
stage, expand and snapshot call. A lock per volume id scales better, and is
the next step for a driver with slow storage.
</details>

## Further reading

- [CSI spec: concurrency](https://github.com/container-storage-interface/spec/blob/master/spec.md#concurrency)
- [CSI spec: error scheme, Aborted](https://github.com/container-storage-interface/spec/blob/master/spec.md#error-scheme)
