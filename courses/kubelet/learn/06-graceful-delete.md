---
title: Stop a pod gracefully
concepts: [graceful termination, SIGTERM, grace period, deletionTimestamp, UID precondition]
---

## Core concept

`kubectl delete pod` does not delete a pod. It sets `deletionTimestamp` and
`deletionGracePeriodSeconds` (from `terminationGracePeriodSeconds`, 30 by
default), and the pod stays in the API, `Terminating`, until its kubelet says
it is done. The kubelet owns the last step:

1. Send every running container **SIGTERM**.
2. Wait for them to exit, but no longer than the grace period.
3. Send **SIGKILL** to whatever is still running.
4. Delete the pod with grace period 0, which really removes it. Give the
   delete a **UID precondition**, so it cannot remove a newer pod that has
   the same name.

Finish as soon as the containers exit. A container that handles SIGTERM in
half a second should not hold its pod for thirty.

A pod deleted with `--grace-period=0 --force` skips all of this. It leaves
the API at once, and the kubelet sees it vanish from its cache. Anything
still running for that pod is now an orphan, so kill it and delete its
containers.

The signal goes to the container's pid 1, and the kernel never delivers a
signal to pid 1 that pid 1 has no handler for. A shell running as pid 1
ignores SIGTERM unless it traps it. That is why the grace period ends in
SIGKILL, which pid 1 cannot ignore.

## Go APIs

- `task.Wait(ctx)`: call it before `Kill`. It returns a channel that
  delivers the exit.
- `task.Kill(ctx, syscall.SIGTERM)`, and later `syscall.SIGKILL`.
- `metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0)), Preconditions:
  metav1.NewUIDPreconditions(uid)}`.
- `task.Delete(ctx, client.WithProcessKill)`,
  `container.Delete(ctx, client.WithSnapshotCleanup)`.

## Hints

<details><summary>Nudge</summary>

Check `deletionTimestamp` before anything else in your sync. A terminating
pod should never have a container started for it.
</details>

<details><summary>Approach</summary>

Stop all the pod's containers in parallel against one shared deadline, then
delete the pod. Keep the pod's worker running until the pod leaves the
cache. When it does, remove the pod's containers and log directory.
</details>

## Further reading

- [Termination of Pods](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
- [Forced pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-forced)
