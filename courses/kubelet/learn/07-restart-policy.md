---
title: Restart what exits
concepts: [restartPolicy, CrashLoopBackOff, restartCount, lastState, exponential back-off]
---

## Core concept

A pod's `restartPolicy` covers every container in it, and only the kubelet
acts on it:

| policy | exit 0 | exit non-zero |
|---|---|---|
| `Always` (the default) | run again | run again |
| `OnFailure` | stay stopped | run again |
| `Never` | stay stopped | stay stopped |

Running a container again does not restart the old one. It creates a **new
container** from the same spec, with `restartCount` one higher and a log file
of its own (`1.log`, `2.log`, …). The run that just ended is kept, because
the status reports it as `lastState`, and that is how `kubectl logs
--previous` finds its output.

A container that keeps exiting must not spin. The kubelet waits **10
seconds** after the first exit, then 20, 40, and so on, doubling up to five
minutes. While it waits, the container's state is `waiting` with reason
**CrashLoopBackOff**, and `lastState.terminated` holds the exit that put it
there. The pod is still `Running` the whole time, because its container will
run again.

This changes the phase rule: a container that is waiting after an exit counts
as stopped, not as waiting to start. With `Always`, the pod stays Running.
With `OnFailure`, it stays Running until every container has exited 0.

## Go APIs

- A label such as `io.kubernetes.container.restartCount` on each containerd
  container tells you which run it is.
- `container.Delete(ctx, client.WithSnapshotCleanup)` for runs too old to
  report.

## Hints

<details><summary>Nudge</summary>

Group a pod's containerd containers by container name and sort them by
restart count. The last one is the current run, and the one before it is
`lastState`.
</details>

<details><summary>Approach</summary>

For each container whose latest run has exited and should run again: if
`finishedAt + backoff(restartCount)` has passed, start run `restartCount+1`;
if not, report CrashLoopBackOff. Keep the latest two runs and delete older
ones.
</details>

## Further reading

- [Container restart policy](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy)
- [Debugging CrashLoopBackOff](https://kubernetes.io/docs/tasks/debug/debug-application/debug-running-pod/)
