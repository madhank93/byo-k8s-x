---
title: Run init containers first
concepts: [init containers, Initialized condition, PodInitializing, ordering]
---

## Core concept

`spec.initContainers` run before the app containers do. They run **one at a
time, in order**, and each must exit 0 before the next starts. Only when the
last one has succeeded does any container in `spec.containers` start. That
ordering is what makes them useful: an init container can wait for a
database, fetch a config, or fix permissions on a volume, and the app can
rely on the work being done.

While they run:

- the pod is **Pending**, and its `Initialized` condition is False;
- `status.initContainerStatuses` reports each init container, in the same
  shape as `containerStatuses`;
- every app container is `waiting` with reason **PodInitializing**, and
  containerd holds no container for it yet.

A failed init container follows the pod's `restartPolicy`, with one change:
`Always` counts as `OnFailure`, because an init container that succeeded is
never run again. With `Never`, one failure fails the whole pod. Otherwise the
container is retried with the same back-off as any other.

A kubelet that restarts must not run the init containers again. Their
finished runs are still in containerd, with exit code 0, so they read as
done.

## Hints

<details><summary>Nudge</summary>

"Done" for an init container means its latest run exited 0. Walk
`spec.initContainers` in order, skip the ones that are done, and act on the
first one that is not. Never go past it.
</details>

<details><summary>Approach</summary>

Pull the start-or-back-off logic for one container out of your sync loop, so
it can take a restart policy. Call it for the first unfinished init container
with the init policy. Once every init container is done, call it for each
app container with the pod's policy. Report the phase as Failed when an init
container fails under `Never`, and as Pending until initialization is done.
</details>

## Further reading

- [Init containers](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/)
- [Detailed behavior](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#detailed-behavior)
