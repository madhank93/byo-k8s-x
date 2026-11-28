---
title: Report the pod back
concepts: [PodStatus, phase, conditions, containerStatuses, status subresource]
---

## Core concept

A container that runs but goes unreported looks to the cluster like a pod
that is still `Pending`. Status is the kubelet's half of the pod, and
everything that reads pods depends on it: `kubectl get`, Deployments counting
ready replicas, Services picking endpoints.

What the kubelet writes, through the pod's `status` subresource:

- **phase**: `Pending` while any container is still waiting to start;
  `Running` while any container runs, or will run again; when they have all
  stopped, `Succeeded` if every one exited 0 and `Failed` if not.
- **hostIP / podIP**: the node's address. A `hostNetwork` pod has no address
  of its own, so both are the node's InternalIP.
- **startTime**: when the kubelet first took the pod on.
- **conditions**: `PodScheduled`, `Initialized`, `ContainersReady` and
  `Ready`. A transition time changes only when the condition's status does.
- **containerStatuses**, one per container:
  - `state`: `waiting` (with a reason: `ContainerCreating`, `ErrImagePull`),
    `running` (with `startedAt`), or `terminated` (with `exitCode`, a reason
    of `Completed` or `Error`, `startedAt` and `finishedAt`);
  - `containerID` (`containerd://ID`), `image` as the spec wrote it, and
    `imageID`, the image pinned by digest (`NAME@sha256:…`);
  - `ready`, `started` and `restartCount`.

Containerd keeps an exited task's status only until the task is deleted. Move
the exit code and time into the container's labels, then delete the task, and
the container keeps its own history. That history survives a restart of the
kubelet too.

Write only when something changed. The API stores times to the second, so
truncate yours to match, or every sync will look like a change.

This stage: once a second, read each pod's containers, work out its status,
and write it if it differs.

## Go APIs

- `task.Status(ctx)`: `Status` is `client.Running` or `client.Stopped`, with
  `ExitStatus` and `ExitTime`.
- `container.SetLabels(ctx, map)` merges labels; `task.Delete(ctx)`.
- `img.Target().Digest` is the digest an `imageID` names.
- `cs.CoreV1().Pods(ns).UpdateStatus(ctx, pod, opts)`;
  `equality.Semantic.DeepEqual` to skip writes that change nothing.

## Hints

<details><summary>Nudge</summary>

Start from a copy of the pod's current status and overwrite the parts you
own. The API server fills in fields like `qosClass` that you must not drop.
</details>

<details><summary>Approach</summary>

Count containers as waiting, running, or stopped (and how many of those
succeeded), then pick the phase in that order. An image pull that fails
leaves no container, so remember the error per container in memory and
report it as `waiting`.
</details>

## Further reading

- [Pod lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- [Container states](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#container-states)
