---
title: Run lifecycle hooks
concepts: [postStart, preStop, graceful shutdown, container lifecycle]
---

## Core concept

A container's `lifecycle` gives the kubelet two hooks to run on its behalf,
each an `exec` command (or an `httpGet`) like a probe:

- **postStart** runs as soon as the container's process has started, beside
  it, not before it. The kubelet waits for it to finish before it counts
  the container as running. If the hook fails, the kubelet kills the
  container, and the restart policy decides what happens next.
- **preStop** runs when the kubelet is about to stop the container: on pod
  deletion, a failed liveness probe, or eviction. It runs **before**
  SIGTERM, and its time counts against the grace period.

preStop is how a process that cannot handle SIGTERM gets a clean shutdown:
`nginx -s quit`, deregistering from a service, or just `sleep 5` so load
balancers notice the pod is not ready before it stops serving. That last one
is the most common preStop hook in production.

The order on deletion becomes: preStop, wait for it, SIGTERM, wait out what
remains of the grace period, SIGKILL.

## Go APIs

- The `execIn` you wrote for exec probes runs hooks too: a second process in
  the task, with the container's environment.
- `time.Until(deadline)` bounds the preStop hook by what is left of the
  grace period.

## Hints

<details><summary>Nudge</summary>

To find a running container's hooks, use its `io.kubernetes.container.name`
label to look up that container in the pod spec.
</details>

<details><summary>Approach</summary>

In `startContainer`, after `task.Start`, run postStart and stop the task if
the hook fails. In `stopContainers`, run each container's preStop in that
container's own goroutine, before its SIGTERM, with the same shared
deadline.
</details>

## Further reading

- [Container lifecycle hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/)
- [Attach handlers to container lifecycle events](https://kubernetes.io/docs/tasks/configure-pod-container/attach-handler-lifecycle-event/)
