---
title: Say when a container is ready
concepts: [readinessProbe, Ready condition, ContainersReady, endpoints, tcpSocket probe]
---

## Core concept

Running is not the same as ready to take traffic. A server may need to warm
a cache or wait for a database, or may want to shed load for a while. A
**readiness probe** decides a container's `ready` flag, and so the pod's
`ContainersReady` and `Ready` conditions:

- A container with a readiness probe starts **not ready**, and becomes ready
  after `successThreshold` passes in a row.
- `failureThreshold` failures in a row make it not ready again.
- Failing never restarts the container. The point is to stop sending it
  traffic, not to kill it.

`Ready` is what the rest of the cluster acts on. The EndpointSlice
controller adds a pod to its Service only while the pod is Ready, and a
rolling update waits for new pods to be Ready before it removes old ones. So
the transition time on that condition matters: it moves only when the status
does.

A container without a readiness probe is ready as soon as it runs. One with a
startup probe is not ready until that probe has passed.

## Hints

<details><summary>Nudge</summary>

You already run probes and count results. Readiness is the one probe that
changes a flag in the status instead of killing anything.
</details>

<details><summary>Approach</summary>

After liveness, tick the readiness probe. Set `ready` when its consecutive
successes reach the threshold, and clear it when its consecutive failures
do. In the status, a running container with a readiness probe is ready only
when that flag is set.
</details>

## Further reading

- [Define readiness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes)
- [Pod readiness](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-readiness-gate)
