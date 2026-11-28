---
title: Restart what stops answering
concepts: [livenessProbe, startupProbe, exec probe, httpGet probe, failureThreshold]
---

## Core concept

A process can be running and still be broken: deadlocked, wedged, serving
nothing. A **liveness probe** is the kubelet asking the container, every
`periodSeconds`, whether it is still all right. After `failureThreshold`
failures in a row, the kubelet kills the container (SIGTERM, the grace
period, then SIGKILL), and the restart policy runs it again, with back-off
as before.

A probe is one of:

- **exec**: run a command inside the container. Exit 0 is success. The
  command runs as a second process in the container's own namespaces, with
  its environment: containerd calls this an exec process in the task.
- **httpGet**: an HTTP GET to the container's port and path. Any status from
  200 to 399 is success. With host networking the container's address is the
  node's.
- **tcpSocket**: a TCP connection that opens is success.

The port can be a number or the **name** of one of the container's `ports`.
Every probe also has a `timeoutSeconds`, and one that takes longer has
failed.

A slow starter would be killed by its liveness probe before it ever came up.
A **startup probe** holds the other probes back: until it passes, neither
liveness nor readiness runs, and the container reports `started: false`. If
it fails `failureThreshold` times, the container is killed, just as a
liveness failure kills it.

Probe results belong to one run of a container. A run that replaces it
starts with a clean slate.

## Go APIs

- `task.Exec(ctx, execID, &processSpec, cio.NullIO)`; copy the container's
  `spec.Process` and set `Args`. Then `proc.Wait`, `proc.Start`, and
  `proc.Delete` when it is done.
- `http.Client{Timeout: …}`, `net.DialTimeout`.
- The API server fills in probe defaults: period 10s, timeout 1s,
  failure threshold 3, success threshold 1.

## Hints

<details><summary>Nudge</summary>

Keep each probe's last run time and its consecutive successes and failures,
keyed by containerd container ID. Then running a probe on every sync is just
a question of whether it is due.
</details>

<details><summary>Approach</summary>

For each running container: if it has a startup probe that has not passed,
run only that one. Otherwise mark the container started, run liveness, and
kill the container when the failures reach the threshold. Report
`started: false` while the startup probe is pending.
</details>

## Further reading

- [Configure liveness, readiness and startup probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
- [Container probes](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#container-probes)
