---
title: Give containers their environment
concepts: [env, valueFrom, downward API, envFrom, variable expansion]
---

## Core concept

A container's environment starts with the image's own `Env` (busybox sets
`PATH`). The kubelet then adds what the pod spec asks for, in order:

1. **envFrom**: every key of a ConfigMap or Secret, each with the
   optional `prefix` in front.
2. **env**, one variable at a time. A later variable replaces an earlier
   one with the same name, so `env` wins over `envFrom`.

Each `env` entry gets its value one of these ways:

- `value`, a literal string, in which `$(NAME)` expands to a variable defined
  **earlier** in the list. A reference to a variable that is not defined
  stays as written, and `$$` is a literal `$`.
- `valueFrom.fieldRef`, the **downward API**: `metadata.name`,
  `metadata.namespace`, `metadata.uid`, `metadata.labels['KEY']`,
  `spec.nodeName`, `status.hostIP`, `status.podIP`, …
- `valueFrom.resourceFieldRef`: the container's own `limits.memory`,
  `requests.cpu`, and so on, in units of the `divisor`, rounded up. A limit
  that is not set is the node's capacity.
- `valueFrom.configMapKeyRef` / `secretKeyRef`: one key of a ConfigMap or
  Secret in the pod's namespace.

`$(NAME)` expands in `command` and `args` too, against the finished
environment. That expansion is the kubelet's, not a shell's, and it is how
`args: ["--port=$(PORT)"]` works without a shell.

A required ConfigMap, Secret or key that does not exist is not a crash. The
container waits with reason **CreateContainerConfigError**, and the kubelet
tries again on the next sync. Mark a reference `optional: true` and a missing
source sets nothing instead.

## Go APIs

- `oci.WithEnv([]string{"K=V", …})`, placed after `oci.WithImageConfig`, so
  it overrides image variables with the same name instead of losing them.
- `cs.CoreV1().ConfigMaps(ns).Get`, `Secrets(ns).Get`. A Secret's `Data`
  holds raw bytes, already base64-decoded.
- `q.MilliValue()` on a `resource.Quantity`, for the divide-and-round-up.

## Hints

<details><summary>Nudge</summary>

Build the environment as an ordered list of names plus a map of values.
Expansion reads the map as it stands when each variable is reached.
</details>

<details><summary>Approach</summary>

Resolve the environment before anything else in `startContainer`, and give
config errors a type of their own so the status can tell them apart from an
image pull failure. Expand `command` and `args` with the same map before you
compute the argv.
</details>

## Further reading

- [Define environment variables for a container](https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container/)
- [Expose pod information through environment variables](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/)
- [Dependent environment variables](https://kubernetes.io/docs/tasks/inject-data-application/define-interdependent-environment-variables/)
