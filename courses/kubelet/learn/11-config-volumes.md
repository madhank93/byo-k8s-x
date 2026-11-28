---
title: Project ConfigMaps and Secrets into files
concepts: [ConfigMap volume, Secret volume, items, defaultMode, live updates]
---

## Core concept

A ConfigMap or Secret mounted as a volume becomes a directory of files, one
per key:

- With no `items`, **every key** is a file named after the key.
- With `items`, only the keys listed are written, each at its `path`. A path
  may include subdirectories, and an item can set its own `mode`.
- `defaultMode` sets the mode of every file that does not set its own. The
  default is 0644. Secrets are often mounted 0400.
- A **Secret** volume is a tmpfs, so a secret never reaches the node's disk.
- `optional: true` lets the volume be empty when the object does not exist.
  Without it, the volume cannot be set up and the container waits in
  `ContainerCreating` until the object appears.

Unlike environment variables, which are fixed when the container starts,
these files **follow their object**. Change the ConfigMap, and the kubelet
rewrites the files while the pod keeps running. A real kubelet notices within
about a minute; this one refreshes on every sync. That is why applications
can reload config from a mounted file and never from an env var.

Write each file so that a reader never sees half of it: write a temporary
file next to it, then rename it into place. A rename within one directory is
atomic. A key that disappears from the object takes its file with it.

## Go APIs

- `cs.CoreV1().ConfigMaps(ns).Get`, with `Data` (strings) and `BinaryData`;
  `Secrets(ns).Get`, with `Data` (bytes).
- `os.WriteFile` and `os.Chmod` on a temporary path, then `os.Rename`;
  `filepath.WalkDir` to find files that should no longer be there.

## Hints

<details><summary>Nudge</summary>

Set up volumes once per sync rather than once per container start. The same
code then creates them the first time and refreshes them after that.
</details>

<details><summary>Approach</summary>

Turn a ConfigMap or Secret into a map of relative path to {bytes, mode},
using either all keys or the items. Write that map into the volume's
directory, skipping files that are already right, and remove anything else.
Give errors from a missing object their own type, and report them as
`ContainerCreating`.
</details>

## Further reading

- [ConfigMaps: mounted ConfigMaps are updated automatically](https://kubernetes.io/docs/concepts/configuration/configmap/#mounted-configmaps-are-updated-automatically)
- [Secrets as files](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod)
