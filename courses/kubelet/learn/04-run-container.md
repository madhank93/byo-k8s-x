---
title: Run a pod's containers
concepts: [informer, field selector, containerd, OCI spec, command and args]
---

## Core concept

The scheduler's whole output is one field: `spec.nodeName`. A kubelet finds
its work by watching pods with a **field selector** on that field —
`spec.nodeName=NAME` — so the API server only ever sends it its own pods. An
informer keeps that set in a local cache, and the kubelet works from the
cache rather than asking the API server every time.

For each pod, the kubelet asks the container runtime to run every container
in `spec.containers`. A real kubelet speaks CRI to containerd; this course
drives containerd through its own Go client instead, in a containerd
**namespace** of its own (`--containerd-namespace`). The worker's real
kubelet treats every CRI sandbox it did not create as an orphan and kills it,
but it never looks outside CRI, so containers in another namespace are yours
alone.

What running a container takes:

- **The image.** Pod images are short names; `busybox:1.36` means
  `docker.io/library/busybox:1.36`. Use the one containerd already has, and
  pull only when it is missing.
- **A snapshot and a spec.** The snapshot is the container's writable rootfs;
  the OCI spec says what to run in it. Start from the image's config.
- **The argv.** `command` replaces the image's `ENTRYPOINT` and `args`
  replaces its `CMD`. Setting `command` alone drops the image's `CMD` too.
- **Labels** that tie it back to the pod: `io.kubernetes.pod.uid`,
  `io.kubernetes.container.name` (plus pod name and namespace). Containerd
  is the kubelet's memory: after a restart, the labels are how it knows
  which container is whose.
- **Its output** in a file:
  `ROOT/logs/NAMESPACE_POD_UID/CONTAINER/0.log`, under `--root-dir`.
- **The network.** Every pod in this course is `hostNetwork: true`, so the
  container shares the node's network namespace. Pod networking is the cni
  course's job.

This stage: watch your pods and start each container once.

## Go APIs

- `informers.NewSharedInformerFactoryWithOptions(cs, 0,
  informers.WithTweakListOptions(...))` with
  `fields.OneTermEqualSelector("spec.nodeName", name)`.
- `client.New("/run/containerd/containerd.sock",
  client.WithDefaultNamespace(ns))` from
  `github.com/containerd/containerd/v2/client`.
- `ctr.GetImage`, `ctr.Pull(ctx, ref, client.WithPullUnpack)`;
  `reference.ParseDockerRef` from `github.com/distribution/reference`.
- `ctr.NewContainer(ctx, id, client.WithImage(img),
  client.WithNewSnapshot(id, img), client.WithContainerLabels(...),
  client.WithNewSpec(oci.WithImageConfig(img), oci.WithProcessArgs(...),
  oci.WithHostNamespace(specs.NetworkNamespace)))`.
- `container.NewTask(ctx, cio.LogFile(path))`, then `task.Start(ctx)`.

## Hints

<details><summary>Nudge</summary>

`ctr.Containers(ctx, "labels.\"io.kubernetes.pod.uid\"==UID")` lists a pod's
containers. A container that already exists has been started; leave it.
</details>

<details><summary>Approach</summary>

Wait for the informer to sync, then loop: for every cached pod, for every
container not yet in containerd, resolve the image, create the container
with a fresh random ID, and start its task with stdout and stderr going to
the log file. `oci.WithImageConfig` sets the image's argv; a later
`oci.WithProcessArgs` overrides it.
</details>

## Further reading

- [Define a command and arguments for a container](https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/)
- [containerd: getting started with the Go client](https://github.com/containerd/containerd/blob/main/docs/getting-started.md)
- [containerd namespaces](https://github.com/containerd/containerd/blob/main/docs/namespaces.md)
