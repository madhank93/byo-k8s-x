---
title: Run static pods from files
concepts: [static pods, mirror pods, pod sources, --pod-manifest-path]
---

## Core concept

The API server is not the kubelet's only source of pods. A **static pod** is
a manifest file in `--pod-manifest-path`. The kubelet runs it without asking
anyone, and no scheduler is involved. That is how a control plane starts
itself: kubeadm writes `kube-apiserver.yaml`, `etcd.yaml` and the rest into
`/etc/kubernetes/manifests`, and the kubelet runs them before there is an
API server to ask.

A static pod still shows up in `kubectl get pods`, as a **mirror pod**:

- named `NAME-NODE`, bound to the node, owned by the Node object;
- annotated `kubernetes.io/config.mirror`, whose value is the static pod's
  UID, a hash of the manifest and the node, so the same file always makes
  the same pod;
- carrying the static pod's status, which the kubelet writes to it.

The mirror pod is a reflection, not the real thing:

- **Delete the mirror pod** and nothing stops. The kubelet sees the static
  pod's mirror is missing and creates it again, with a new UID. The
  container keeps running untouched.
- **Remove the file** and the static pod stops: graceful termination as
  usual, then the kubelet deletes the mirror pod.

The kubelet re-reads the directory every few seconds (20 by default), so
editing a manifest is a deploy: a changed file has a new hash, which makes it
a new pod.

## Go APIs

- `os.ReadDir`, `sigs.k8s.io/yaml.Unmarshal` into a `corev1.Pod`.
- `crypto/md5` over the file and the node name, for a stable UID.

## Hints

<details><summary>Nudge</summary>

A static pod and its mirror share a cache key (`namespace/name`) but not a
UID. Look the mirror up by key, and check that its annotation names the
static pod.
</details>

<details><summary>Approach</summary>

Load the manifests into a map of UID to pod. Give each static pod a worker
that reads its spec from the map and its status from the mirror. Write
status to the mirror's object. Housekeeping creates any missing mirror,
deletes mirror pods whose static pod is gone (or that someone deleted, with
grace 0, so they can be recreated), and never starts a worker for a mirror
pod itself.
</details>

## Further reading

- [Create static pods](https://kubernetes.io/docs/tasks/configure-pod-container/static-pod/)
- [kubeadm: static pod manifests](https://kubernetes.io/docs/reference/setup-tools/kubeadm/implementation-details/#generate-static-pod-manifest-for-control-plane-components)
