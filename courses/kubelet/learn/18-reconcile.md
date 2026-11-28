---
title: Pick up where it left off
concepts: [kubelet restart, reconciliation, orphaned containers, state in the runtime]
---

## Core concept

Kubelets restart all the time: an upgrade, a config change, a crash. Their
pods must not notice. That works because the kubelet keeps almost nothing of
its own. The truth is split between two stores it reconciles:

- the **API server** says which pods should exist on this node;
- **containerd** says which containers do exist, and its labels say whose
  they are, which run they are, and how they ended.

So:

- **Stopping the kubelet stops no containers.** Its pods keep running
  without it.
- **A restarted kubelet adopts** every container whose pod still exists. It
  finds the running task, reports the same `containerID`, and keeps
  `restartCount` where it was. Nothing restarts.
- **A container whose pod is gone is an orphan.** If a pod was deleted while
  the kubelet was down, nobody ran its graceful termination, so the kubelet
  kills its containers and removes them.

Orphan cleanup must run only after the informer has synced and every live
pod has a worker. Otherwise a kubelet that has just started would read every
container as an orphan and kill them all.

## Hints

<details><summary>Nudge</summary>

If your earlier stages kept all state in containerd labels, adoption needs
no new code. Check it by restarting your kubelet in the middle of a run.
</details>

<details><summary>Approach</summary>

At the end of housekeeping, which has already started a worker for every
pod, list every container in your containerd namespace. Kill and delete any
whose `io.kubernetes.pod.uid` has no worker.
</details>

## Further reading

- [Kubelet garbage collection](https://kubernetes.io/docs/concepts/architecture/garbage-collection/#containers-images)
- [Upgrading kubelet without restarting pods](https://kubernetes.io/docs/tasks/administer-cluster/kubeadm/kubeadm-upgrade/)
