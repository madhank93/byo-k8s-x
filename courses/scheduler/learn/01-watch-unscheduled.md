---
title: Find the pods waiting for you
concepts: [watch, informer, schedulerName, fieldselector]
---

## Core concept

A scheduler starts from one question: which pods have nobody placing them? For
this program the answer is a pod whose `spec.schedulerName` is `byok8s` and
whose `spec.nodeName` is still empty. Every other pod is someone else's —
either the default scheduler's, or already on a node.

There are two halves to finding them, and each misses what the other catches.
A **list** sees the pods that existed when you asked and nothing after. A
**watch** sees what changes after you start and nothing before. A scheduler
that only watches never places the pods that were waiting when it started; one
that only lists never places anything created later. An informer does both, in
the right order: it lists, then watches from the point the list left off, so
nothing falls in between.

You do not have to receive every pod in the cluster to find yours. Pods
support a field selector on both fields, so the API server can do the
filtering and send you only what is yours:

```
spec.schedulerName=byok8s,spec.nodeName=
```

The empty value after `spec.nodeName=` is the point: it matches pods with no
node.

For this stage, print one line per pod you find:

```
unscheduled <namespace>/<name>
```

The tester creates one such pod before your program starts and one while it
runs, and one that names the default scheduler, which must not appear.

## Go APIs

- `informers.NewSharedInformerFactoryWithOptions` with
  `informers.WithTweakListOptions` to set the field selector on both the list
  and the watch.
- `fields.AndSelectors` and `fields.OneTermEqualSelector` from
  `k8s.io/apimachinery/pkg/fields` to build the selector.
- `cache.ResourceEventHandlerFuncs{AddFunc: …}` — an informer delivers the
  listed pods as adds, exactly like new ones.
- `cache.WaitForCacheSync` before trusting that you have seen everything.

## Hints

<details><summary>Nudge</summary>

What does your program see of a pod that was created a minute before it
started?
</details>

<details><summary>Approach</summary>

One informer on pods, in every namespace, with the field selector above. Print
from its add handler. Keep the program running until it is signalled.
</details>

## Further reading

- [Scheduling in Kubernetes](https://kubernetes.io/docs/concepts/scheduling-eviction/kube-scheduler/)
- [Configure multiple schedulers](https://kubernetes.io/docs/tasks/extend-kubernetes/configure-multiple-schedulers/)
