---
title: List the pods
concepts: [typed clients, listing, namespaces]
---

## What this stage teaches

The first real read. A typed clientset gives you a method per resource —
`CoreV1().Pods(ns).List(...)` — which returns `*corev1.PodList` with real Go
fields. It is the pleasant way to work when you know the type at compile time,
and it is the thing you will trade away at stage 14 for the ability to handle
resources you have never heard of.

Note where the namespace goes: into the *client*, not the options. That is the
URL shape showing through — the namespace is a path segment, and the empty
string means a path without one.

Every call takes a `context.Context`. It is not decoration: it is the timeout
and the cancellation for a network request that can hang.

## Go you'll reach for

- `kubernetes.NewForConfig(cfg)` — the typed clientset.
- `cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})`.
- `metav1` is `k8s.io/apimachinery/pkg/apis/meta/v1` — the options and
  ObjectMeta types shared by every group.

## Hints

<details><summary>Nudge</summary>

`kubernetes.NewForConfig`, then follow the group and version into the method:
pods are `v1` in the core group, which client-go spells `CoreV1()`.
</details>

## Going deeper

- [client-go clientset](https://pkg.go.dev/k8s.io/client-go/kubernetes)
