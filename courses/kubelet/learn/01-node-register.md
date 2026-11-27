---
title: Register a node
concepts: [Node, self-registration, labels, taints, idempotent create]
---

## Core concept

A kubelet's first act is to create its own Node object. The name is the
machine's hostname unless `--hostname-override` says otherwise, and the Node
carries a few labels every node has:

- `kubernetes.io/hostname` — the node's name.
- `kubernetes.io/arch` — the architecture it runs, `runtime.GOARCH`.

`--register-with-taints` lists taints to put on the Node at creation, in the
form `KEY=VALUE:EFFECT`, comma-separated. A `NoSchedule` taint keeps the
scheduler from placing pods that do not tolerate it.

Registration has to survive a restart. A kubelet that restarts finds its Node
already there, and that is the normal case, not an error: it carries on with
the Node it has. Deleting and recreating it would give it a new UID and orphan
everything that refers to the old one.

This stage: create Node NAME with those labels and taints, treat an existing
one as yours, and keep running until SIGTERM.

## Go APIs

- `clientcmd.BuildConfigFromFlags("", path)` and `kubernetes.NewForConfig`.
- `cs.CoreV1().Nodes().Create`.
- `apierrors.IsAlreadyExists(err)`.
- `corev1.LabelHostname`, `corev1.LabelArchStable`, `corev1.Taint`.
- `signal.NotifyContext(ctx, syscall.SIGTERM)` to wait for the stop.

## Hints

<details><summary>Nudge</summary>

Exiting after the create is the most common failure. A kubelet is a daemon:
block on a context that SIGTERM cancels.
</details>

<details><summary>Approach</summary>

Parse the three flags with `flag`. Split the taints on `,`, each one on the
last `:` for the effect and on `=` for the value. Create the Node; if the API
server answers AlreadyExists, carry on. Then wait for the signal.
</details>

## Further reading

- [Nodes: self-registration](https://kubernetes.io/docs/concepts/architecture/nodes/#self-registration-of-nodes)
- [kubelet command-line reference](https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/)
- [Well-known labels, annotations and taints](https://kubernetes.io/docs/reference/labels-annotations-taints/)
