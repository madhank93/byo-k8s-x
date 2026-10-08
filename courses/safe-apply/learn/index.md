---
title: Build your own diff and safe apply — primer
concepts: [declarative config, three-way merge, server-side apply, dry-run, managedFields, prune, drift]
---

## The mental model

`kubectl apply` answers one question: *make the cluster look like these
files.* `kubectl diff` answers the question you should ask first: *what will
that actually change?* This course builds both, and then goes past them to
the questions a careful operator asks next — whose fields am I about to take,
what will I delete, has anyone changed things behind my back, and will the
change I reviewed be the change that lands?

Every stage grows one program. By the end it can:

```
  manifests ──► load ──► fetch live ──► dry-run apply ──► diff ──► plan file
                                            │                         │
                                   admission, ownership,              ▼
                                   conflicts, prune guards       apply the plan,
                                                                 or refuse it
```

## Three versions of every object

A declarative tool always has three things in view:

| | What it is | Where it lives |
|---|---|---|
| **desired** | what your manifest says now | the file |
| **live** | what the cluster has now | the API server |
| **last applied** | what *you* said last time | an annotation, or `managedFields` |

Two of them are not enough. Comparing desired with live cannot tell a field
you deleted from your manifest (remove it) from a field someone else set
(leave it alone). The third version is what tells them apart, and the whole
history of `apply` is a history of where that third version is kept.

Client-side apply keeps it in the `kubectl.kubernetes.io/last-applied-configuration`
annotation and computes a **three-way merge** on your machine. Server-side
apply keeps it in `metadata.managedFields`, per field and per manager, and
does the merge on the server — which is why the server can also tell you,
before you write anything, that someone else owns a field you are changing.

## Dry-run is the server telling the truth

A local merge is a guess about what the server will do. The server also
defaults fields, runs mutating and validating admission, and enforces
ownership. A **server-side dry-run** (`dryRun=All`) runs all of that and
stops just before storage, so the object it returns is the object you would
get. From stage 6 onwards, every diff is computed against that answer.

## Exit codes

The `diff` command follows `kubectl diff`: exit **0** when nothing would
change, **1** when something would, and **2** or more when the diff itself
could not be computed — including when the server refuses the change.

## How the stages are graded

Each stage writes manifests to a temporary directory, runs your program with
a kubeconfig whose context points at a namespace of its own, and judges the
exit code, stdout, and the state the cluster is left in. Your program uses
the field manager `byok8s` unless told otherwise. Nothing inspects your
source.

## Further reading

- [Declarative management of objects](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/declarative-config/)
- [Server-Side Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
- [kubectl diff](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_diff/)
