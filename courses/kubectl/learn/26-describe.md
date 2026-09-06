---
title: Describe one object
concepts: [output formats, unstructured, api semantics]
---

## What this stage teaches

Describe answers a different question from get, and the difference explains the
format. A table compares many objects, so every row must fit one line and the
columns are chosen in advance. Describe explains *one* object, so it can spend
vertical space and show fields that only matter when you are looking at exactly
this thing.

It is also not a dump. `-o yaml` is the dump. Describe is a curated view, which
is why real kubectl has a hand-written describer per kind: "the useful fields"
is a judgement, not a rule, and no generic renderer can make it.

Your version can be honest about that — print the shared metadata for anything,
and the few extra fields you know about for pods. The shape of the output is
label, tab, value, run through a tabwriter so the values align.

Missing fields are normal. An unscheduled pod has no node and no IP, and
`unstructured.NestedString` reports "not found" separately from "wrong type" so
you can render `<none>` rather than an empty line.

## Go you'll reach for

- `unstructured.NestedString(obj.Object, "spec", "nodeName")`.
- `tabwriter` again, this time with padding 1 — two columns, not a grid.
- `obj.GetLabels()` returns a map, so sort the keys before joining; map
  iteration order in Go is deliberately random and would make two runs differ.

## Hints

<details><summary>Nudge</summary>

One object means a Get, not a List — and the resource still has to be resolved
through the mapper.
</details>

<details><summary>Approach</summary>

Print `Name`, `Namespace`, `Labels` for any resource, then branch on
`gvr.Group == "" && gvr.Resource == "pods"` for `Node`, `Status` and `IP`.
Leave room at the bottom: stage 27 appends events here.
</details>

## Going deeper

- [kubectl describe](https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands#describe)
