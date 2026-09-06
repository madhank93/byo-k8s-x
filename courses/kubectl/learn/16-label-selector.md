---
title: Filter by label
concepts: [selectors, labels, rest api]
---

## What this stage teaches

Labels are the identifying metadata everything in Kubernetes coordinates
through: a Service finds its pods by selector, a Deployment owns its
ReplicaSets by selector. Filtering by them is not a convenience feature, it is
how the system refers to sets of objects.

The essential part is *where* the filter runs. `LabelSelector` goes into the
request, so the objects you did not ask for are never sent. Listing everything
and filtering in your loop gives the same answer on a three-pod cluster and
falls over on a real one — and the cost lands on the apiserver, not just on
you.

The syntax is richer than equality: `app=web`, `app!=web`, `tier in (a,b)`,
`app` (exists), `!app` (does not), comma-separated for AND. You can pass the
string straight through; client-go parses it server-side.

## Go you'll reach for

- `metav1.ListOptions{LabelSelector: s}`.
- `labels.Parse(s)` if you want to validate before sending.

## Hints

<details><summary>Nudge</summary>

The flag value can go into the request almost unchanged. If you are writing a
comparison loop, you are solving the wrong half.
</details>

<details><summary>Approach</summary>

Thread the selector down to the `List` call and leave the printing untouched.
Note that it is only meaningful for List — a Get already names one object.
</details>

## Going deeper

- [Labels and selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
