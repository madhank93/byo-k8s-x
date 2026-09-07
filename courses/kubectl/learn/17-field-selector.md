---
title: Filter by field
concepts: [selectors, indexing, api semantics]
---

## Core concept

Field selectors look like label selectors and are a different axis. Labels are
yours to invent and are indexed by design; fields belong to the object itself
and **only a few are indexed** — `metadata.name`, `metadata.namespace`,
`status.phase` for pods, `spec.nodeName` on most versions, and per-resource
extras registered by the apiserver.

Ask for anything else and the request is **rejected**, not silently scanned.
That refusal is deliberate and worth appreciating: an unindexed filter would
look instant to you and cost the apiserver a full collection scan on every
call. The API would rather say no than let one client's convenience become
everyone's latency.

Practically: use fields when you want objects "in state X" or "on node Y", and
labels for anything you control. Both can be sent on the same request; they AND
together.

## Go APIs

- `metav1.ListOptions{FieldSelector: s}`.
- `fields.OneTermEqualSelector(k, v)` and `fields.AndSelectors(...)` when you
  are building one in code rather than passing a flag through — stage 27 needs
  exactly that.

## Hints

<details><summary>Nudge</summary>

Same plumbing as the label selector, different `ListOptions` field. The
interesting work is understanding the error you get from an unsupported field.
</details>

<details><summary>Approach</summary>

Pass the flag through and let the server validate. Try
`--field-selector spec.containers[0].image=nginx` once to see the refusal — it
is a much better error than a wrong answer.
</details>

## Further reading

- [Field selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/)
