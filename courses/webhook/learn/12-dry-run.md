---
title: Honour a dry run, change nothing
concepts: [dryrun, side effects, sideeffectclass, idempotence]
---

## Core concept

`kubectl apply --dry-run=server` asks the cluster what *would* happen. The
request runs the whole admission chain, including your webhook, and is thrown
away instead of being stored.

Your program is told, in `request.dryRun`. What it means for you depends on
what your webhook does besides answering:

- If you only decide, nothing changes. Judge as usual and reply as usual.
- If you have side effects — writing a record, calling an external system,
  allocating something — you must skip them when `dryRun` is true. The request
  is hypothetical; your side effect would not be.

The verdict itself must not change. A webhook that allows in a dry run and
rejects in the real one makes `--dry-run` a lie, which is worse than not
supporting it.

This is what `sideEffects` in the registration is about, and the values are a
contract rather than documentation:

- `None` — calling the webhook changes nothing outside the request. Dry-run
  requests are sent to you.
- `NoneOnDryRun` — you have side effects, but you skip them when `dryRun` is
  set. Dry-run requests are sent to you.
- `Some` / `Unknown` — you have side effects you cannot suppress. **Dry-run
  requests are refused outright**, so `--dry-run=server` stops working for
  every object your rules match. Both are deprecated for exactly that reason.

So the honest declaration is `None` if you truly only decide, and
`NoneOnDryRun` the moment you do anything else — and then the code has to
actually honour it.

## Go APIs

- `review.Request.DryRun` — a `*bool`, nil meaning false.
- `admissionregistrationv1.SideEffectClassNone` and
  `SideEffectClassNoneOnDryRun` in the registration.
- `kubectl apply --dry-run=server` and `metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}`
  from a client.

## Hints

<details><summary>Nudge</summary>

`DryRun` is a pointer. Dereferencing it without a nil check turns every
ordinary request into a panic — which stage 5 taught you to survive, but not to
cause.
</details>

<details><summary>Approach</summary>

Keep the decision and the side effect apart in the code. Then honouring a dry
run is one branch around the second half, and the verdict is provably the same
either way.
</details>

## Further reading

- [Side effects](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#side-effects)
- [Dry-run](https://kubernetes.io/docs/reference/using-api/api-concepts/#dry-run)
