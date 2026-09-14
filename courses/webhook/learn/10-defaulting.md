---
title: Fill in what was left out
concepts: [defaulting, mutating before validating, admission order, drift]
---

## Core concept

Defaulting is mutation with a policy attached: instead of refusing an object
for what it lacks, you supply it.

The reason it works at all is the order admission runs in. Mutating webhooks go
first, then schema validation, then validating webhooks. So a default written
by your mutating webhook is already present by the time your validating webhook
looks, and the rule never sees the gap it filled.

That ordering is also a loaded gun. Default a field your own policy refuses on
and the policy stops meaning anything: the pod stage 4 exists to reject would
sail straight through, defaulted into compliance by the same program that was
supposed to judge it. The `owner` label stays untouched here for exactly that
reason.

That is also the design question. "Reject, or fill in?" is not a technical
choice:

- **Reject** when only the author knows the right answer. Defaulting an
  `owner` label to `unknown` produces objects nobody owns, which is worse than
  the error you avoided.
- **Default** when there is an obviously correct value and the alternative is
  making everyone type it — a resource limit, a scheduler name, a label your
  tooling needs.

This stage defaults the first kind: a container that requests no CPU gets
`100m`. Nobody meant to leave it out, every scheduler wants it, and no rule in
this program rejects a pod for it.

Two properties keep a defaulter from becoming a mystery.

**It must be idempotent.** Apply it to its own output and nothing more should
change. This is not optional politeness: reinvocation can call you twice for
the same object.

**Only fill what is absent.** A defaulter that overwrites a value the user set
is not defaulting, it is enforcing — and it is the thing people mean when they
say the cluster is fighting them. Check for the empty case explicitly rather
than assigning unconditionally.

Say what you did, too. An annotation recording that you defaulted a field turns
"why does my pod have this label" into something the user can answer without
reading your source.

## Go APIs

- `MutatingWebhookConfiguration` with its own rule, registered alongside the
  validating one.
- `/spec/containers/0/resources/requests` — a pointer into an array, by index.
  Every container is its own path, so a pod with two of them needs two ops.
- `review.Request.Operation == admissionv1.Create`. Resources are settled when
  a pod is created: patch them on an update and the API server refuses the
  whole request, naming a field the user never touched.
- A JSON Patch built only from the fields that are empty in the incoming
  object.
- `pod.Annotations` — the place to record that a value was supplied rather than
  given.

## Hints

<details><summary>Nudge</summary>

Build the patch from what is missing, not from what you want the object to
look like. If nothing is missing, send no patch at all — an empty patch list is
still a patch the API server has to apply.
</details>

<details><summary>Approach</summary>

Both webhooks now judge the same field. Keep them consistent by deciding the
default in one function and calling it from the mutating path only; the
validating path keeps asking the question it always asked.
</details>

## Further reading

- [Admission control phases](https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#what-does-each-admission-controller-do)
- [Mutating webhooks: idempotence](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#side-effects)
