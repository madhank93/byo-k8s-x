---
title: Every version of the same thing
concepts: [matchpolicy, equivalent, requestkind, api-versions, crd-versions]
---

## Core concept

A rule names API versions. So far that has been harmless: pods have exactly
one. Most resources that live long enough grow a second — Deployments were
once served as both `apps/v1` and `extensions/v1beta1`, HorizontalPodAutoscalers
are served as `autoscaling/v1` and `v2`, and any CRD that has been through a
schema change serves its old version alongside the new. Every served version is
another way to write the same stored object.

This stage gives the program's allowlist a second one. `OwnerList` is now
served as `v1` and `v1beta1` — the same fields, `v1` the one stored — and the
webhook starts judging OwnerLists themselves, because a malformed allowlist
locks a namespace out as surely as a wrong rule: an empty list refuses every
pod, and an owner like `Platform Team` can never equal a label value.

The rule names `v1` only. What happens to a list written as `v1beta1` is
decided by **`matchPolicy`**:

- `Exact`: the rule matches only the versions it names. A `v1beta1` write never
  reaches you. Nothing fails; the bad list just lands.
- `Equivalent`: the API server knows the two are the same resource. It converts
  the object to the version your rule names and asks you anyway.

`Equivalent` is the default in `admissionregistration.k8s.io/v1`. The trap is
an explicit `Exact` — often carried over from the old v1beta1 admission API,
where it was the default, in a config nobody questioned since. Spell out
`Equivalent`, so the next reader does not have to wonder.

When a converted request arrives, two fields tell the story:

- `request.kind` and `request.resource`: the version the object is in — the one
  your rule named. Decode as this.
- `request.requestKind` and `request.requestResource`: what the client
  actually wrote. Log it; do not decode as it.

A write through `v1beta1` arrives with `kind` at `v1` and `requestKind` at
`v1beta1`.

One ordering detail. Installing the policy seeds an OwnerList, and the webhook
now judges OwnerLists — so seed before you register. A webhook that is
registered but not yet serving refuses the program's own write, and with a
`Fail` policy the program never gets as far as serving.

The course fixes the contract so the grader can check it: the OwnerList CRD
serves `v1` (stored) and `v1beta1` with the same schema, and the webhook
refuses an OwnerList with no owners or with any owner that is not a valid label
value. The type is the program's, so the program updates it when it lacks
`v1beta1`; the lists stay the namespace's.

## Go APIs

- `ValidatingWebhook.MatchPolicy`: a pointer to `admissionregistrationv1.Equivalent`.
- `req.Kind` and `req.RequestKind` (`metav1.GroupVersionKind`); `req.Resource`
  and `req.RequestResource`.
- `validation.IsValidLabelValue` from `k8s.io/apimachinery/pkg/util/validation`.
- A CRD's `spec.versions`: several may be `served`, exactly one is `storage`.

## Hints

<details><summary>Nudge</summary>

How many ways are there to write an OwnerList now, and how many of them does
your rule name?
</details>

<details><summary>Approach</summary>

Name `v1` in the rule, set `matchPolicy: Equivalent`, and judge the OwnerList
you are handed — it is already `v1`. Move the policy install ahead of the
webhook registration.
</details>

## Further reading

- [Matching requests: matchPolicy](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-matchpolicy)
- [Versions in CustomResourceDefinitions](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definition-versioning/)
