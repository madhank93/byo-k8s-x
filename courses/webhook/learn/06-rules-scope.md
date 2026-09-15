---
title: Ask only for the objects you judge
concepts: [rules, operations, scope, oldobject, blast radius]
---

## Core concept

A `rule` is the blast radius of your webhook, written down. Four axes decide
what the API server diverts to you:

```yaml
rules:
  - apiGroups:   [""]
    apiVersions: ["v1"]
    resources:   ["pods"]
    operations:  ["CREATE", "UPDATE"]
    scope:       Namespaced
```

Every request that matches waits for your answer. Every request that does not
match never reaches you, no matter what your code says — which is why a rule
that is too *narrow* looks like a bug in your logic, and a rule that is too
*wide* looks like an outage.

Wildcards are where clusters get hurt. `resources: ["*"]` with
`operations: ["*"]` means your program is now on the write path for every
object in the cluster, including the ones the control plane needs to make
progress. If your code assumes the object is a pod, it is now judging Nodes and
Leases with a pod's rules.

`CREATE` alone is the other half of the lesson. It admits a correct object and
then lets anyone edit it into an incorrect one — the rule was checked at birth
and never again. Adding `UPDATE` closes that, and brings a second field with
it: `oldObject`, the version being replaced. For "the label must be present"
you can ignore it. For "the label must not change once set" you cannot.

Two smaller things live on this axis. `scope` distinguishes namespaced objects
from cluster-scoped ones — the pods you judge are `Namespaced`, and saying so
keeps a rule from quietly matching a cluster-scoped kind. And subresources are
separate resources: `pods` does not match `pods/exec` or `pods/status`, so a
rule that means to police exec has to name it.

Even with a tight rule, judge defensively. Rules are data in a cluster you do
not control; something else can widen yours. A webhook that answers "not mine,
allowed" for a resource it does not recognise is one that cannot take the
cluster down when that happens.

## Go APIs

- `admissionregistrationv1.Rule{Scope: &scope}` with
  `admissionregistrationv1.NamespacedScope`.
- `[]OperationType{Create, Update}` in `RuleWithOperations`.
- `review.Request.Resource` — the GroupVersionResource you were actually asked
  about.
- `review.Request.OldObject.Raw`, populated on UPDATE and empty on CREATE.

## Hints

<details><summary>Nudge</summary>

Once UPDATE is in the rule, every write to a pod reaches you — including the
status updates the kubelet makes constantly. Make sure your verdict does not
depend on fields the kubelet is allowed to change.
</details>

<details><summary>Approach</summary>

Guard on `review.Request.Resource.Resource` before decoding. If it is not
`pods`, answer allowed and say nothing: you are not the right webhook for it.
</details>

## Further reading

- [Matching requests: rules](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-rules)
- [Best practices: avoiding deadlocks](https://kubernetes.io/docs/concepts/cluster-administration/admission-webhooks-good-practices/)
