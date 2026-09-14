---
title: Never gate the namespace you run in
concepts: [self-exclusion, deadlock, bootstrap, namespaceselector, recovery]
---

## Core concept

Stage 7 narrowed this webhook to one namespace. This stage is about the one
namespace it must never contain: its own.

Picture the webhook running as a pod, gating pods, in the namespace it gates.
It dies — a node drains, an image is rolled, a deployment is updated. Something
tries to create the replacement pod. The API server calls the webhook to ask
whether that pod may be admitted, and the webhook that would answer is the one
being replaced. Under `failurePolicy: Fail` the replacement is refused, so the
webhook stays down, so the next attempt is refused too.

Nothing about that state is transient. The mechanism that normally recovers a
crashed workload is the mechanism the webhook has switched off, and it switched
it off for itself. The escape is the same one as for `kube-system`, a scope
smaller: exclude your own namespace from your own selector.

```yaml
namespaceSelector:
  matchExpressions:
    - key: kubernetes.io/metadata.name
      operator: NotIn
      values: ["byok8s-system"]
```

`matchExpressions` entries are ANDed, so the `In` from stage 7 and a `NotIn`
here compose without fighting: policed namespaces, minus this one.

Excluding yourself is not a hole in the policy. A webhook cannot meaningfully
judge its own bootstrap — at the moment the decision matters most there is
nobody alive to make it — and pretending otherwise buys a rule that holds right
up until the first time it is needed.

## Go APIs

- `metav1.LabelSelectorOpNotIn`, alongside the `In` requirement from stage 7.
- The namespace a pod runs in comes from the downward API, usually as a
  `POD_NAMESPACE` environment variable; running on the host, `cc.Namespace()`
  is the same answer.
- `admissionregistrationv1.Fail` is what makes the deadlock permanent rather
  than merely embarrassing.

## Hints

<details><summary>Nudge</summary>

You already read your own namespace at startup for stage 7. That is the value
to exclude — no new lookup is needed.
</details>

<details><summary>Approach</summary>

Add a second `matchExpressions` entry rather than a second selector. Both
requirements must hold, which is exactly the meaning you want: the namespaces
this webhook polices, except its own.
</details>

## Further reading

- [Avoiding operating on the kube-system namespace](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#avoiding-operating-on-the-kube-system-namespace)
- [Matching requests: namespaceSelector](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-namespaceselector)
