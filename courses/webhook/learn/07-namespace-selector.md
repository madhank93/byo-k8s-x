---
title: Leave other namespaces alone
concepts: [namespaceselector, kube-system, labels, deadlock]
---

## Core concept

A rule says *what kind* of object you judge. A `namespaceSelector` says *where*
— and it is the field that decides whether a mistake in your webhook is an
inconvenience or an outage.

The selector matches labels on the **namespace**, not on the object. Every
namespace carries `kubernetes.io/metadata.name` automatically, so selecting one
namespace by name needs no preparation:

```yaml
namespaceSelector:
  matchExpressions:
    - key: kubernetes.io/metadata.name
      operator: In
      values: ["team-a"]
```

The reason to reach for this before you think you need it is `kube-system`. A
webhook that gates it can stop the cluster from repairing itself: the API
server calls you, you are down, and the pods that would bring you back are the
ones being refused. With `failurePolicy: Fail` that state is self-sustaining,
and the way out is deleting your own webhook configuration by hand — which is
also an API call, and one of the few that admission cannot block.

So the shape to internalise is: **exclude the namespaces that keep the cluster
alive, and include only the ones you are actually policing.** `NotIn` for the
first, `In` for the second, and the two compose.

A caveat worth knowing: for cluster-scoped objects the selector matches nothing
and the request goes through unfiltered, so a rule that mixes scopes is not
narrowed by the selector you added for the namespaced half.

## Go APIs

- `admissionregistrationv1.ValidatingWebhook{NamespaceSelector: &metav1.LabelSelector{…}}`.
- `metav1.LabelSelectorRequirement{Key: "kubernetes.io/metadata.name", Operator: metav1.LabelSelectorOpIn, Values: []string{ns}}`.
- The namespace your kubeconfig selects — `cc.Namespace()` from the client
  config you already load.

## Hints

<details><summary>Nudge</summary>

You already know which namespace to care about: the one your kubeconfig
context selects. Read it once at startup and put it in the selector.
</details>

<details><summary>Approach</summary>

`matchExpressions` with `In` on `kubernetes.io/metadata.name` needs no labelling
step — the API server maintains that label on every namespace.
</details>

## Further reading

- [Matching requests: namespaceSelector](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-namespaceselector)
- [Avoiding operating on the kube-system namespace](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#avoiding-operating-on-the-kube-system-namespace)
