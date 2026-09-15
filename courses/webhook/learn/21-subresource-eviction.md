---
title: An eviction is not a delete
concepts: [subresources, eviction, drain, rules, lookup]
---

## Core concept

Your rule says `resources: ["pods"]`, and for twenty stages that has been
enough. It does not cover an eviction.

Eviction is its own resource. `kubectl drain` does not delete the pods on a
node; it creates an `Eviction` object on each pod's `pods/eviction`
subresource, and the API server turns that into a delete on your behalf. A
webhook registered on `pods` is never consulted, because nothing was created,
updated or deleted on `pods` — something was created on `pods/eviction`.

So a webhook that carefully refuses to let a pod go is walked straight past by
a drain, which is the one moment it was written for. The fix is to say the
subresource out loud:

```yaml
rules:
  - operations: ["CREATE"]
    apiGroups: [""]
    apiVersions: ["v1"]
    resources: ["pods/eviction"]
    scope: Namespaced
```

Note the operation. An eviction is a **CREATE**, even though what it achieves
is a deletion. Registering `DELETE` here catches nothing.

Now the second half, which is where a handler written for the last twenty
stages quietly breaks. The request that arrives names `pods` as its resource
and `eviction` as its subresource, and `request.object` holds an `Eviction` —
not the pod. An `Eviction` carries a name, a namespace and some delete options.
It carries no labels, no spec, no containers.

Unmarshal that into a `corev1.Pod` and Go gives you an empty pod rather than an
error, because the field names simply do not match. Every eviction then looks
like a pod with no labels, and a rule that refuses unlabelled pods starts
refusing every eviction in the namespace. The object under review is not the
object being acted on, so the verdict needs a lookup: take `request.name` and
go read the pod.

## Go APIs

- `Resources: []string{"pods/eviction"}` in the rule; the subresource is part
  of the resource string, not a field of its own.
- `req.SubResource` is `"eviction"` while `req.Resource.Resource` is still
  `"pods"` — a check on the resource alone cannot tell them apart.
- `cs.CoreV1().Pods(ns).Get(ctx, req.Name, metav1.GetOptions{})` to fetch what
  the payload left out.
- `cs.PolicyV1().Evictions(ns).Evict(ctx, &policyv1.Eviction{...})` is what the
  other side of this looks like.

## Hints

<details><summary>Nudge</summary>

Two things are wrong, and the second only shows up once the first is fixed.
Until the rule names the subresource nothing reaches you at all; once it does,
the thing that reaches you is not a pod.
</details>

<details><summary>Approach</summary>

Add a second entry to `rules` rather than widening the first — the operations
differ, and an eviction is CREATE only. Then branch on `req.SubResource` before
you unmarshal anything, and keep the pod-shaped path exactly as it was.
</details>

## Further reading

- [API-initiated eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/api-eviction/)
- [Matching requests: rules](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-rules)
