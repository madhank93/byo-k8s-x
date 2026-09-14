---
title: Narrow by label on the object itself
concepts: [objectselector, labels, opt-in policy, matchexpressions]
---

## Core concept

`namespaceSelector` filters by where an object lives. `objectSelector` filters
by what it *is* — the labels on the object being admitted.

That turns a policy from something imposed on a namespace into something an
object opts into, or out of:

```yaml
objectSelector:
  matchExpressions:
    - key: byok8s.dev/skip
      operator: DoesNotExist
```

Which reads: judge everything except objects that asked to be skipped. Flip the
operator to `Exists` and you have the opposite — a webhook that only touches
objects that opted in, which is how sidecar injectors are usually wired.

Filtering here rather than in your handler is not only tidier. A request that
the selector excludes never leaves the API server, so it costs no round trip,
cannot time out, and is unaffected by your webhook being down. Every request
you can decide *not* to see is one that can never hurt the cluster.

Two sharp edges.

**The label must be on the object you are asked about, which is not always the
object you have in mind.** A Deployment's labels are not its pods' labels; if
your rule is about pods, the selector reads the pod template's labels as they
appear on the created pod.

**A mutating webhook can change the labels the selector matched on.** The
selector is evaluated against the object as it arrives at that webhook, so an
earlier mutation can pull an object into, or out of, your webhook's view.

## Go APIs

- `admissionregistrationv1.ValidatingWebhook{ObjectSelector: &metav1.LabelSelector{…}}`.
- `metav1.LabelSelectorOpExists`, `metav1.LabelSelectorOpDoesNotExist`,
  `metav1.LabelSelectorOpIn`, `metav1.LabelSelectorOpNotIn`.
- `metav1.LabelSelector{MatchLabels: map[string]string{…}}` for the simple case.

## Hints

<details><summary>Nudge</summary>

An empty selector matches everything, and a nil one does too. If you want to
exclude something, say so with `DoesNotExist` rather than leaving the field
out.
</details>

## Further reading

- [Matching requests: objectSelector](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-objectselector)
- [Labels and selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
