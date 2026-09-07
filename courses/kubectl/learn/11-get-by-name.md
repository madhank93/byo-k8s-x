---
title: Ask for one object
concepts: [rest api, errors, api semantics]
---

## Core concept

Get and List are different endpoints with different failure modes, and the
difference is the lesson. A List of an empty namespace succeeds and returns
nothing. A Get of a name that does not exist returns **404 NotFound** — an
error, not an empty result.

Collapsing the two is a real bug: "there is no such pod" and "there are no
pods" are different answers, and a script that cannot tell them apart deletes
the wrong thing. So let the NotFound surface as a non-zero exit.

To keep one printing path, wrap the single object in a list of one. That way
`-o json`, the table and everything after it stay unchanged.

## Go APIs

- `ri.Get(ctx, name, metav1.GetOptions{})`.
- `apierrors.IsNotFound(err)` from `k8s.io/apimachinery/pkg/api/errors`, when
  you want to say something better than the raw message.

## Hints

<details><summary>Nudge</summary>

Choose between Get and List on whether a name was given, then make both
branches return the same type.
</details>

## Further reading

- [apimachinery errors](https://pkg.go.dev/k8s.io/apimachinery/pkg/api/errors)
