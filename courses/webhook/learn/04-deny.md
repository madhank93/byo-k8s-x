---
title: Reject what breaks the rule
concepts: [allowed, status message, forbidden, policy]
---

## Core concept

Everything so far said yes. A validating webhook earns its name by saying no,
and the mechanics of no are smaller than they look: `allowed: false`, plus a
`status` explaining why.

The rule this webhook enforces from here on: **every pod carries an `owner`
label**. A pod without one is refused; a pod with one is admitted unchanged.

That message is not decoration. It is the entire explanation the person
applying the object will ever see:

```
Error from server: admission webhook "pods.byok8s.dev" denied the request:
pod has no owner label
```

Nothing else is logged for them, nothing points at your program, and they
cannot read your code. A message like "validation failed" turns a two-second
fix into a support ticket, so write the message you would want at 3am: what was
wrong, and what would be right.

Two distinctions matter here.

**A rejection is a successful HTTP response.** Status 200, `allowed: false`.
Returning a 500 or timing out is not a stricter rejection — it is a broken
webhook, and what happens next is decided by your `failurePolicy`, not by you.

**The verdict is about the object, not the user.** Admission runs after
authorisation: by the time you are asked, the request is already permitted.
You are judging what is being written, which is why "this user may not do this"
belongs in RBAC and "this pod is missing something" belongs here.

You can also set `status.code`. It defaults to 403, which is nearly always what
you want — the object was understood and refused.

## Go APIs

- `admissionv1.AdmissionResponse{Allowed: false, Result: &metav1.Status{Message: …}}`.
- `metav1.Status{Code: http.StatusForbidden, Reason: metav1.StatusReasonForbidden}`
  when you want to be explicit.
- `json.Unmarshal(review.Request.Object.Raw, &pod)` with `corev1.Pod`, now that
  the decision depends on what is inside the object.

## Hints

<details><summary>Nudge</summary>

Decode the raw object into a `corev1.Pod` and look at its labels. Judging the
JSON as a string works right up until someone nests a matching key somewhere
else.
</details>

## Further reading

- [Response](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#response)
- [Status](https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/status/)
