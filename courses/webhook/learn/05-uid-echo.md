---
title: Echo the UID, or the answer is discarded
concepts: [uid, error handling, http status, failure policy]
---

## Core concept

Your program has two ways to answer, and only one of them is admission.

The first is an `AdmissionReview` with a verdict in it: 200, `allowed` set, the
request's `uid` echoed back. The API server reads it and acts on it.

The second is everything else — a 400 because the body would not decode, a 500
from a nil dereference, a connection closed halfway. None of those are
verdicts. The API server cannot tell a bug from a rejection, so it does not
try: the call has failed, and what happens next is decided by `failurePolicy`.
Under `Ignore`, the object you meant to refuse is admitted. Under `Fail`,
objects you would have allowed are refused, including the ones someone needs to
fix your webhook.

The `uid` is the same story in miniature. The API server matches an answer to
its question by uid; a reply carrying the wrong one, or none, is dropped as
though the call never returned. And because it is dropped rather than
rejected, the symptom is a timeout several seconds later, in a component that
is not yours.

So the rule for the rest of this course: **there is exactly one reply shape.**
Whatever goes wrong — a body that will not decode, an object that is not the
kind you expected, a panic you recovered — you answer 200 with an
`AdmissionReview`, the request's uid, and `allowed: false` with a message that
says what happened. A webhook that returns an HTTP error under load is a
webhook that stops being a policy and starts being an outage.

The one case you cannot answer is a request whose uid you never learned,
because the body itself was unreadable. That is the only place a 400 is
honest.

## Go APIs

- `admissionv1.AdmissionResponse{UID: review.Request.UID, Allowed: false, Result: &metav1.Status{Message: …}}`
  built once, from every path.
- `defer func() { if r := recover(); r != nil { … } }()` in the handler, so a
  panic becomes a verdict rather than a closed connection.
- `review.Request.Kind` and `review.Request.Resource` — what you were actually
  asked about, when it is not what you expected.

## Hints

<details><summary>Nudge</summary>

Write one small function that turns a uid and a message into the reply, and
call it from every branch. The bug this stage is about only exists when there
are two places that build a response.
</details>

## Further reading

- [Response](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#response)
- [Failure policy](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#failure-policy)
