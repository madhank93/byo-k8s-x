---
title: Read an AdmissionReview, answer with one
concepts: [admissionreview, uid, allowed, request and response]
---

## Core concept

The payload of admission is one kind, `AdmissionReview`, and it travels in both
directions. The API server POSTs one with `.request` filled in; you reply with
one with `.response` filled in. Same apiVersion, same kind, different half
populated.

Inside `.request` is everything you need to judge: the `operation`, the
`resource` being written, the `namespace`, the `object` as raw JSON, and — for
an update — the `oldObject`. Inside `.response` goes your verdict: `allowed`,
and the `uid` you were asked with.

The `uid` is the part people lose. The API server may have several reviews in
flight and matches the answer to the question by uid; a response that carries
the wrong one, or none, is discarded as if you had never replied. It fails as
a timeout, not as an error, which is why it is worth getting right before there
is anything interesting to decide.

Two smaller traps live here too. The reply must be JSON with the `apiVersion`
and `kind` set explicitly — Go's encoder does not fill in `TypeMeta` for you,
and a body without a kind is unreadable to the API server. And a non-200 status
is not a rejection: it is a broken webhook, handled by your `failurePolicy`.
Rejecting is something you do *inside* a 200 response, in stage 4.

At this stage every answer is `allowed: true`. Nothing is registered yet, so
the only caller is the harness — which is deliberate. The wire format is easier
to get right while the API server is not yet the one asking.

## Go APIs

- `admissionv1.AdmissionReview`, with `.Request *AdmissionRequest` and
  `.Response *AdmissionResponse`.
- `metav1.TypeMeta{APIVersion: admissionv1.SchemeGroupVersion.String(), Kind: "AdmissionReview"}`.
- `json.NewDecoder(r.Body).Decode(&review)` and `json.NewEncoder(w).Encode(reply)`.
- `review.Request.Object.Raw` — a `runtime.RawExtension`, the object as it
  arrived.

## Hints

<details><summary>Nudge</summary>

Build a fresh `AdmissionReview` for the reply rather than mutating the one you
decoded. Sending the request's `.request` back is harmless but confusing, and
it is bigger on the wire than the answer needs to be.
</details>

<details><summary>Approach</summary>

Decode, reject an empty `.request` with a 400, then respond with
`&AdmissionResponse{UID: review.Request.UID, Allowed: true}` and set `TypeMeta`
by hand.
</details>

## Further reading

- [Request and response](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#request)
- [AdmissionReview v1](https://kubernetes.io/docs/reference/kubernetes-api/extend-resources/admission-review-v1/)
