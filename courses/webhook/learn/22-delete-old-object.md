---
title: Judge what is being taken away
concepts: [delete, oldobject, operations, admissionreview]
---

## Core concept

Stage 21 stopped a drain from taking a protected pod. It left the front door
open: `kubectl delete pod` still works, because the rule covers CREATE and
UPDATE and an eviction, and a delete is none of those.

Adding `DELETE` to the operations is the easy half:

```yaml
operations: ["CREATE", "UPDATE", "DELETE"]
```

The half that catches people is what arrives when that rule matches. Every
request so far has carried the object you were judging in `request.object`. A
delete carries nothing there — there is no incoming object, because nothing is
being written. The pod being removed is in **`request.oldObject`**.

Stage 6 introduced `oldObject` as the version being replaced on an update, with
both fields populated. On a delete only the old one is. `request.object`
arrives as `null`, `Raw` is left empty, and `json.Unmarshal` on it returns
`unexpected end of JSON input`. What happens next depends on your handler:

- If it refuses on a decode error, as it should, every delete is refused —
  protected or not — with a message about a pod it could not read.
- If the error is dropped anywhere on the way (a `_ =` on a metadata read is
  enough), Go hands back a zero-valued pod: no labels, no name, no spec. Every
  delete then looks unprotected and goes through.

Either way the verdict no longer depends on the pod being deleted. It is the
same shape as the eviction trap: the object in front of you is not the pod you
are being asked about.

## Go APIs

- `admissionregistrationv1.Delete` in the rule's `Operations`.
- `req.OldObject.Raw` on a delete; `req.Object.Raw` is empty, and
  `json.Unmarshal` on it fails with `unexpected end of JSON input`.
- `req.Operation == admissionv1.Delete` is how you know which field to read.
- `req.Name` is populated on a delete, so the name is available even before the
  object is.

## Hints

<details><summary>Nudge</summary>

Nothing is being written, so there is no new object to look at. The thing you
are being asked about is the thing that is already there.
</details>

<details><summary>Approach</summary>

Branch on the operation before you unmarshal, rather than inside the rule. The
create and update path already works and should keep reading `object`; the
delete path reads `oldObject` and is otherwise the same check.
</details>

## Further reading

- [Request: AdmissionReview object](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#request)
- [Matching requests: operations](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-rules)
