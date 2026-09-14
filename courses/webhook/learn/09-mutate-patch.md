---
title: Change an object with a JSON patch
concepts: [mutatingwebhookconfiguration, jsonpatch, patchtype, base64]
---

## Core concept

A mutating webhook answers the same question with one extra field: not just
"may this be written", but "write this instead".

You do not send back a modified object. You send a **JSON Patch** (RFC 6902) —
a list of operations against the object you were given — base64-encoded, with
`patchType: JSONPatch` next to it:

```json
{"allowed": true,
 "patchType": "JSONPatch",
 "patch": "W3sib3AiOiJhZGQiLCJwYXRoIjoiL21ldGFkYXRhL2Fubm90YXRpb25zIiwidmFsdWUiOnt9fSx7Im9wIjoiYWRkIiwicGF0aCI6Ii9tZXRhZGF0YS9hbm5vdGF0aW9ucy9ieW9rOHMuZGV2fjFpbmplY3RlZCIsInZhbHVlIjoidHJ1ZSJ9XQ=="}
```

Patching rather than replacing is what keeps two webhooks from silently undoing
each other: each one describes its own change, and the API server applies them
in order.

The paths are where this goes wrong. A JSON Pointer is not a field path:

- **`/` inside a key is escaped as `~1`**, so the annotation
  `byok8s.dev/injected` is `/metadata/annotations/byok8s.dev~1injected`. A `~`
  is `~0`.
- **`add` into a map that does not exist fails.** If the object has no labels
  at all, `add /metadata/annotations/byok8s.dev~1injected` has nowhere to go — you first
  `add /metadata/labels` with an empty object, or add the whole map at once.
- **`/spec/containers/-` appends**, while `/spec/containers/0` replaces the
  first one.
- **`replace` on a missing path fails** where `add` would have created it.
  `add` is what you usually want, since it overwrites when the path exists.

And one rule that outranks all of them: **a mutating webhook must be
idempotent.** It can be called more than once for the same object — a later
webhook's change can cause yours to be reinvoked — so applying your patch to
its own output has to be a no-op. Appending a sidecar without first checking
whether it is already there is the classic way to get two of them.

Mutating webhooks are a separate registration kind,
`MutatingWebhookConfiguration`, and they run *before* validating ones. Your
program can serve both from one process: two paths, two registrations.

What to mark is a choice with consequences. This stage stamps every pod with
`byok8s.dev/injected: "true"` and leaves the `owner` label alone: a mutating
webhook that filled in `owner` would run *before* the validating one and quietly
answer the question stage 4 exists to ask.

## Go APIs

- `admissionregistrationv1.MutatingWebhookConfiguration` and `MutatingWebhook`.
- `admissionv1.PatchTypeJSONPatch` and `resp.PatchType = &pt`.
- `resp.Patch = jsonBytes` — the API server expects base64, and the JSON
  encoder does that for a `[]byte` field.
- `json.Marshal([]map[string]any{{"op": "add", "path": …, "value": …}})`.

## Hints

<details><summary>Nudge</summary>

`Patch` is a `[]byte`. Encoding it yourself with `base64.StdEncoding` double-
encodes it, and the API server reports the patch as unparseable.
</details>

<details><summary>Approach</summary>

Serve mutation on a second path — `/mutate` — and register it separately. Keep
`/validate` as it is: the two webhooks disagree about what they are for, and
one handler doing both gets confusing at the reinvocation stage.
</details>

<details><summary>Implementation</summary>

```go
patch := []map[string]any{}
if pod.Annotations == nil {
    patch = append(patch, map[string]any{"op": "add", "path": "/metadata/annotations", "value": map[string]string{}})
}
patch = append(patch, map[string]any{"op": "add", "path": "/metadata/annotations/byok8s.dev~1injected", "value": "true"})
```
</details>

## Further reading

- [Response: patch](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#response)
- [RFC 6902 — JavaScript Object Notation (JSON) Patch](https://datatracker.ietf.org/doc/html/rfc6902)
- [RFC 6901 — JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901)
