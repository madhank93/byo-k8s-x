---
title: Filter with CEL before the request reaches you
concepts: [matchconditions, cel, filtering, latency]
---

## Core concept

Rules and selectors filter by shape: which resource, which namespace, which
labels. `matchConditions` filter by **content**, using CEL expressions the API
server evaluates before it calls you:

```yaml
matchConditions:
  - name: exclude-kubelet
    expression: "request.userInfo.username != 'system:node:worker'"
  - name: only-real-pods
    expression: "object.spec.containers.size() > 0"
```

Every condition must be true for the request to be sent. They run after the
rules and selectors match, and they are the last chance to not be called.

The reason to push a check up here rather than doing it in your handler is that
the checks are not equivalent. A request excluded by a match condition costs
nothing: no round trip, no timeout risk, and — crucially — no dependence on
your webhook being up. A request your handler allows because it recognised the
caller costs a network hop and a deadline, every time.

What CEL can see is the request: `object`, `oldObject`, `request` (the whole
AdmissionRequest, including `userInfo` and `dryRun`), plus `namespaceObject`
and `authorizer`. What it cannot do is call out to anything or change the
object; it is a predicate, evaluated in bounded time.

The one that saves people is excluding trusted system identities. A webhook
that judges the control plane's own writes is a webhook that can wedge a
cluster; one line of CEL keeps those requests from ever arriving.

Failures are conservative: an expression that errors — a missing field, a type
mismatch — is treated as a failed call and handed to your `failurePolicy`.
`optional(object.metadata.labels)` and `has()` are how you avoid writing an
outage into a filter.

## Go APIs

- `admissionregistrationv1.MatchCondition{Name: …, Expression: …}` on the
  webhook.
- CEL variables: `object`, `oldObject`, `request`, `namespaceObject`,
  `authorizer`.
- `has(object.metadata.labels)` before indexing a map that may be absent.

## What this stage checks

- Both registrations carry at least one match condition, each with a name and
  an expression — the validating one and the mutating one. An exemption that
  covers one and not the other still pays for the round trip it was meant to
  avoid.
- Writes from the user `byok8s-exempt` never reach the handler: a pod that
  your policy would refuse is created anyway, and nothing your program logs on
  entry — in either handler — ever mentions it.
- That exclusion happens in the API server, not in your code. A handler that
  recognises the caller and allows the pod still logs the request, and this
  stage fails it.
- Every other identity is judged exactly as before — a pod with no `owner`
  label is still refused.

## Hints

<details><summary>Nudge</summary>

Name each condition something a person reading an audit event would
understand — the name is what appears when a condition rejects or errors.
</details>

<details><summary>Approach</summary>

Move one check out of your handler and into a condition, then confirm the
handler never sees those requests: the log line you print on entry stops
appearing.
</details>

## Further reading

- [Matching requests: matchConditions](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#matching-requests-matchconditions)
- [CEL in Kubernetes](https://kubernetes.io/docs/reference/using-api/cel/)
