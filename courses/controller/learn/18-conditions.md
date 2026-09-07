---
title: Say what Ready means
concepts: [conditions, reason and message, lastTransitionTime, list-map keys]
---

## Core concept

A condition is a yes/no question with a reason attached, and the reason is the
useful half. "Not ready" is a status light. "Not ready because 0 of 3 replicas
are available" is a diagnosis, and it is the difference between a person
reading `kubectl describe` and a person reading your source.

The conventions are worth following exactly, because tooling relies on them:

- `status` is the string `"True"`, `"False"` or `"Unknown"` — not a boolean,
  because "I do not know yet" is a real answer.
- `reason` is a short CamelCase token meant for machines; `message` is the
  sentence for people.
- `lastTransitionTime` changes only when `status` changes, so it means "since
  when". Rewriting it every pass destroys the only interesting thing about it.

Declaring the list as a **map keyed by type** matters too: the API server then
treats two writers setting different condition types as touching different
fields, rather than as two people fighting over one list.

## Go APIs

- `x-kubernetes-list-type: map` with `x-kubernetes-list-map-keys: [type]` in the
  CRD schema.
- `unstructured.NestedSlice` / `SetNestedSlice` for the conditions list.
- With typed APIs this is `meta.SetStatusCondition` from
  `apimachinery/pkg/api/meta`, which does the transition-time handling for you.
- An `additionalPrinterColumn` with jsonPath
  `.status.conditions[?(@.type=="Ready")].status`.

## Hints

<details><summary>Nudge</summary>

Read the existing condition before writing the new one; you need its old
`status` to decide whether the transition time moves.
</details>

<details><summary>Approach</summary>

Rebuild the list rather than mutating it in place: copy every condition whose
type is not the one you are setting, then append yours.
</details>

<details><summary>Implementation</summary>

```go
changed := time.Now().UTC().Format(time.RFC3339)
for _, raw := range conditions {
    cond, _ := raw.(map[string]any)
    if cond["type"] == "Ready" && cond["status"] == status {
        changed, _ = cond["lastTransitionTime"].(string) // unchanged: keep "since when"
    }
}
```
</details>

## Further reading

- [API conventions: typical status properties](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties)
- [meta.SetStatusCondition](https://pkg.go.dev/k8s.io/apimachinery/pkg/api/meta#SetStatusCondition)
