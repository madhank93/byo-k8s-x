---
title: Print the object as YAML
concepts: [output formats, serialization]
---

## What this stage teaches

Same object, different serializer — and the way you get there matters. Handing
a Go struct to a generic YAML library emits *Go* field names (`ObjectMeta`,
`CreationTimestamp`) because those libraries read `yaml` struct tags, which the
Kubernetes API types do not have. The API's field names live in its `json`
tags.

So the route is object → JSON → YAML. `sigs.k8s.io/yaml` exists precisely to do
that: it marshals through JSON so the tags that matter are the ones honoured.
Every Kubernetes project uses it for this reason, and the bug it prevents —
YAML output that no `kubectl apply` will accept — is a memorable one to have
seen coming.

## Go you'll reach for

- `sigs.k8s.io/yaml` — `JSONToYAML(data)`, or `Marshal(obj)` which converts
  through JSON for you.
- YAML output needs no trailing newline of your own; use `fmt.Print`.

## Hints

<details><summary>Nudge</summary>

You already produce the correct JSON in stage 8. This stage is one more
conversion, not a second rendering path.
</details>

<details><summary>Approach</summary>

Marshal to JSON exactly as before, then `yaml.JSONToYAML(data)`. If you reach
for `gopkg.in/yaml.v3` here, look closely at the field names in the output.
</details>

## Going deeper

- [sigs.k8s.io/yaml](https://pkg.go.dev/sigs.k8s.io/yaml)
