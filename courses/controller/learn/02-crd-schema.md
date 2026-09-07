---
title: Give the kind a schema
concepts: [structural schema, openapi validation, defaulting, printer columns]
---

## Core concept

The schema is not documentation. It is the contract the API server enforces on
your behalf: a Website with `replicas: "two"` is refused before it is ever
stored, and a Website with no `replicas` at all comes back with `1`. Every
client — kubectl, another controller, yours — reads objects that already fit
the shape, which is why a controller can index into a spec without checking
every field first.

Defaulting is the part people underestimate. It means "unset" and "the default
value" are the same thing by the time anyone reads the object, so the
controller never has to encode the default a second time, and the default can
change in one place.

**additionalPrinterColumns** is the same idea for humans: `kubectl get
websites` shows what the resource is actually about instead of a name and an
age.

## Go APIs

- `spec.versions[].schema.openAPIV3Schema` — a **structural** schema: every
  field typed, `properties` under `type: object`, no free-form maps unless you
  ask for them explicitly.
- `required: ["image"]` for what cannot be omitted, `default:` for what can.
- `spec.versions[].additionalPrinterColumns` with a `jsonPath` each.

## Hints

<details><summary>Nudge</summary>

`unstructured` is JSON, and JSON has one number type. A `default` of `1` must
be written `int64(1)`, and a `minimum` of `0` must be `float64(0)` — the wrong
one panics on conversion rather than failing politely.
</details>

<details><summary>Approach</summary>

Put the schema in its own function. It is the largest literal in the program
and it will grow twice more in this course — once for status, once for
conditions.
</details>

<details><summary>Implementation</summary>

```go
"spec": map[string]any{
    "type":     "object",
    "required": []any{"image"},
    "properties": map[string]any{
        "image":    map[string]any{"type": "string"},
        "replicas": map[string]any{"type": "integer", "default": int64(1), "minimum": float64(0)},
        "host":     map[string]any{"type": "string"},
    },
},
```
</details>

## Further reading

- [Specifying a structural schema](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#specifying-a-structural-schema)
- [Defaulting](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#defaulting)
