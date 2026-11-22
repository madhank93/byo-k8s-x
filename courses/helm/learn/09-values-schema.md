---
title: Refuse values that do not fit
concepts: [JSON Schema, values.schema.json, validation]
---

## Core concept

A chart's values are its interface, and nothing so far stops a user from
passing `replicas: two` and getting a manifest the API server rejects — or
worse, accepts. A chart can ship a **`values.schema.json`** next to
`values.yaml`, a JSON Schema the merged values must satisfy:

```json
{
  "type": "object",
  "required": ["replicas"],
  "properties": {
    "replicas": {"type": "integer", "minimum": 1},
    "image": {
      "type": "object",
      "properties": {"pullPolicy": {"enum": ["Always", "IfNotPresent"]}}
    }
  }
}
```

Helm checks before it renders anything, and the rules for *which* values
meet *which* schema follow from the earlier stages:

- What is checked is the **merged** values — defaults, `-f`, `--set`, with
  nulls applied — so `--set replicas=null` fails a schema that requires
  `replicas`.
- Each chart is checked against its own schema with its own slice:
  `charts/db/values.schema.json` sees what `db`'s templates would see as
  `.Values`, globals included.
- A subchart switched off by a condition is not checked: it is not part of
  the render.
- Every failure is reported, chart by chart, and the command exits 1:

```
Error: values don't meet the specifications of the schema(s) in the following chart(s):
shop:
- at '/replicas': minimum: got 0, want 1
- at '/image/tag': got number, want string
db:
- at '/port': maximum: got 70,000, want 65,535
```

The location is a JSON Pointer into the values (`/image/tag`). The stage
grades the chart name and the pointer; the wording after it belongs to the
schema library.

## Go APIs

Helm 3.18 uses `github.com/santhosh-tekuri/jsonschema/v6`, and so can you —
the messages above are that library's:

- `jsonschema.UnmarshalJSON(r)` reads a schema document (keeping numbers
  exact).
- `c := jsonschema.NewCompiler()`, `c.AddResource(url, doc)`,
  `sch, err := c.Compile(url)`. Helm names the resource
  `file:///values.schema.json`.
- `sch.Validate(values)` takes the values as Go data and returns a
  `*jsonschema.ValidationError` whose `Error()` lists every failure as
  `- at '<pointer>': <message>`, under a first line naming the resource,
  which Helm strips.

YAML numbers arrive as `float64`; JSON Schema's `integer` accepts any
number with no fractional part, so `replicas: 2` from `values.yaml` passes
and `2.5` does not.

## Hints

<details><summary>Nudge</summary>

Load `values.schema.json` beside `values.yaml`, and validate after the
subcharts are decided but before the first template runs.
</details>

<details><summary>Approach</summary>

Walk the chart tree the way rendering does: validate the chart's schema
against its values, then each remaining subchart's against
`values[<sub>]`. Collect the failures into one error.
</details>

## Further reading

- [Charts: schema files](https://helm.sh/docs/topics/charts/#schema-files)
- [JSON Schema: getting started](https://json-schema.org/learn/getting-started-step-by-step)
- [santhosh-tekuri/jsonschema](https://pkg.go.dev/github.com/santhosh-tekuri/jsonschema/v6)
