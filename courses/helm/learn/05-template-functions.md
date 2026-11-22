---
title: Give templates a toolbox
concepts: [function maps, sprig, toYaml, nindent, required]
---

## Core concept

`text/template` ships with almost no functions — `len`, `index`, `printf`,
comparison and little else. Charts lean on dozens more:

```
name: {{ .Values.name | lower | trunc 63 | trimSuffix "-" }}
labels:
  {{- toYaml .Values.labels | nindent 4 }}
port: {{ .Values.port | quote }}
tag: {{ .Values.image.tag | default "latest" | quote }}
```

A pipe passes the left side as the **last** argument of the function on the
right, so `x | trunc 63` is `trunc 63 x`. That ordering is why every
function a chart uses takes its subject last.

Helm's function map is built in two layers:

1. **Sprig** (`github.com/Masterminds/sprig/v3`) supplies the general ones:
   strings (`upper`, `trunc`, `trimSuffix`, `quote`, `squote`), lists
   (`join`, `sortAlpha`), `default`, `add`, `b64enc`, `toJson`, and a hundred
   more. Helm uses sprig rather than its own versions, so using it is how your
   output agrees with Helm's byte for byte — `default` treating `""`, `0` and
   empty lists as unset is sprig's rule, not Go's.
2. **Helm's own** additions on top. For this stage: `toYaml` and `required`.
   `include` and `tpl` arrive with named templates.

Helm also takes two functions *away*: `env` and `expandenv`. A chart is code
written by someone else, run on your machine; it should not be able to read
your environment into a manifest. A template calling `env` must fail to parse.

`toYaml` marshals with `sigs.k8s.io/yaml` and **drops the trailing
newline**, so that `nindent`/`indent` decide all the whitespace around it.

`required "message" value` fails the render with that message when the
value is missing or an empty string, and otherwise returns the value, so it
sits in a pipeline like any other function.

The function map has to be attached before any template is parsed: the
parser rejects a call to a function it does not know.

## Go APIs

- `sprig.TxtFuncMap()` returns a fresh `template.FuncMap` you can edit.
- `t.Funcs(fm)` on the root template, before the first `Parse`.
- A template function may return `(value, error)`; a non-nil error stops
  execution, and Go's error carries the message and the file.

## Hints

<details><summary>Nudge</summary>

Print `toYaml` of a two-key map and look at the last byte.
</details>

<details><summary>Approach</summary>

Build the map once: take sprig's, delete `env` and `expandenv`, set
`toYaml` and `required`, and pass it to `Funcs` where you create the
template set.
</details>

## Further reading

- [Template functions and pipelines](https://helm.sh/docs/chart_template_guide/functions_and_pipelines/)
- [Template function list](https://helm.sh/docs/chart_template_guide/function_list/)
- [Sprig function documentation](https://masterminds.github.io/sprig/)
