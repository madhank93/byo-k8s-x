---
title: Define once, include anywhere
concepts: [define, template, include, tpl, template sets]
---

## Core concept

Every chart repeats itself: the same name, the same labels, on every
resource. Go templates name a piece of template with `define` and call it
with `template`:

```
{{- define "demo.fullname" -}}
{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end -}}

metadata:
  name: {{ template "demo.fullname" . }}
```

`template` is an *action*, not a function: it writes straight to the
output, so its result cannot be piped. That is why Helm adds **`include`**,
which runs a named template into a string and returns it:

```
  labels:
    {{- include "demo.labels" . | nindent 4 }}
  pair: {{ include "demo.pair" (dict "k" "color" "v" "blue") | quote }}
```

The second argument is the data the named template sees as `.` — usually
the top-level `.`, but any value will do.

Helm adds one more: **`tpl`** renders a *string* as a template. A values
file can then hold template text, `greeting: "hello from {{ .Release.Name }}"`,
and `{{ tpl .Values.greeting . }}` renders it. The string may call any named
template the chart defines, with `template` as well as `include`.

Three rules come from the single template set stage 3 built:

- **A define is visible everywhere.** It can live in `_helpers.tpl` or in
  an ordinary manifest file; every file sees every define.
- **When two files define the same name, Helm picks by parse order.** Helm
  parses the deepest paths first and, at the same depth, in reverse lexical
  order. A later parse replaces an earlier define, so the shallowest,
  lexically first file wins: `_a.tpl` over `_b.tpl` over `sub/_c.tpl`.
- **An undefined name is an error**, naming the template that was asked for.

## Go APIs

- A `FuncMap` function can close over the `*template.Template` it is added
  to: `t.ExecuteTemplate(&buf, name, data)` from inside the function runs a
  define from the same set. The function exists before the set is parsed;
  it only needs the set when it is called.
- `t.Clone()` copies the set, defines and functions included; parsing into
  the copy (`clone.New("gotpl").Parse(text)`) leaves the original alone.
- Parsing a `define` whose name already exists replaces it.

## Hints

<details><summary>Nudge</summary>

`include` cannot be written before you have the set, and the set cannot be
parsed before `include` is in its function map. A closure over a variable
breaks the cycle.
</details>

<details><summary>Approach</summary>

Add `include` and `tpl` to the function map, both closing over the set.
Parse the templates in Helm's order — sort a copy by path depth, deepest
first, then by path descending — and render them in path order as before.
</details>

## Further reading

- [Named templates](https://helm.sh/docs/chart_template_guide/named_templates/)
- [The tpl function](https://helm.sh/docs/howto/charts_tips_and_tricks/#using-the-tpl-function)
- [Go: text/template, nested template definitions](https://pkg.go.dev/text/template#hdr-Nested_template_definitions)
