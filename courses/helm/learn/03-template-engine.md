---
title: Render the templates
concepts: [text/template, helm template, partials, missing values]
---

## Core concept

Helm's template language is Go's `text/template`, nearly untouched. A
template is text with actions in `{{ }}`; the data it sees is a map, and for
now that map has one key:

```
{{ .Values.greeting }}                       a value
{{- if .Values.enabled }} … {{- end }}       a condition
{{- range $i, $v := .Values.items }} … {{- end }}
```

The `-` in `{{-` and `-}}` trims the whitespace on that side, which is how
a template controls blank lines in YAML.

What Helm adds is the handling around the engine:

- **One template set.** Every file in `templates/` is parsed into the same
  set, named `CHART/templates/…` (`demo/templates/configmap.yaml`), so any
  file can call a template another file defines — stage 6 relies on it.
- **Not everything is output.** A file whose name starts with `_`
  (`_helpers.tpl`) holds definitions only. `NOTES.txt` is shown to the user
  after an install, never sent to the cluster.
- **Missing values render empty.** Helm runs with `missingkey=zero` and
  then blanks the `<no value>` Go prints for a missing map key, so
  `{{ .Values.nothing }}` is an empty string, not an error. Reading a field
  *of* something that is not a map — `.Values.greeting.deep` when greeting is
  a string — is still an error.
- **Empty output is dropped.** A template that renders to whitespace (its
  `if` was false) produces no document at all.

For this stage, add:

```
app template [-f FILE]... [--set k=v]... NAME DIR
```

printing each rendered template, in path order, exactly as `helm template`
does — the content with surrounding whitespace trimmed, under a header:

```
---
# Source: demo/templates/a-greeting.yaml
apiVersion: v1
kind: ConfigMap
…
```

`NAME` is the release name; it matters from the next stage. A template that
fails to parse or execute makes the command exit non-zero, and Go's error
already names the file.

## Go APIs

- `template.New(root).Option("missingkey=zero")`, then `t.New(name).Parse(src)`
  per file: every file joins the one set and inherits the option.
- `t.ExecuteTemplate(w, name, data)` runs one named template.
- `strings.TrimSpace`, `strings.ReplaceAll`.

## Hints

<details><summary>Nudge</summary>

Parse everything before executing anything. A file can only call a
definition that has already been parsed into the set.
</details>

<details><summary>Approach</summary>

Two loops over the templates: the first parses every file (partials
included) under its `CHART/path` name; the second executes the ones that are
output, skipping `_*` and `NOTES.txt`, and keeps the non-empty results.
</details>

## Further reading

- [Getting started with a chart template](https://helm.sh/docs/chart_template_guide/getting_started/)
- [Flow control](https://helm.sh/docs/chart_template_guide/control_structures/)
- [text/template](https://pkg.go.dev/text/template)
