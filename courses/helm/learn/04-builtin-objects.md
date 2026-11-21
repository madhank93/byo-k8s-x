---
title: Tell a template where it is
concepts: [built-in objects, .Release, .Chart, .Template, .Files]
---

## Core concept

A template rarely needs only its values. It names objects after the release
so two installs of one chart do not collide, labels them with the chart's
version, and reads config files shipped beside it. Helm puts all of that in
the data a template is executed with, next to `.Values`:

| Object | Fields a template uses |
|---|---|
| `.Release` | `Name`, `Namespace`, `Revision`, `IsInstall`, `IsUpgrade`, `Service` |
| `.Chart` | the whole of `Chart.yaml`: `Name`, `Version`, `AppVersion`, `Description`, … |
| `.Template` | `Name` (`demo/templates/cm.yaml`) and `BasePath` (`demo/templates`) |
| `.Files` | every chart file that is not a template, `Chart.yaml` or `values.yaml` |

`helm template` renders as if it were a first install: `Revision` is 1,
`IsInstall` is true, `IsUpgrade` false, and `Service` is always `Helm`.
`Namespace` comes from `-n`/`--namespace`; with neither, use `default` for
now — once the program talks to a cluster, the kubeconfig's namespace takes
its place.

`.Template` is different for every file, so it cannot be built once: each
template is executed with its own copy of the data.

`.Files` is not a map but an object with methods, and the one every chart
uses is `.Files.Get "files/app.conf"`, returning the file's contents as a
string — or an empty string for a file that is not there. The chart's own
`Chart.yaml`, `values.yaml` and templates are not files in this sense; Helm
has already turned them into `.Chart`, `.Values` and the template set.

For this stage, `template` accepts `-n NS` and `--namespace NS`, and every
template sees the objects above. The stage renders a chart that prints them
and compares the output exactly.

## Go APIs

- Templates resolve `.Release.Name` on a struct field or a map key alike, so
  either works; a struct with exported fields documents the shape.
- A method on a named type is callable from a template:
  `type Files map[string][]byte` with `func (f Files) Get(name string) string`
  makes `{{ .Files.Get "x" }}` work.
- `flag.StringVar` twice on the same variable gives a flag two names.

## Hints

<details><summary>Nudge</summary>

`.Chart.AppVersion` already works if the struct you loaded `Chart.yaml` into
is what you put under `Chart`.
</details>

<details><summary>Approach</summary>

Keep every non-template file the loader walks past, apart from `Chart.yaml`
and `values.yaml`, in a `Files` map. Build the data map per template: the
shared `Values`, `Release`, `Chart` and `Files`, plus that file's `Template`.
</details>

## Further reading

- [Built-in objects](https://helm.sh/docs/chart_template_guide/builtin_objects/)
- [Accessing files inside templates](https://helm.sh/docs/chart_template_guide/accessing_files/)
