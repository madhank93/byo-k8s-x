---
title: Read a chart
concepts: [charts, Chart.yaml, semantic versioning]
---

## Core concept

A chart is a directory with a fixed shape:

```
demo/
  Chart.yaml          # required: what this chart is
  values.yaml         # defaults for every knob (stage 2)
  templates/          # Go templates, rendered into manifests (stage 3)
    _helpers.tpl      # leading underscore: definitions, not output
    NOTES.txt         # printed after install, never sent to the cluster
    configmap.yaml
  charts/             # subcharts (stage 7)
  files/app.conf      # anything else: readable from templates (stage 4)
```

`Chart.yaml` is the only file that must exist, and Helm refuses a chart whose
`Chart.yaml` it cannot trust:

- `name` is required, and must be a plain name — `a/b` is a path.
- `version` is required and must be a **semantic version**. It is not
  decoration: dependency ranges (`~1.2.0`) are resolved against it later.
- `type`, when present, is `application` or `library`.
- `apiVersion` is `v2` for Helm 3 charts. Helm 2 charts often left it out,
  and Helm 3 still loads those, treating them as `v1`.

Everything under `templates/` is a template, including files in nested
directories, except dotfiles directly inside `templates/`, which Helm never
loads. Nothing outside it is.

For this stage, add a command:

```
app chart DIR
```

It prints the chart's name and version, then every file under `templates/`
as a slash path from the chart root, sorted:

```
demo 1.2.3
templates/NOTES.txt
templates/_helpers.tpl
templates/configmap.yaml
templates/sub/deeper.yaml
```

Sorted means by the whole path as a string, the way Helm sorts:
`templates/a-b.yaml` comes before `templates/a/x.yaml`, because `-` sorts
before `/`.

A missing or unparseable `Chart.yaml`, or one that fails a check above, makes
the command exit non-zero with the reason on stderr.

## Go APIs

- `sigs.k8s.io/yaml` — `yaml.Unmarshal` into a struct with `json:` tags. Helm
  uses this package throughout, and so will you: it converts YAML to JSON
  first, so a value that would not survive JSON does not survive it either.
- `github.com/Masterminds/semver/v3` — `semver.NewVersion`, the parser Helm
  uses. It is lenient on purpose: `v1.2.3` and `1.2` are accepted.
- `filepath.WalkDir` sorts each directory on its own, which is not the same
  order as sorting whole paths; `sort.Strings` or `slices.Sort` afterwards is.

## Hints

<details><summary>Nudge</summary>

Load `Chart.yaml` into a struct whose Go field names are the ones a template
will see later: `.Chart.Name`, `.Chart.Version`, `.Chart.AppVersion`.
</details>

<details><summary>Approach</summary>

Walk the chart directory, make each path relative to the chart root, convert
it to slashes, and keep the ones that start with `templates/`. Keep the file
contents too — stage 3 renders them.
</details>

## Further reading

- [The Chart.yaml file](https://helm.sh/docs/topics/charts/#the-chartyaml-file)
- [Semantic Versioning 2.0.0](https://semver.org/)
