---
title: Merge the values
concepts: [values, precedence, deep merge, --set]
---

## Core concept

A chart's `values.yaml` holds its defaults. Whoever installs it overrides
them in layers, and the order is fixed no matter how the flags are written:

```
  values.yaml          the chart's defaults           lowest
  -f prod.yaml         each file, in the order given
  -f last.yaml
  --set a=1,b=2        each --set, in the order given  highest
```

`--set` beats every file even when it comes first on the command line.

Layers merge **map by map**: a key in a later layer replaces that key, and
only that key, so `-f` setting `image.tag` leaves `image.repository` alone.
Anything that is not a map — a string, a number, a **list** — is replaced
whole. There is no way to append to a list from `-f`.

A `null` in the user's values **deletes** the chart's default. That is why
the user's layers are merged together first and laid onto the defaults
second: while merging the user's layers, a `null` is a value like any other
(a later file can still replace it); only the final step against the
defaults turns it into a deletion. Helm calls that step *coalescing*, and it
keeps the user-supplied layer on its own because the release remembers it
separately from the defaults.

A `null` with no default under it has nothing to delete, so it stays in the
result as a null: `--set extra=null` on a chart with no `extra` gives
`extra: null`.

`--set` is a small language of its own. A dotted key builds nested maps,
commas separate pairs, and the value is typed:

| `--set` value | becomes |
|---|---|
| `true`, `false` | a boolean |
| `null` | null, so it deletes |
| `10` | an integer |
| `007` | the string `"007"` — a leading zero keeps it a string |
| anything else | a string |

For this stage, add:

```
app values [-f FILE]... [--set k=v]... DIR
```

printing the merged values as YAML. Key order and style are up to you; the
stage reads your output as data. A `--set` with no `=`, a missing `-f` file
or one that is not YAML makes the command exit non-zero. A chart with no
`values.yaml` simply has no defaults.

## Go APIs

- `flag.Var` with a type implementing `flag.Value` collects a repeated flag.
  Go's `flag` accepts `--set` as well as `-set`.
- `yaml.Unmarshal` into `map[string]any` gives nested `map[string]any`, so a
  type switch is enough to tell maps from everything else.
- `strings.Cut(pair, "=")` reports whether the separator was there at all.

## Hints

<details><summary>Nudge</summary>

Write two merges: one that lets `null` overwrite (for the user's layers) and
one that turns `null` into a delete (onto the defaults).
</details>

<details><summary>Approach</summary>

Merge every `-f` file into one empty map, then apply every `--set` to that
same map, then coalesce it onto a copy of the chart's values. Copy the
defaults: the chart's own map is reused when templates render.
</details>

## Further reading

- [Values files](https://helm.sh/docs/chart_template_guide/values_files/)
- [The format and limitations of --set](https://helm.sh/docs/intro/using_helm/#the-format-and-limitations-of---set)
- [Deleting a default key](https://helm.sh/docs/chart_template_guide/values_files/#deleting-a-default-key)
