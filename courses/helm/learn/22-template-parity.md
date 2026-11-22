---
title: Agree with helm
concepts: [byte-for-byte parity, error rewording, --set syntax, .Capabilities]
---

## Core concept

Every earlier stage graded one feature. This one grades `template` the way a
user notices a difference: run the same command through the real helm and
your program, and stdout, stderr and the exit code must be **identical**.
Helm reads flags anywhere among the arguments, so from here on yours does
too: `template rel ./chart --set a=1` is the same command as
`template --set a=1 rel ./chart`.

```
app template [-n NS] [-f FILE]... [--set k=v]... [--set-string k=v]...
             [--set-json k=JSON]... [--set-file k=FILE]...
             [-s TEMPLATE]... [--kube-version V] NAME DIR
```

The chart the grader renders packs in the traps: chomping with `-}}` and
`{{-`, `nindent` after an `include`, `toYaml` quoting `"yes"` and wrapping
long strings, `1000000` from values.yaml printing as `1e+06`, `default` on
every kind of empty, a document that is only a comment (kept, and printed
last because it has no kind). If your earlier stages are faithful, most of
it already matches. What follows is what is new.

**What templates can still ask.** `template` never talks to the cluster:

| | gives |
|---|---|
| `lookup "v1" "ConfigMap" NS NAME` | an empty map, `map[]`, so `if not (lookup ...)` is true |
| `.Capabilities.KubeVersion.Version` | `v1.33.0`, the version helm was built against; `--kube-version 1.29.3` makes it `v1.29.3` |
| `.Capabilities.APIVersions.Has "apps/v1"` | true for every group/version (and group/version/Kind) the client knows |

`.Files` gains Helm's methods: `Glob PATTERN` (a `Files` of the matches),
`.AsConfig` and `.AsSecrets` (base name to contents, as YAML; base64 for
secrets), `Lines` (one trailing newline dropped, then split on `\n`) and
`GetBytes`.

**How helm words a failed render.** text/template's errors stay as they are,
with two rewrites, and every failure inside the templates gains a hint:

```
Error: execution error at (perr/templates/cm.yaml:7:8): A valid .Values.who entry required!

Use --debug flag to render out invalid YAML
```

| failure | stderr after `Error: ` |
|---|---|
| `required`, `fail` | `execution error at (FILE:LINE:COL): MSG` |
| parse error | `parse error at (FILE:LINE): ` and the last clause of Go's message |
| anything else at run time | Go's own: `template: FILE:LINE:COL: executing "FILE" at <...>: ...` |
| inside `tpl` | `error calling tpl: error during tpl function execution for "TEXT": ...` |
| a document that is not YAML | `YAML parse error on FILE: ...` |

Each of those ends with the blank line and the `--debug` hint. Errors from
before rendering (bad values, a schema, `-s` matching nothing) do not.

**`--set`'s own syntax.** Commas separate pairs, dots separate keys, and:

| `--set` | value |
|---|---|
| `v={a,b}` | the list `[a b]`, each element typed like any value |
| `v={}` | a list of one empty string |
| `a=x\,y`, `b.c\.d=1` | a backslash escapes `,` and `.`: `"x,y"`, key `c.d` |
| `h=a=b` | everything after the first `=` is the value |
| `x` | `failed parsing --set data: key "x" has no value` |
| `x={a` | `failed parsing --set data: list must terminate with '}'` |

A key may index a list, and lists grow to fit, padded with nulls:

| `--set` | value |
|---|---|
| `a[2].b=y` | `a: [null, null, {b: y}]` |
| `m[1][1]=z` | `m: [null, [null, z]]` |
| `a[0]=x` then `a[1]=y` | `a: [x, y]`: later flags write into the same list |
| `a[0]=x` then `a.b=1` | `unable to parse key: interface conversion: interface {} is []interface {}, not map[string]interface {}` |
| `a[-1]=x` | `negative -1 index not allowed` |
| `a[65537]=x` | `index of 65537 is greater than maximum supported index of 65536` |
| `a[0]b=1` | `unexpected data at end of array index: ['b']` |

A list given with `--set` replaces the chart's list whole; it is never
merged index by index with the default.

Three more flags share the syntax, and helm applies them in this order
after the `-f` files, whatever order they were typed in:

1. `--set-json k=JSON`: the value is JSON, so `k=[1,"a"]` is a list and
   `k={"x":1}` a map. A bad value fails with only
   `failed parsing --set-json data ARG`.
2. `--set`
3. `--set-string`: every value a string.
4. `--set-file k=PATH`: the value is the file's contents.

**Output edges.** A chart that renders only hooks prints an empty line
first: helm prints the trimmed manifest and a newline, then the hooks. And
`-s` picks from everything printed, so `-s templates/hook.yaml` shows a hook.

## Go APIs

- `errors.As(err, &template.ExecError{})` tells run-time errors from the rest.
- `semver.NewVersion` for `--kube-version`, then `"v" + v.String()`.
- `scheme.Scheme.AllKnownTypes()` lists the group/version/kinds client-go knows.
- `path.Match` for `Files.Glob`.

## Hints

<details><summary>Nudge</summary>

To lift a `required` message out from under text/template's wrapping, helm
wraps it in markers when it raises it and pulls out what is between them
when it reports it.
</details>

<details><summary>Approach</summary>

Diff your program against helm on every case the stage lists and fix the
first difference each time. For `--set`, scan by hand: the key to the
first unescaped `=`, then either a `{...}` list or a value to the next
unescaped comma.
</details>

## Further reading

- [The template engine's error rewording](https://github.com/helm/helm/blob/v3.18.6/pkg/engine/engine.go)
- [strvals, the --set parser](https://github.com/helm/helm/blob/v3.18.6/pkg/strvals/parser.go)
- [helm template](https://helm.sh/docs/helm/helm_template/)
