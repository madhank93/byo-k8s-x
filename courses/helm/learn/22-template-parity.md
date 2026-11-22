---
title: Agree with helm
concepts: [byte-for-byte parity, error rewording, --set syntax, .Capabilities]
---

## Core concept

Every earlier stage graded one feature. This one grades `template` the way a
user notices a difference: run the same command through the real helm and
your program, and stdout, stderr and the exit code must be **identical**.
Flags go before NAME, as in every stage.

```
app template [-n NS] [-f FILE]... [--set k=v]... [--set-string k=v]...
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

`--set-string` is the same syntax with every value a string, applied after
all the `--set` flags.

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
