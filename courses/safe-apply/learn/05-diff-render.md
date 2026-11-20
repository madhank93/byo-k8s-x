---
title: Render it like diff(1)
concepts: [kubectl diff, unified diff, KUBECTL_EXTERNAL_DIFF]
---

## Core concept

`kubectl diff` does not contain a diff algorithm. It writes two directories —
one file per object, the live version in one and the would-be version in the
other — and runs `diff -u -N` over them. `KUBECTL_EXTERNAL_DIFF` swaps in any
other program that takes two directories, which is how people plug in
`dyff` or `delta`.

```
  $TMP/live/v1.ConfigMap.web.app      $TMP/merged/v1.ConfigMap.web.app
  data:                               data:
    a: "1"                       ─►     a: 1x
    b: "2"
    c: "3"                              c: "3"
```

`-N` treats a file present on one side only as empty on the other, so a new
object shows up as all additions with no special case. The file name only
has to be stable and unique per object: group-version, kind, namespace, name.

Strip the same noise fields as the structured diff, from both sides, before
writing YAML.

For this stage, add a `--unified` flag to `diff`. With it, print the output of
`diff -u -N` over the two directories instead of the field list. The exit
codes stay the same — and they are already diff(1)'s: 0 same, 1 different,
2 trouble.

## Go APIs

- `os.MkdirTemp`, then `live/` and `merged/` inside it.
- `sigs.k8s.io/yaml.Marshal(obj.Object)`.
- `exec.Command("diff", "-u", "-N", "live", "merged")` with `cmd.Dir` set, and
  `errors.As(err, &exitErr)` to read its exit code — 1 is not a failure here.

## Hints

<details><summary>Nudge</summary>

Run `kubectl diff` with `KUBECTL_EXTERNAL_DIFF="ls -R"` and look at what it
hands the program.
</details>

<details><summary>Approach</summary>

You already have live and after for each object from the structured diff.
Write each non-nil one into its side's directory, run diff, pass its stdout
through.
</details>

## Further reading

- [kubectl diff](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_diff/)
- [diff(1): unified format](https://www.gnu.org/software/diffutils/manual/html_node/Unified-Format.html)
