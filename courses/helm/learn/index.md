---
title: Build your own Helm — primer
concepts: [charts, values, go templates, releases, three-way merge, hooks]
---

## The mental model

Helm is two programs wearing one name. The first is a **renderer**: it takes
a chart — a directory of Go templates and a file of default values — merges
in the values you supply, and prints Kubernetes manifests. The second is a
**release manager**: it sends those manifests to a cluster, remembers what it
sent as a numbered revision, and can upgrade, roll back and uninstall from
that record.

This course builds both, in that order, as one program:

```
  chart dir ──► load ──► values ──► templates ──► manifests      (stages 1-10)
                                                      │
                          install, upgrade, rollback ◄┘           (stages 11-20)
                                   │
                    release record (a Secret per revision)
```

Stages 1 to 10 never touch the cluster: they are `helm template`, built up
one idea at a time. From stage 11 the manifests go to the API server, and
every revision is stored in the cluster the way Helm stores it, so that by
the last stage the real `helm` can read your releases and you can read its.

## Charts, values, releases

| | What it is | Where it lives |
|---|---|---|
| **chart** | the package: `Chart.yaml`, `values.yaml`, `templates/` | a directory, or a `.tgz` of one |
| **values** | the knobs: chart defaults, then `-f` files, then `--set` | merged in memory |
| **release** | one installation of a chart, under a name, in a namespace | a Secret per revision |

The same chart installed twice under two names is two releases. A release's
history is a list of revisions, each holding the chart, the values and the
exact manifest that was sent — which is what makes rollback possible without
the original files.

## The command line

The program grows a small subset of Helm's commands. Flags always come before
the positional arguments, which both Go's `flag` package and `helm` accept:

```
app chart DIR
app values   [-f FILE]... [--set k=v]... DIR
app template [-n NS] [-f FILE]... [--set k=v]... NAME DIR
app install  [-n NS] [-f FILE]... [--set k=v]... NAME DIR
app upgrade | rollback | list | status | history | test | uninstall …
```

Errors go to stderr and exit 1, as Helm's do.

## How the stages are graded

Each stage writes charts and values files to a temporary directory and runs
your program on them. Rendering stages judge stdout and the exit code; from
stage 11 the stage also inspects what your program left in its namespace,
using a kubeconfig whose context points there. The last stage runs the real
`helm` beside yours and compares. Nothing inspects your source.

## Further reading

- [Helm: charts](https://helm.sh/docs/topics/charts/)
- [Helm: the chart template developer's guide](https://helm.sh/docs/chart_template_guide/)
- [Go: text/template](https://pkg.go.dev/text/template)
