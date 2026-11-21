---
title: Order the manifests like Helm
concepts: [multi-document YAML, install order, --show-only]
---

## Core concept

A template file can hold several resources separated by `---`, and the
order a chart's files sort in says nothing about what has to exist first.
A Deployment that mounts a ConfigMap, in a Namespace the chart also
creates, should reach the API server after both. So before printing (and,
from the next stage, before installing), Helm splits and sorts:

1. **Split** each rendered file into documents. Each document is printed
   on its own, under its file's `# Source:` header.
2. **Sort** the documents by kind, by Helm's install order:

```
PriorityClass  Namespace  NetworkPolicy  ResourceQuota  LimitRange
PodSecurityPolicy  PodDisruptionBudget  ServiceAccount  Secret  SecretList
ConfigMap  StorageClass  PersistentVolume  PersistentVolumeClaim
CustomResourceDefinition  ClusterRole  ClusterRoleList  ClusterRoleBinding
ClusterRoleBindingList  Role  RoleList  RoleBinding  RoleBindingList
Service  DaemonSet  Pod  ReplicationController  ReplicaSet  Deployment
HorizontalPodAutoscaler  StatefulSet  Job  CronJob  IngressClass  Ingress
APIService
```

   Kinds not in the table — your CRDs' resources, and also webhook
   configurations, which the table leaves out — come after every known
   kind, alphabetically by kind. Documents of the same kind keep the order
   they had: file path, then position in the file.

The split is a regular expression, and its edges show up in the output, so
use Helm's exactly:

```go
regexp.MustCompile("(?:^|\\s*\n)---\\s*")
```

Split the trimmed file, skip empty pieces, and trim each document. The
pattern eats whitespace on both sides of `---` but nothing else, so
`--- # note` leaves `# note` at the top of the next document, and an empty
document (`---` followed only by blank lines and another `---`) leaves the
second `---` at the start of the document after it.

Each document must parse as YAML: Helm reads its `kind`, and a document
that does not parse fails the whole command with
`YAML parse error on web/templates/x.yaml: …`, printing nothing.

Finally, `helm template` takes **`--show-only`** (`-s`), repeatable: a
template path without the chart name (`templates/svc.yaml`,
`charts/db/templates/cm.yaml`), or a `filepath.Match` glob. The output is
each pattern's documents in turn, in sorted order. A pattern that matches
nothing is an error naming it.

## Go APIs

- `regexp.Regexp.Split(s, -1)` splits on every match.
- `sort.SliceStable` keeps equal elements in their original order, which is
  what "same kind keeps its order" needs.
- `filepath.Match(pattern, name)` for `--show-only`.

## Hints

<details><summary>Nudge</summary>

Unmarshal each document into a struct with only a `Kind` field. That is
both the parse check and the sort key.
</details>

<details><summary>Approach</summary>

Render as before, then turn the list of files into a list of documents,
each keeping its file name. Stable-sort it with a less function on kind
ranks: both unknown compare by name, one unknown goes last, both known
compare by rank. Print, or filter by the `--show-only` patterns and print.
</details>

## Further reading

- [helm template](https://helm.sh/docs/helm/helm_template/)
- [Helm's kind sorter](https://github.com/helm/helm/blob/v3.18.6/pkg/releaseutil/kind_sorter.go)
