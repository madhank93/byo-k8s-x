---
title: List what is installed
concepts: [label selectors, latest revision, state filters, table layout]
---

## Core concept

The records from the last stage are all Helm needs to answer "what is
installed?". Two read-only commands:

```
app list [-n NS] [-a|--all] [-A|--all-namespaces] [-q|--short]
app status [-n NS] NAME
```

**`list`** finds every Secret labelled `owner=helm` in the namespace (the
kubeconfig's, or `-n`'s; every namespace with `-A`), decodes them, and then,
in this order:

1. keeps only the **latest revision** of each release, a release being a
   name in a namespace;
2. keeps only **`deployed` and `failed`** releases, unless `-a` asks for
   every state (`uninstalled`, `pending-install`, ...);
3. **sorts by release name**, not by Secret name: `web` comes before
   `web-a`, though `sh.helm.release.v1.web-a.v1` sorts before
   `sh.helm.release.v1.web.v1`.

With `-q` it prints the names, one per line. Otherwise it prints a table:

```
NAME 	NAMESPACE	REVISION	UPDATED                             	STATUS  	CHART    	APP VERSION
web  	shop     	1       	2026-11-21 16:38:31.34105 -0600 CST 	deployed	web-1.0.0	2.1        
web-a	shop     	2       	2026-11-21 16:40:08.985454 -0600 CST	failed  	web-1.0.0	2.1        
```

Every cell, header included and the last column too, is padded with spaces
to its column's widest cell, and cells are joined by a tab. `UPDATED` is the
revision's `last_deployed` printed by Go's `time.Time.String()`, in local
time; `CHART` is the chart's name and version joined by `-`. With nothing to
list, the header is still printed.

**`status`** prints the latest revision, whatever its state, in the same
summary `install` prints, with `STATUS` and `REVISION` from the record. A
name with no revisions is an error:

```
Error: release: not found
```

The grader runs every one of these through your program and through the
real `helm`, against the same releases, and wants the same bytes. Some of
those releases your program installed and some helm did, including one
helm upgraded to revision 2 and one it uninstalled with `--keep-history`,
which leaves its last revision marked `uninstalled`. Running `helm list`
beside `app list` is the quickest way to find a difference.

## Go APIs

- `CoreV1().Secrets("")` lists across every namespace.
- `time.Time.String()` is the `UPDATED` format; a time decoded from JSON
  whose offset is the local one prints with the local zone's name.
- `fmt.Sprintf("%-*s", width, cell)` pads a cell to a width.
- `sort.SliceStable` keeps the API's order for equal names.

## Hints

<details><summary>Nudge</summary>

Group by namespace and name before filtering by state: a release whose
latest revision is `uninstalled` must not show its older `deployed` one.
</details>

<details><summary>Approach</summary>

Reuse the decoder from the last stage with a selector of just
`owner=helm`. Keep the highest version per `namespace/name`, filter, sort,
then either print names or build the rows and pad them in one pass over the
widths.
</details>

## Further reading

- [helm list](https://helm.sh/docs/helm/helm_list/)
- [helm status](https://helm.sh/docs/helm/helm_status/)
- [Helm's list action](https://github.com/helm/helm/blob/v3.18.6/pkg/action/list.go)
