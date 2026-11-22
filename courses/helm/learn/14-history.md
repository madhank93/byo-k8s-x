---
title: Keep every revision
concepts: [revisions, numeric ordering, per-revision reads, computed values]
---

## Core concept

Every upgrade or rollback adds a revision, and the old ones stay: that is
what makes a rollback possible. Your program cannot upgrade yet, but the
real `helm` can, and since your records are in Helm's format it will
happily upgrade a release your program installed. The grader does exactly
that, up to revision 11, with one failed upgrade along the way. Your
program reads the result:

```
app history [-n NS] [--max N] NAME
app get manifest [-n NS] [--revision N] NAME
app get values   [-n NS] [--revision N] [-a|--all] NAME
app get notes    [-n NS] [--revision N] NAME
```

**`history`** prints the last `--max` revisions (default 256; 0 or less
prints the header alone), oldest first, in the same padded table as `list` with other columns:

```
REVISION	UPDATED                 	STATUS    	CHART    	APP VERSION	DESCRIPTION     
1       	Sat Nov 21 16:40:08 2026	superseded	web-1.0.0	2.1        	Install complete
2       	Sat Nov 21 16:40:38 2026	deployed  	web-1.0.0	2.1        	Upgrade complete
```

Here `UPDATED` is `time.ANSIC`, not the `time.Time.String()` that `list`
uses. Order revisions **as numbers**: the Secret for revision 10 sorts
before the one for revision 2 by name. `DESCRIPTION` is the record's own,
however long: helm's failed upgrade writes `Upgrade "web" failed: <the
error>`, and the column is as wide as that.

**`get`** reads one revision: the latest, or `--revision N`. A revision
that does not exist is `Error: release: not found`, as is a name with none.
Find it by its **number**, not its place: helm's `--history-max` prunes the
oldest revisions, and the grader has it do so, so revision 11 can be the
first in a history.

| command | prints |
|---|---|
| `get manifest` | the record's manifest, then a newline (`fmt.Println`) |
| `get notes` | `NOTES:`, a newline, the notes untrimmed, a newline; nothing at all when the revision has no notes |
| `get values` | `USER-SUPPLIED VALUES:`, a newline, then `config` as YAML: `null` when there were none |
| `get values -a` | `COMPUTED VALUES:`, a newline, then `config` merged over the stored chart's values, as YAML |

The computed values come from the **record** alone: the stored chart's
defaults with the user's values merged over them, exactly as `values`
merges them: maps merge key by key at every depth, and a user's `null`
removes the default under it. The record holds no subcharts, so neither do
they.

As in the last stage, the grader runs each command through your program
and the real `helm` and wants the same bytes.

## Go APIs

- `sort.Slice` on the decoded `version`, not on Secret names.
- `sigs.k8s.io/yaml`'s `Marshal` prints a nil map as `null` and keys
  sorted, as Helm does.

## Hints

<details><summary>Nudge</summary>

`status` already finds the latest revision. Teach that function a revision
number, with 0 meaning the latest, and all four commands sit on top of it.
</details>

<details><summary>Approach</summary>

`get` takes a second word, so turn `get values` into one command before
parsing flags. Keep `history` sorted by version, take its tail for
`--max`, and reuse the table printer from `list`.
</details>

## Further reading

- [helm history](https://helm.sh/docs/helm/helm_history/)
- [helm get](https://helm.sh/docs/helm/helm_get/)
