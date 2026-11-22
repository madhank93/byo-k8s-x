---
title: Go back to a revision
concepts: [rollback, revisions as an append-only log, three-way merge]
---

## Core concept

A rollback never rewinds. It appends: the next revision is a **copy** of an
old one, and the cluster is moved to it exactly as an upgrade moves it.

```
app rollback [-n NS] NAME [REVISION]
```

From revisions 1 (`--set color=blue`) and 2 (an upgrade), `rollback web 1`
writes revision 3:

| field of revision 3 | taken from |
|---|---|
| `chart`, `config`, `manifest`, `info.notes` | revision 1, the target |
| `version` | the latest plus one |
| `info.first_deployed` | the latest (the install's time, carried along) |
| `info.description` | `Rollback to 1` |

Then the update from the last stage, from the **latest** revision's
manifest to the target's: objects only the target has are created again,
objects it lacks are deleted, and the rest are patched three ways, so a
key someone added by hand survives this too.

**Which revision.** A number is that revision; none, or `0`, means the
latest minus one, literally: after a failed revision 7, `rollback web` goes
to 6. Any existing revision is allowed, the latest included. The errors,
each exit 1 with nothing on stdout:

| | stderr |
|---|---|
| no such revision | `Error: release has no 99 version` |
| one revision, no number | `Error: release has no 0 version` |
| no such release | `Error: release: not found` |
| not a number | `Error: could not convert revision to a number: strconv.Atoi: parsing "abc": invalid syntax` |

None of them records anything.

**Records.** Write the new revision as `pending-rollback` first. On success
every `deployed` revision becomes `superseded`, the new one `deployed`, and
the program prints one line:

```
Rollback was a success! Happy Helming!
```

On failure the new revision is `failed`, `Rollback "web" failed: <the error>`,
and, unlike a failed upgrade, **the latest revision is marked `superseded`**
anyway: afterwards the release may have nothing deployed. The error is
printed bare, `Error: failed to create resource: ...`, with no
`ROLLBACK FAILED` in front.

## Go APIs

- `strconv.Atoi` for the revision; its error already reads
  `strconv.Atoi: parsing "abc": invalid syntax`.

## Hints

<details><summary>Nudge</summary>

If the upgrade's update takes two lists of objects, old and new, rollback
is a few lines of record keeping around one call to it.
</details>

<details><summary>Approach</summary>

Read the history, pick the latest and the target, copy the target into a
new record, parse both manifests, update, then fix up the statuses. Check
the revision exists before writing anything.
</details>

## Further reading

- [helm rollback](https://helm.sh/docs/helm/helm_rollback/)
- [The rollback action](https://github.com/helm/helm/blob/v3.18.6/pkg/action/rollback.go)
