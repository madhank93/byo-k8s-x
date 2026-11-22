---
title: Uninstall cleanly
concepts: [resource policy, uninstall order, delete hooks, keep-history, name reuse]
---

## Core concept

```
uninstall [--keep-history] [--ignore-not-found] [--dry-run] NAME...
```

Uninstall works on the latest revision:

1. Mark it `uninstalling`, set `info.deleted`, description
   `Deletion in progress (or silently failed)`.
2. Run its `pre-delete` hooks, then save it.
3. Delete its manifest's objects, **in uninstall order**, skipping any
   annotated `helm.sh/resource-policy: keep` (trimmed and lowercased, so
   `" Keep "` counts). An object already gone is no error.
4. Run its `post-delete` hooks.
5. Mark it `uninstalled`, description `Uninstallation complete`. Then
   either delete the Secret of **every** revision, or, with
   `--keep-history`, save this one.

Uninstall order is Helm's own list, not install order reversed: Services
go before the Deployments that use them. Within a kind, documents keep
manifest order. What was kept is printed, in that order:

```
These resources were kept due to the resource policy:
[Service] a-keep-svc
[Deployment] a-keep3

release "a" uninstalled
```

Without kept objects only the last line is printed. Several names are
uninstalled in turn, each printing its own block.

**Kept objects keep the release's marks.** Hooks are not the release's,
so uninstall does not delete those either. Install therefore now has to
**adopt** an object that is already marked as its own, patching it the
way upgrade does instead of failing to create it. Otherwise reinstalling
a name after an uninstall breaks.

**Errors.**

| case | result |
|---|---|
| no such release | `uninstall: Release not loaded: NAME: release: not found` |
| `--ignore-not-found` | prints `release "NAME" uninstalled` anyway, exit 0 |
| a post-delete hook fails | `uninstallation completed with 1 error(s): <err>`, but the release is gone |
| `--keep-history` on an uninstalled release | `the release named "NAME" is already deleted` |
| plain uninstall of one | purges its history, prints `release "NAME" uninstalled` |
| `--dry-run` | prints `release "NAME" uninstalled`, changes nothing |

**An uninstalled history still holds the name.** `list` hides it, and
`list -a` or `list --uninstalled` shows it. Then:

- `install NAME` is refused: `cannot re-use a name that is still in use`.
- `install --replace NAME` installs the next revision number, marking the
  previous one `superseded` / `superseded by new release` (a `failed` one
  is left as it is). The same goes when the name's last revision failed.
- `upgrade NAME` fails: `"NAME" has no deployed releases`. Upgrade starts
  from the deployed revision, or else from a failed or superseded last one,
  and an uninstalled one is neither.
- `upgrade -i NAME` says `Release "NAME" does not exist. Installing it
  now.` and does an `install --replace`.
- `rollback NAME 1` works, and revision 1 stays `uninstalled`: rollback
  supersedes only what is `deployed`.

## Go APIs

- `metav1.DeletePropagationBackground` in `DeleteOptions`, as for hooks.
- `apierrors.IsNotFound` to ignore an object someone else deleted.

## Hints

<details><summary>Nudge</summary>

The order needs a second kind list. Sort the stored manifest's documents
by it with a stable sort, and split them by the annotation before
deleting anything.
</details>

<details><summary>Approach</summary>

Install gains `replace`: allow an uninstalled or failed last revision,
number from it, supersede it. Install's create loop becomes upgrade's
create-or-patch with no old manifest. Uninstall itself is the five steps
above, using the hook runner you already have.
</details>

## Further reading

- [Resource policy](https://helm.sh/docs/howto/charts_tips_and_tricks/#tell-helm-not-to-uninstall-a-resource)
- [Helm's uninstall action](https://github.com/helm/helm/blob/v3.18.6/pkg/action/uninstall.go)
