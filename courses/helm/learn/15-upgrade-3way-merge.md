---
title: Upgrade without stamping on edits
concepts: [upgrade, three-way merge, strategic merge patch, value reuse, failed revisions]
---

## Core concept

An upgrade renders the chart again, records the next revision, and moves
the cluster from what the release sent last time to what it sends now:

```
app upgrade [-n NS] [-i|--install] [--reuse-values|--reset-values] [-f FILE]... [--set k=v]... NAME DIR
```

Moving the cluster is the hard part. Someone may have edited a live object
since the last revision, so neither "replace it with the new manifest" nor
"apply the difference between old and new manifests" is right. Helm takes
all three into account:

| | old manifest | new manifest | live |
|---|---|---|---|
| `steady` | `same` | `same` | `edited-live` |
| `changing` | `old` | `new` | `edited-live` |
| `liveadded` | | | `yes` |

After the upgrade, `steady` is `same` and `changing` is `new`: **every key
the chart declares takes the chart's value**, even one the chart never
changed. `liveadded` stays: a key no manifest mentions is the cluster's
business. That is a three-way merge patch: the change from live to the new
manifest, except that only keys the old manifest had are deleted. Labels
work the same way, so `app.kubernetes.io/managed-by` is never in danger.

Objects are handled whole like this:

- in the new manifest but not live: **create** it, with the ownership
  marks. An object that is new to the release but already exists must pass
  the install's ownership check, refused as `Unable to continue with update: ...`
  before anything is recorded.
- in both: **patch** it, three ways.
- in the old manifest but not the new: **delete** it. Already gone is fine.

**Which revision is "old".** The deployed one, not the latest: after a
failed upgrade the latest is `failed`, and the cluster still mostly looks
like the deployed revision. With none deployed, use the latest.

**Values.** Helm's default here surprises everyone:

| flags | user values of the new revision |
|---|---|
| none | the old revision's, unchanged |
| any `-f` or `--set` | only the new ones; the old are dropped |
| `--reuse-values` | the new ones merged over the old |
| `--reset-values` | only the new ones, even if there are none |

**Records.** Write revision N+1 as `pending-upgrade` (`Preparing upgrade`)
before touching the cluster. On success the old revision becomes
`superseded`, the new one `deployed` with `Upgrade complete`, and
`first_deployed` is carried over from the old one. On failure the new one
is `failed` with `Upgrade "web" failed: <the error>`, and the old one **stays
deployed**: nothing was superseded. The command prints:

```
Release "web" has been upgraded. Happy Helming!
NAME: web
...                 (the install summary, REVISION: N+1)
```

or `Error: UPGRADE FAILED: <the error>`, as `"web" has no deployed releases`
when the name has no revisions at all. With `--install` such a name is
installed instead, after `Release "web" does not exist. Installing it now.`

## Go APIs

- `k8s.io/apimachinery/pkg/util/strategicpatch`: `CreateThreeWayMergePatch(original, modified, current, meta, true)`,
  with `NewPatchMetaFromStruct` of the typed object from
  `k8s.io/client-go/kubernetes/scheme`'s `Scheme.New(gvk)`. Send it as
  `types.StrategicMergePatchType`.
- For a kind the scheme does not know (a custom resource), `jsonmergepatch.CreateThreeWayJSONMergePatch`
  and `types.MergePatchType`.
- A patch of `{}` changes nothing; skip the request.

## Hints

<details><summary>Nudge</summary>

Parse the old revision's manifest back into objects with the same code that
parses a fresh render. Then the update is a loop over the new objects and a
loop over the old ones, keyed by kind, namespace and name.
</details>

<details><summary>Approach</summary>

Split rendering so it takes the user's values instead of flags: install
computes them from `-f`/`--set`, upgrade then decides whether to reuse the
old ones. Keep the update itself, old objects to new, in a function of its
own: rollback is the same update with a different pair of manifests.
</details>

## Further reading

- [Helm 3's three-way strategic merge](https://helm.sh/docs/faq/changes_since_helm2/#improved-upgrade-strategy-3-way-strategic-merge-patches)
- [The upgrade action](https://github.com/helm/helm/blob/v3.18.6/pkg/action/upgrade.go)
- [Strategic merge patch](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-api-machinery/strategic-merge-patch.md)
