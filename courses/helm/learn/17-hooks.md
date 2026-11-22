---
title: Run hooks around the install
concepts: [lifecycle hooks, delete policies, readiness, release-scoped vs owned objects]
---

## Core concept

A chart can ask for work to happen at a point in a release's life: a
migration Job before an upgrade, a check after an install. Such a document
carries an annotation:

```yaml
metadata:
  name: web-migrate
  annotations:
    "helm.sh/hook": pre-upgrade,pre-install
    "helm.sh/hook-weight": "5"                          # stage 18
    "helm.sh/hook-delete-policy": hook-succeeded
```

The events are `pre-` and `post-` each of `install`, `upgrade`, `rollback`
and `delete`, plus `test` (stage 19). A document naming any other event is
**dropped**, neither hook nor object, with
`info: skipping unknown hook: "pre-foo"` on stderr.

**Hooks are not part of the manifest.** After splitting and ordering the
documents, take the hooks out: the release's `manifest` holds only the
rest, and the record gains `hooks`, in the same order (kind, then path):

```json
{"name": "web-migrate", "kind": "Job", "path": "web/templates/migrate.yaml",
 "manifest": "apiVersion: batch/v1\nkind: Job\n...",
 "events": ["pre-upgrade", "pre-install"],
 "last_run": {"started_at": "", "completed_at": "", "phase": ""},
 "weight": 5, "delete_policies": ["hook-succeeded"]}
```

`manifest` is the document alone, no `# Source` line. `weight` and
`delete_policies` are left out when empty, `last_run` never is.

**Running an event's hooks**, one at a time:

1. No delete policy? It gets `before-hook-creation`, and that is recorded.
2. With `before-hook-creation`, delete any object with the hook's name and
   wait until it is gone. Whoever owns it.
3. Create the hook's object **without** the ownership marks and without the
   ownership check: hooks are not the release's. `last_run` gets
   `started_at` and phase `Running`.
4. Wait for it: a Job until its `Complete` condition, a Pod until
   `Succeeded`; anything else is ready at once. A `Failed` Job condition is
   `job NAME failed: <its reason>`, and the wait gives up after `--timeout`
   (default 5m) with `timed out waiting for the condition`.
5. Record `completed_at` and `Succeeded` or `Failed`. On failure delete it
   if its policy is `hook-failed`, and stop.

When all of an event's hooks succeed, delete those whose policy is
`hook-succeeded`, last first.

**Where events run.**

| command | order |
|---|---|
| install | record `pending-install`, pre-install, create objects, post-install, `deployed` |
| upgrade | record, pre-upgrade, update, post-upgrade, supersede, `deployed` |
| rollback | record, pre-rollback, update, post-rollback, supersede, `deployed` |

A rollback runs the hooks **stored with the target revision**, as they were
rendered then: they are copied into the new record along with the manifest.

**Failures** are recorded like any other, with Helm's wording, which is not
consistent: an install says `failed pre-install: <err>`, an upgrade
`pre-upgrade hooks failed: <err>`. Helm wraps each hook error in a
one-entry list, so the text is `1 error occurred:\n\t* <the error>\n\n`; a
create the API refused also says which hook:
`warning: Hook pre-upgrade web/templates/x.yaml failed: 1 error occurred: ...`.

## Go APIs

- `slices.Contains` for events and policies; `strconv.Atoi` for the weight.
- `metav1.DeletePropagationBackground` in `DeleteOptions`, as Helm deletes
  hooks: the Job goes at once, its pods after.
- `unstructured.NestedSlice(obj, "status", "conditions")` and
  `NestedString(obj, "status", "phase")` to read readiness from a dynamic
  client's object.

## Hints

<details><summary>Nudge</summary>

Do the split where the manifests are ordered, so `template`, install and
upgrade all get it, and give the hook struct Helm's JSON tags: the real
`helm` reads these records.
</details>

<details><summary>Approach</summary>

One `execHooks(record, event)` that filters the record's hooks, runs them,
and updates them in place; the caller saves the record. Install and upgrade
call it twice, rollback twice on the copied hooks. A wait loop that polls
every half second is all the readiness this needs.
</details>

## Further reading

- [Chart hooks](https://helm.sh/docs/topics/charts_hooks/)
- [Helm's hook runner](https://github.com/helm/helm/blob/v3.18.6/pkg/action/hooks.go)
