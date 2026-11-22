---
title: Test a release
concepts: [test hooks, in-place record updates, status rendering, pod logs]
---

## Core concept

A chart can ship its own checks: Pods under `templates/tests/` annotated
`"helm.sh/hook": test`. Install leaves them alone; `test NAME` runs them
against the latest revision and records how they went.

```
test [--timeout D] [--filter name=N | --filter !name=N]... [--logs] NAME
```

**`test` is the event**, and Helm 2's `test-success` is the same one, stored
as `test`. The run is stage 17's hook runner on that event, so the order is
stage 18's: weight, then name. It stops at the first hook that fails.

**No new revision.** The hooks' `last_run` (and the default
`before-hook-creation` policy they ran with) are written back into the
latest revision's own Secret. Its status stays `deployed` even when a test
failed. The Pods are kept, and the next run deletes and recreates them.

**Status shows the runs**, in stored order, not run order. Where the
summary said `TEST SUITE: None`:

- a release with no test hooks: `TEST SUITE: None`, as before;
- otherwise one block per test hook that has run, and nothing at all for
  those that have not (so a fresh install prints no `TEST SUITE` line):

```
TEST SUITE:     tst-ok
Last Started:   Sat Nov 21 16:41:54 2026
Last Completed: Sat Nov 21 16:41:56 2026
Phase:          Succeeded
```

Labels are padded to 16 columns; times are `time.ANSIC`. `test` prints
this status for the record it just saved, then fails with the hook's error
if one failed: `Error: 1 error occurred:\n\t* pod tst-bad failed`.

**`--filter`** picks hooks by name, `name=N` to run only those, `!name=N`
to skip them. The skipped hooks keep their old `last_run`, and Helm saves
the list **skipped first, run last**, which status then shows.

**`--logs`** prints, after the status, an empty line and then for each test
hook the filter picked, by weight then name:

```
POD LOGS: tst-ok
ok-says-hello
second line

```

It walks every test hook the release has, not only those that ran this
time, so a Pod left from an earlier run shows its old logs. A hook that is
not a Pod has none, and the command fails:
`unable to get pod logs for tst-test-cm: pods "tst-test-cm" not found`.

## Go APIs

- `Pods(ns).GetLogs(name, &corev1.PodLogOptions{}).Stream(ctx)` returns
  the container's output; copy it to stdout. Its error for a missing Pod is
  the API's own `pods "x" not found`. `DoRaw` gives a vaguer one.
- `slices.Clone` before sorting for the logs, so the stored order survives.

## Hints

<details><summary>Nudge</summary>

Run only the picked hooks by handing the hook runner a record whose
`Hooks` are just those, then put the skipped ones back in front before
saving.
</details>

<details><summary>Approach</summary>

Status needs one change: collect the hooks with the `test` event; none
means `None`, otherwise print a block for each whose `started_at` is set.
`test` is load, filter, run, save, print, logs, then return the run's
error.
</details>

## Further reading

- [Chart tests](https://helm.sh/docs/topics/chart_tests/)
- [Helm's test action](https://github.com/helm/helm/blob/v3.18.6/pkg/action/release_testing.go)
