---
title: Summarise what a change will do
concepts: [plans, impact, review]
---

## Core concept

A diff answers *what changes, field by field*. The person approving a change
asks something shorter first: *what happens to each object?* Forty lines of
`~ .spec.template...` hide the one line that matters, which is that a Secret
is about to be deleted. Terraform answers that question with a plan, and
ends it with a count:

```
ConfigMap/precious kept: prune disabled
ConfigMap/new-a create
ConfigMap/changed update
ConfigMap/same-a unchanged
ConfigMap/gone delete
1 to create, 1 to update, 1 to delete, 1 unchanged
```

`plan -f FILE` takes the same flags as `apply` (`--set`, `--prune`,
`--force-conflicts`) and says what that apply would do, without doing it.
With `--force-conflicts` a field another manager owns is the server's to take,
so that object is an update, not a refusal.
Every piece already exists; this stage puts them in one place:

```
  action      comes from
  ─────────   ──────────────────────────────────────────────────────
  create      live fetch found nothing                     (stage 2)
  update      dry-run result differs from live             (stage 6)
  unchanged   dry-run result is the same as live
  delete      a leftover of the set, with --prune          (stage 12)
  kept        a leftover marked prune: disabled            (stage 13)
```

Use the dry-run, not the manifest, to decide between update and unchanged. A
manifest that leaves out a defaulted field is not asking for a change, nor is
one that writes a ResourceQuota's `cpu: 1000m` the server stores as `"1"`, and
only the server knows what it would default or normalise. A kept leftover is
named but not counted: it is not going anywhere.

The totals line comes last and always has all four counts, in that order. A
refusal is reported the way `diff` reports it and the object is left out of
the plan. Exit codes follow `diff`: **0** when every object is unchanged and
nothing is deleted, **1** when something would change, **2** when the server
would refuse part of it.

## Go APIs

Nothing new. `live`, `after`, `fieldDiff`, `leftovers`, `pruneDisabled` and
`report` are all yours already.

## Hints

<details><summary>Nudge</summary>

Write it as a function that returns the changes, and print them from `run`.
Next stage writes the same changes to a file.
</details>

<details><summary>Approach</summary>

For each object: fetch live, dry-run, classify. With `--prune`, add a delete
for each leftover that is not protected. Count by action as you print.
</details>

<details><summary>Implementation</summary>

```go
action := "unchanged"
switch {
case live == nil:
    action = "create"
case len(fieldDiff(live, after)) > 0:
    action = "update"
}
out = append(out, change{action, o})
```
</details>

## Further reading

- [terraform plan](https://developer.hashicorp.com/terraform/cli/commands/plan)
- [kubectl diff](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_diff/)
