---
title: Write the plan down
concepts: [plans, optimistic concurrency, resourceVersion, review]
---

## Core concept

A plan on a terminal is gone once you scroll past it. A reviewer approves a
change in a pull request an hour after it was planned, and a pipeline applies
it later still. For "the change I reviewed is the change that lands" to mean
anything, the plan has to be a file: written once, read by a person, and then
applied *as written*, not recomputed from whatever the manifest and the
cluster say by then.

`plan --out PLAN` prints the same report as before and also writes the plan
as JSON:

```json
{
  "set": "web",
  "prune": true,
  "force": false,
  "changes": [
    {"action": "create", "object": {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {...}, "data": {...}}},
    {"action": "update", "uid": "5f1c…", "resourceVersion": "48211", "object": {...}},
    {"action": "unchanged", "uid": "9a03…", "resourceVersion": "48190", "object": {...}},
    {"action": "delete", "uid": "c7e2…", "resourceVersion": "48195",
     "object": {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "gone", "namespace": "team-a"}}}
  ]
}
```

Three things make it something you can apply later:

- **Every object, as it will be sent.** Namespace filled in, set label on.
  Applying the plan must not need the manifest any more.
- **Every change in the order apply makes it**, deletes last, exactly as stage
  13 prunes only after everything else has landed.
- **A pin on everything that already exists.** `uid` and `resourceVersion` are
  the live object's as the plan saw it. The resourceVersion changes on every
  write, so if it still matches at apply time, nothing has touched the object
  since review. If it does not, the plan describes a cluster that no longer
  exists. It is the same check stage 13 put on each prune delete, applied to
  every change. A create has no pin; its precondition is that the object is
  still absent.

`set`, `prune` and `force` record the options the plan was made with, so
applying it cannot quietly change them. A protected leftover is not a change,
so it is not in the file.

If the server would refuse any part of the change, there is no plan to
approve: report the refusal, exit 2, and write no file at all. Not an empty
one, not a partial one.

## Go APIs

- `json.MarshalIndent`: a plan is read by people before a program reads it.
- `live.GetUID()`, `live.GetResourceVersion()`: from the same `live` fetch the
  plan already makes. For a delete, from the listed leftover.

## Hints

<details><summary>Nudge</summary>

Give last stage's `change` two more fields, and the file is a loop over the
changes you already compute.
</details>

<details><summary>Approach</summary>

Record the pin when you classify each object. After printing the totals, if
nothing was refused and `--out` is set, write the file. For a delete, store
only what identifies the object.
</details>

<details><summary>Implementation</summary>

```go
type planChange struct {
    Action          string         `json:"action"`
    UID             string         `json:"uid,omitempty"`
    ResourceVersion string         `json:"resourceVersion,omitempty"`
    Object          map[string]any `json:"object"`
}
```
</details>

## Further reading

- [Resource versions](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions)
- [terraform plan -out](https://developer.hashicorp.com/terraform/cli/commands/plan#out-filename)
