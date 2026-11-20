---
title: Stop at someone else's field
concepts: [server-side apply, conflicts, preflight, all-or-nothing]
---

## Core concept

An apply that sets a field another manager owns, to a different value, is a
**conflict**. The server refuses it with a 409 and says exactly which fields
and whose they are:

```
Apply failed with 1 conflict: conflict with "other-tool": .data.greeting
```

It is a question, not a fault. The other manager might be a teammate's
pipeline, an HPA that owns `spec.replicas`, or a controller that will set the
field straight back. Answering it is the user's job, so the tool's job is to
ask it clearly: which object, which field, which owner.

The second half of this stage is about *when* you find out. Applying objects
one by one and stopping at the first refusal leaves the cluster half-changed:
everything before the refusal landed, everything after did not. A safe apply
dry-runs every object first, and writes nothing unless all of them would be
accepted.

```
  dry-run all ──► any refused? ──yes──► report every refusal, write nothing
                       │ no
                       ▼
                  apply each
```

For this stage, report a conflict as one line per field:

```
ConfigMap/contested: conflict: .data.greeting owned by "other-tool"
```

`diff` prints these like any other refusal and exits 2. `apply` dry-runs
every object before writing any; if anything is refused it prints the
reasons, writes nothing, and exits non-zero.

## Go APIs

- `apierrors.IsConflict(err)`.
- `status.Status().Details.Causes`: each cause with type
  `metav1.CauseTypeFieldManagerConflict` has the path in `Field` and the
  manager, quoted, in `Message`.

## Hints

<details><summary>Nudge</summary>

Print the whole error once and read it. Then print its `Details`.
</details>

<details><summary>Approach</summary>

A preflight loop over the dry-run you already have, collecting refusals,
before the apply loop. The report function is shared with `diff`.
</details>

<details><summary>Implementation</summary>

```go
for _, cause := range status.Status().Details.Causes {
    if cause.Type != metav1.CauseTypeFieldManagerConflict { continue }
    _, rest, _ := strings.Cut(cause.Message, `"`)
    manager, _, _ := strings.Cut(rest, `"`)
    fmt.Printf("%s: conflict: %s owned by %q\n", ref(o), cause.Field, manager)
}
```
</details>

## Further reading

- [Server-Side Apply: conflicts](https://kubernetes.io/docs/reference/using-api/server-side-apply/#conflicts)
