---
title: Apply, and let go of what you drop
concepts: [server-side apply, field ownership, declarative removal]
---

## Core concept

Now write for real. An apply is the same request as your dry-run without
`dryRun`, and because it is the same request, everything the diff showed is
what lands.

The interesting part is what happens to a field you **stop** setting. A
server-side apply is a complete statement of what this manager wants. A field
the manager owned last time and omits this time is released, and then:

```
  owners before        manifest drops it     result
  ─────────────        ─────────────────     ──────────────────────────
  byok8s               yes                   removed
  byok8s, other-tool   yes                   kept, other-tool owns it now
  other-tool           (never yours)         kept, untouched
```

This is the same rule as the three-way merge in stage 3, with the
last-applied annotation replaced by `managedFields` — and it is why the diff
in stage 6 already showed the removal before you wrote this command.

For this stage, add:

```
app apply -f FILE
```

Server-side apply each object as field manager `byok8s`, printing
`Kind/name applied` for each. Exit non-zero if any apply fails.

## Go APIs

- The same `Patch(..., types.ApplyPatchType, body, PatchOptions{FieldManager: "byok8s"})`
  as the dry-run, without `DryRun`.

## Hints

<details><summary>Nudge</summary>

If you wrote one apply helper with a dry-run switch in stage 6, this stage is
a loop.
</details>

<details><summary>Approach</summary>

Run `owners` before and after applying a manifest that drops a field, once
with the field shared and once without. The server does the bookkeeping; your
job is not to fight it — never send the full live object back, or you claim
every field in it.
</details>

## Further reading

- [Server-Side Apply: merge strategy](https://kubernetes.io/docs/reference/using-api/server-side-apply/#merge-strategy)
- [Server-Side Apply: field management](https://kubernetes.io/docs/reference/using-api/server-side-apply/#field-management)
