---
title: Take the field, deliberately
concepts: [server-side apply, force, field ownership]
---

## Core concept

Sometimes the answer to last stage's question is "yes, it is mine now". The
replicas a migration script set by hand, a label a retired tool still claims:
the manifest is right and the other manager is stale. Server-side apply has a
switch for that answer, `force: true`, and kubectl spells it
`--force-conflicts`.

Forcing is narrower than it sounds. It takes **only the fields that
conflict**, and only by moving their ownership to you. Everything else the
other manager owns stays theirs:

```
  other-tool owns         manifest sets        after a forced apply
  ───────────────         ─────────────        ─────────────────────────────
  .data.greeting=theirs   greeting: ours       greeting=ours, owned by you
  .data.color=blue        (nothing)            color=blue, still other-tool's
```

That is what makes it safe to reach for: you cannot wipe out fields you never
mentioned. But it is still taking something, so a good tool never does it
silently. Before applying, dry-run unforced to learn which fields will change
hands, and print each one:

```
ConfigMap/contested: took .data.greeting from "other-tool"
```

Print those lines only once the forced preflight has passed: if something else
refuses the change, nothing was taken.

`diff --force-conflicts` shows the change the forced apply would make, so the
user can read it before passing the same flag to `apply`. Without the flag,
both commands behave exactly as in stage 10.

## Go APIs

- `metav1.PatchOptions{Force: &force}`: a `*bool`, only meaningful for
  `types.ApplyPatchType`.
- The conflict parser you wrote for stage 10 already returns the field and the
  manager: reuse it for the "took" lines.

## Hints

<details><summary>Nudge</summary>

Without force the server tells you exactly what force would take. Ask it first.
</details>

<details><summary>Approach</summary>

Add a `force` field to the client and pass it on every patch. For `apply
--force-conflicts`: dry-run each object unforced and collect its conflicts,
then set `force`, preflight, apply, and print what you collected.
</details>

<details><summary>Implementation</summary>

```go
for _, o := range objs {
    _, err := c.apply(o, true) // force still false
    for _, cf := range conflicts(err) {
        taken = append(taken, fmt.Sprintf("%s: took %s from %q", ref(o), cf.field, cf.manager))
    }
}
c.force = true
```
</details>

## Further reading

- [Server-Side Apply: conflicts](https://kubernetes.io/docs/reference/using-api/server-side-apply/#conflicts)
- [Transferring ownership](https://kubernetes.io/docs/reference/using-api/server-side-apply/#transferring-ownership)
