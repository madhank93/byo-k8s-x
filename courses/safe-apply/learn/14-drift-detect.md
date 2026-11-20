---
title: Notice what changed behind your back
concepts: [drift, managedFields, field ownership]
---

## Core concept

You applied a manifest last week. Since then someone ran `kubectl edit`, a
script patched a value during an incident, another tool force-applied over
you. The cluster no longer matches what you said, and nobody told you. That
gap is **drift**.

`diff` already shows it, mixed in with everything else: a drifted field and a
field you just edited in your manifest both appear as changes. They mean very
different things. Your own edit is the change you intend. A drifted field is
a change someone else made on purpose, and your next apply will undo it, or
stop at a conflict over it.

`managedFields` tells the two apart. Server-side apply records which manager
owns each field, and a write moves ownership:

```
  how someone else changed a field you own     what happens to your ownership
  ──────────────────────────────────────────   ──────────────────────────────
  an update (kubectl edit, a controller)       moves to them
  a forced apply                               moves to them
  an apply of the same value                   shared: still yours as well
```

So a field the manifest sets is drift when its live value differs **and you no
longer own it**. If you still own it, the difference is your manifest moving,
not the cluster.

```
  manifest says   live has   byok8s owns it?   verdict
  ─────────────   ────────   ───────────────   ──────────────────────────
  color: blue     red        no, kubectl-edit  drift
  size: s         xl         no, other-tool    drift
  mode: b         a          yes (shared)      your change, not drift
  tone: cool      cool       no, kubectl-edit  same value, not drift
  (nothing)       extra: x   no                not yours, not drift
```

`drift -f FILE` prints one line per drifted field: what the manifest says,
what is there now, and who changed it.

```
ConfigMap/app: drift: .data.color "blue" -> "red" by "kubectl-edit"
```

It exits 0 when nothing has drifted and 1 when something has, and changes
nothing. A field missing from the live object has no owner left to name, so
it looks exactly like a field you just added; leave it out.

Ownership has blind spots. A list compares whole, and byok8s keeps owning each
element's key, so a `kubectl set image` inside a container list goes
unreported. And a field the manifest newly adds that someone else already holds
reads as drift: managedFields cannot tell "never owned" from "lost".

## Go APIs

- `ownership(live)` from stage 8 already gives every field's managers, and
  `leaves` from stage 4 gives every field's value. Their paths line up for
  map fields: `.data.color` in both.

## Hints

<details><summary>Nudge</summary>

Run `owners` on an object after `kubectl edit` changes one of your fields. Who
owns it now?
</details>

<details><summary>Approach</summary>

For each manifest field whose live value differs, look up the field's owners.
Skip it if `byok8s` is one of them; otherwise report it with the other owner.
</details>

<details><summary>Implementation</summary>

```go
for p, want := range leaves(desired) {
    got, ok := have[p]
    if !ok || got == want { continue }
    // mine: byok8s owns p; by: any other owner of p
    if mine || by == "" { continue }
    out = append(out, fmt.Sprintf("%s %s -> %s by %q", p, want, got, by))
}
```
</details>

## Further reading

- [Server-Side Apply: field management](https://kubernetes.io/docs/reference/using-api/server-side-apply/#field-management)
- [Argo CD: diffing and sync status](https://argo-cd.readthedocs.io/en/stable/user-guide/diffing/)
