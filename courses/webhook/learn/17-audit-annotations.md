---
title: Say why, where an auditor will read it
concepts: [auditannotations, warnings, audit log, observability]
---

## Core concept

A verdict answers one person: whoever ran the command. Two other audiences need
to know what you decided, and both have a field.

**`auditAnnotations`** are key-value pairs attached to the API server's audit
event for the request. They are the record: which policy fired, which rule
matched, what value you defaulted. Nobody sees them at the terminal — they end
up in the audit log, where someone reconstructing an incident is looking.

Keys are namespaced by the API server with your webhook's name, so
`policy: owner-required` is recorded as
`pods.byok8s.dev/policy: owner-required`. That means your keys only have to be
unique to you.

**`warnings`** are the opposite: a list of strings shown immediately to the
person applying the object, prefixed with `Warning:`. They are how you
deprecate a field without breaking anyone — the object is admitted, and the
message says what will stop working.

The two divide cleanly:

| You want to… | Field |
|---|---|
| leave a trace for later | `auditAnnotations` |
| tell the user right now | `warnings` |
| stop the write | `allowed: false` |

Both are attached to an **allowed** response as readily as a rejected one, and
that is where they earn their keep. A webhook that only speaks when it says no
is invisible while it is quietly doing the right thing — and invisible is
exactly what you do not want when someone asks what changed.

Keep warnings short and few: they are printed to a human on every matching
request, and a warning that appears on every apply is one people stop reading.

## Go APIs

- `resp.AuditAnnotations = map[string]string{"policy": "owner-required"}`.
- `resp.Warnings = []string{"…"}`.
- The audit event these land in is `ResponseComplete`, at `Metadata` level or
  above.

## Hints

<details><summary>Nudge</summary>

Both fields go on the `AdmissionResponse` you already build, and both work on a
response that allows the object. That is the case worth writing them for.
</details>

## Further reading

- [Response: audit annotations](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#audit-annotations)
- [Auditing](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/)
