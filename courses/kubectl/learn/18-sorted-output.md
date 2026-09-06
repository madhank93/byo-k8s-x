---
title: A stable order
concepts: [api semantics, output formats, determinism]
---

## What this stage teaches

The API returns items in whatever order the storage layer hands them over.
It is stable enough to look sorted and not stable enough to rely on — the
worst combination, because it works in testing and reorders in production.

Sorting by name is what makes two runs of the same command diffable, and what
makes a script that greps line 3 mean anything. Once `-A` is in play the key is
`(namespace, name)`, because the namespace is the outer grouping a reader
expects.

This is a small stage with a general lesson: determinism at the boundary is
part of the interface. Anything a person or a script consumes should be ordered
by something you chose, not by something you inherited.

## Go you'll reach for

- `sort.Slice(list.Items, func(i, j int) bool { ... })`.
- Compare namespace first, fall through to name when they match.

## Hints

<details><summary>Nudge</summary>

Sort before printing, and make the comparison a two-level one so the `-A`
listing groups sensibly.
</details>

## Going deeper

- [sort.Slice](https://pkg.go.dev/sort#Slice)
