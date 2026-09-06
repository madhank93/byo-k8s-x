---
title: Print the object as JSON
concepts: [output formats, serialization, api objects]
---

## What this stage teaches

`-o json` is not "your table, as JSON". It is the API's own object, verbatim —
apiVersion, kind, metadata, spec, status, managedFields and all. That is what
makes it composable: `| jq` gets exactly what the server sent, including fields
your program never looked at and does not know about.

Which means the right implementation *skips* your rendering path entirely
rather than reconstructing an object from the columns you chose. Anything you
build from the table is a summary pretending to be an object.

One detail: kubectl prints a `List` object when listing, not a bare JSON array,
so a caller can tell "a collection of pods" from "a pod that happens to be
first". Marshal the list you got, not `list.Items`.

## Go you'll reach for

- `json.MarshalIndent(list, "", "    ")` — kubectl indents with four spaces.
- The API types carry `json` struct tags, so field names come out as the API
  spells them.

## Hints

<details><summary>Nudge</summary>

Branch before you build the table, not after.
</details>

<details><summary>Approach</summary>

Marshal the whole list object the client returned. Resist the urge to strip
`managedFields` or other noise — a faithful dump is the contract, and stage 22
will make those fields the point.
</details>

## Going deeper

- [encoding/json](https://pkg.go.dev/encoding/json)
- [Kubernetes API objects](https://kubernetes.io/docs/reference/using-api/api-concepts/#standard-api-terminology)
