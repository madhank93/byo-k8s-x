---
title: Wider columns
concepts: [output formats, unstructured]
---

## Core concept

`-o wide` adds the columns that only make sense for a particular resource: a
Pod's node and IP, a Service's cluster IP and ports. That per-resource nature
is the point — there is no generic "wide", which is why real kubectl asks the
*server* for a Table with the columns already chosen and can therefore print a
CRD's own `additionalPrinterColumns`.

Deciding the columns client-side, as you will here, is the readable version of
the same idea and has an honest cost: your program only knows about resources
it has been taught.

Missing values need care. An unscheduled pod has no `spec.nodeName` and no
`status.podIP`, and an empty cell makes the row look misaligned. `<none>` is
kubectl's answer.

## Go APIs

- `unstructured.NestedString(o.Object, "status", "podIP")` — the second return
  is "found", separate from the error.
- One small function for headers and one for values, both keyed on the GVR.

## Hints

<details><summary>Nudge</summary>

Both the header and the row need the extra cells, and they must agree — which
is an argument for deriving them from the same place.
</details>

## Further reading

- [Server-side printing](https://kubernetes.io/docs/reference/using-api/api-concepts/#receiving-resources-as-tables)
