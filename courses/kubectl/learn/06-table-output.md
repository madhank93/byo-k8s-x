---
title: Print a table
concepts: [output formats, tabwriter]
---

## What this stage teaches

A table is a user interface, and the property that makes it one is that the
columns line up when the names do not. Computing widths by hand works until the
first 40-character pod name; `text/tabwriter` does it properly by buffering
the rows and choosing each column's width from the widest cell in it.

That buffering is why the `Flush` matters: nothing is written until you call
it, and forgetting it produces a program that prints nothing and exits zero.

Worth knowing for later: real kubectl usually does not build the table at all.
It asks the server for `application/json;as=Table`, and the *server* decides
the columns — which is how it prints a CRD it has never seen. Doing it
client-side here keeps the program readable.

## Go you'll reach for

- `tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)` — minwidth, tabwidth,
  padding, padchar, flags. Three spaces of padding is kubectl's look.
- Write cells separated by `\t`, one row per line, then `w.Flush()`.

## Hints

<details><summary>Nudge</summary>

The separator between cells is a literal tab, and the header row is written the
same way as the data rows.
</details>

<details><summary>Approach</summary>

Build each row as a `[]string` and `strings.Join(row, "\t")` it. Keeping rows
as slices costs nothing now and is what makes the optional columns in stages 10
and 15 an `append` rather than a rewrite.
</details>

## Going deeper

- [text/tabwriter](https://pkg.go.dev/text/tabwriter)
- [Server-side printing](https://kubernetes.io/docs/reference/using-api/api-concepts/#receiving-resources-as-tables)
