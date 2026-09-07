---
title: Look everywhere at once
concepts: [namespaces, rest api, output formats]
---

## Core concept

`-A` is not a client-side filter and it is not a loop over namespaces. It is a
*different URL*: drop the `/namespaces/{ns}` segment and the same list endpoint
returns objects from the whole cluster.

```
  /api/v1/namespaces/web/pods   →  one namespace
  /api/v1/pods                  →  every namespace
```

In client-go that difference is spelled as the empty namespace string, which is
why the empty string here is a request rather than a fallback — worth a comment
in your code, because it reads like a missing value.

The output changes too. Once rows can come from anywhere, the namespace stops
being context and becomes data, so a NAMESPACE column goes in front. That is a
general rule for tables: a column earns its place when the value varies across
the rows.

## Go APIs

- The same `List` call with `""` as the namespace.
- Building the header and each row as `[]string` so the extra column is an
  `append`, not a second format string.

## Hints

<details><summary>Nudge</summary>

Think about the URL the client builds before you think about the code.
</details>

<details><summary>Approach</summary>

Set the namespace to `""` when the flag is set, and prepend `"NAMESPACE"` to
the header and `o.GetNamespace()` to each row. Cluster-scoped resources ignore
the namespace entirely, which is why this works uniformly.
</details>

## Further reading

- [Namespaces and the API](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-uris)
