---
title: Ask what the server serves
concepts: [discovery, api groups, resources]
---

## Core concept

This is the request that makes a client *general*. Nothing in your program
knows what a Pod is: the server reports its groups, the versions in each, and
the resources in those — names, shortnames, kinds, and whether each is
namespaced. That listing includes CRDs installed five minutes ago, which is how
kubectl works with resources that did not exist when it was compiled.

Two details separate a working implementation from a correct one:

- **Preferred versions.** A group can serve several versions at once. Asking
  for the preferred one gives you the single version the server would pick, and
  the one-row-per-resource listing a reader expects.
- **Partial failure is normal.** An aggregated API whose backend is down fails
  its own group and no other. `ServerPreferredResources` returns *both* results
  and an error; discarding the results because an error came with them turns a
  degraded cluster into an unusable tool.

Subresources come back in this list too, spelled `pods/log`, `pods/exec`. They
are addressed through their parent and are not resources in their own right, so
filter out anything containing a slash.

## Go APIs

- `dc.ServerPreferredResources()` — `([]*metav1.APIResourceList, error)`, and
  yes, both can be non-empty.
- `r.ShortNames`, `r.Namespaced`, `r.Kind`, and `list.GroupVersion`.

## Hints

<details><summary>Nudge</summary>

Handle the error by asking "did I get anything at all?" rather than by
returning immediately.
</details>

<details><summary>Approach</summary>

```go
groups, err := dc.ServerPreferredResources()
if err != nil {
    if len(groups) == 0 { return err }
    fmt.Fprintln(os.Stderr, "warning: some groups did not respond:", err)
}
```

Then one row per resource whose `Name` has no `/` in it.
</details>

## Further reading

- [API groups and versioning](https://kubernetes.io/docs/reference/using-api/#api-groups)
- [discovery.DiscoveryInterface](https://pkg.go.dev/k8s.io/client-go/discovery#DiscoveryInterface)
