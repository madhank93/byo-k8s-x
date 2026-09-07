---
title: Read from the cache
concepts: [lister, informer cache, api load]
---

## Core concept

Once the informer holds the objects, asking the API server for them again is
pure waste. A **lister** reads the informer's store: no network, no
rate-limiting, no chance of being throttled at the moment a cluster is busiest
and your controller matters most.

The cost is that the cache is *eventually* consistent. It is a snapshot from
whenever the last event arrived, which is fine for deciding what to do and
wrong for the write itself — that is why later stages read fresh before
updating status. Read from the cache; write with a fresh copy or a patch.

## Go APIs

- `factory.ForResource(gvr).Lister()` → `cache.GenericLister`.
- `lister.List(labels.Everything())` for everything in the cache.
- `lister.ByNamespace(ns).Get(name)` for one object, returning a
  `NotFound` error you can test with `apierrors.IsNotFound`.

## Hints

<details><summary>Nudge</summary>

The handlers run after the store has been updated, so a count taken inside a
`DeleteFunc` already excludes the object that was deleted.
</details>

<details><summary>Implementation</summary>

```go
known, err := lister.List(labels.Everything())
if err != nil {
    return err
}
fmt.Printf("cache %d\n", len(known))
```
</details>

## Further reading

- [Lister](https://pkg.go.dev/k8s.io/client-go/tools/cache#GenericLister)
- [labels.Everything](https://pkg.go.dev/k8s.io/apimachinery/pkg/labels#Everything)
