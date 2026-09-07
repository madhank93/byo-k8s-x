---
title: Let an informer keep the cache
concepts: [informers, cache sync, event handlers, reflector]
---

## Core concept

The list-then-watch you just wrote is what an **informer** does for you — and
then it does the part you did not write: it keeps everything it has seen in a
local store, so the events stop being the data and start being a notification
that the data changed.

That distinction is the foundation of every controller. Events are unreliable
in a specific, bounded way: they can be replayed, they can be coalesced, and a
delete can reach you as a tombstone with no live object behind it. The store
is not. So handlers should do as little as possible — usually just note that
something needs looking at — and the real decision should read the store.

`WaitForCacheSync` marks the moment the store is trustworthy. Before it, "no
Website exists" and "nobody has told me yet" are indistinguishable, and a
controller that acts on that difference deletes things it should not.

## Go APIs

- `dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, resync, ns, tweak)`.
- `factory.ForResource(gvr).Informer()` and `.Lister()`.
- `informer.AddEventHandler(cache.ResourceEventHandlerFuncs{...})`.
- `factory.Start(ctx.Done())` then `cache.WaitForCacheSync(ctx.Done(), informer.HasSynced)`.
- `cache.DeletedFinalStateUnknown` — the tombstone a delete can arrive as.

## Hints

<details><summary>Nudge</summary>

`factory.Start` returns immediately; it launches the reflector in the
background. Nothing in the store is safe to read until `WaitForCacheSync`
returns true.
</details>

<details><summary>Approach</summary>

Set the resync period to 0 for now. A non-zero resync replays every object as
an update on a timer, which is a useful safety net later but noise while you
are watching the output.
</details>

<details><summary>Implementation</summary>

```go
if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc:    func(obj any) { fmt.Printf("add %s\n", objName(obj)) },
    UpdateFunc: func(_, obj any) { fmt.Printf("update %s\n", objName(obj)) },
    DeleteFunc: func(obj any) { fmt.Printf("delete %s\n", objName(obj)) },
}); err != nil {
    return err
}
```
</details>

## Further reading

- [client-go/tools/cache](https://pkg.go.dev/k8s.io/client-go/tools/cache)
- [Writing controllers: the client-go workqueue example](https://github.com/kubernetes/sample-controller/blob/master/controller.go)
