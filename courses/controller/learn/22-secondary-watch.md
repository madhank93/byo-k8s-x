---
title: Watch what you own
concepts: [secondary resources, owner mapping, event handlers, fast convergence]
---

## Core concept

The timer bounds how long the world can be wrong. Watching your children makes
it right almost immediately: someone deletes the Deployment, an event arrives,
and the pass that recreates it runs in the time it takes to hear about it.

The mapping is the trick. The event is about a Deployment; the queue holds
Website keys. So a child event is turned into its **owner's** key by reading
the controller owner reference, and the pass that follows is the ordinary one —
it re-reads the Website and rebuilds what should exist. No special "child was
deleted" path exists, and none is needed.

This is also why owner references were worth setting properly. They are not
only for garbage collection: they are the edge that lets any child point back
at the one object responsible for it.

## Go APIs

- `informers.NewSharedInformerFactoryWithOptions(clientset, resync, informers.WithNamespace(ns))`
  for typed informers on Deployments.
- `metav1.GetControllerOf(obj)` → the owner reference, or nil for something
  that is not yours.
- The same `queue.Add(key)` the primary handlers use.

## Hints

<details><summary>Nudge</summary>

Filter on the owner's `Kind`, not just on its existence. A Deployment owned by
something else must map to nothing at all.
</details>

<details><summary>Approach</summary>

Handle deletes as well as updates, and remember the tombstone: a delete can
arrive as `cache.DeletedFinalStateUnknown` wrapping the last known object.
</details>

<details><summary>Implementation</summary>

```go
enqueueOwner := func(obj any) {
    child, ok := unwrap(obj)
    if !ok {
        return
    }
    owner := metav1.GetControllerOf(child)
    if owner == nil || owner.Kind != kind {
        return
    }
    queue.Add(child.GetNamespace() + "/" + owner.Name)
}
```
</details>

## Further reading

- [controller-runtime: Owns()](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/builder#Builder.Owns)
- [client-go informers](https://pkg.go.dev/k8s.io/client-go/informers)
