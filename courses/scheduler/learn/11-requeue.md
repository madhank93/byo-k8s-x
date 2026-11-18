---
title: Try again when the cluster changes
concepts: [requeue, unschedulable, informer handlers, node events]
---

## Core concept

Cordon every node, start your scheduler, and create a pod. It says what you
taught it to say last stage:

```
waiting default/later: no node fits
```

Now uncordon the nodes. Nothing happens. The pod stays `Pending` — five
seconds, thirty seconds, forever — on a cluster with three idle workers.

Your scheduler decided once, when the pod arrived, and threw the answer away.
`AddFunc` fired, `pick` found nothing, the handler returned. No node has told
it anything since, because it never asked to be told.

This is the difference between a program that schedules and a scheduler. The
real one keeps two queues: an *active* queue of pods to try, and an
*unschedulable* queue of pods that have already failed. A pod is not stuck in
the second one — it is moved back to the first whenever something happens that
could plausibly change the answer: a node added, a node updated, a pod
deleted. The scheduler calls these cluster events, and the move is called a
requeue.

You need the small version of that:

- Remember the pods you could not place.
- Watch nodes for **add** and **update**, not just pods.
- When a node changes, try the remembered pods again.

The node informer is already there — you list nodes through its lister. It has
handlers too, and an uncordon is an update to the node's `spec.unschedulable`,
so the update handler fires with the node that just opened up.

Two things worth getting right:

- **A pod that has since been placed, or deleted, must be forgotten.** Retrying
  a pod that already has a node earns you a `409` from the Binding call, and
  retrying a deleted one is noise. Drop a pod from the waiting set once you
  bind it.
- **Handlers run on more than one goroutine.** The node handler and the pod
  handler touch the same waiting set, so it needs a mutex — the single-handler
  reasoning from stage 3 does not hold any more.

A retry is a real attempt, not a queue entry: run the same filters against the
current nodes and bind if one fits now.

## Go APIs

- `nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: …, UpdateFunc: …})`
  — `UpdateFunc` takes `(oldObj, newObj any)`.
- `sync.Mutex` around the waiting set; `map[string]*corev1.Pod` keyed by
  `namespace/name` is enough.
- The pods you remember can go stale; re-read the pod before binding, or accept
  that a bind may fail and drop it from the set either way.

## Hints

<details><summary>Nudge</summary>

What in the cluster could make "no node fits" stop being true? Who tells you
when that happens?
</details>

<details><summary>Approach</summary>

Keep a map of the pods you left waiting. Add a handler to the node informer,
and on add or update, walk that map and try each pod again. Remove a pod from
the map when it binds.
</details>

## Further reading

- [Scheduling framework: the scheduling queue](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/)
- [Node taints and cordoning](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/)
