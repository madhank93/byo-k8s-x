---
title: Making room, not waiting for it
concepts: [preemption, victims, nominatednodename, eviction]
---

## Core concept

Stage 19 decided who gets room *when some appears*. Preemption is the other
half: an important pod that fits nowhere does not wait for a cluster that may
never free anything. It takes the room.

In the real scheduler this is a `PostFilter` plugin — it runs only after every
filter has said no, which is the first thing to internalise. **Preemption is
not a way to choose a node. It is what happens when there is no node.** A pod
that fits somewhere never preempts anything, however important it is.

The pass, per node:

1. **Is this node otherwise suitable?** Taints, selectors, volumes, cordon:
   evicting pods does not change any of them, so a node that failed those
   filters is not a candidate whatever you remove from it.
2. **Who on it matters less?** Only pods with a strictly lower
   `spec.priority`. Equal priority is not lower priority — if it were, two
   replicas of one Deployment would take turns evicting each other forever.
3. **How few of them answer the request?** Take the least important first and
   stop the moment the pod fits. The point is the smallest harm that works, not
   an empty node.

Then pick the node needing the fewest victims, delete them, and let the pod be
scheduled by the ordinary path once they are gone.

**You do not bind the pod here.** Deleting a pod starts a shutdown; the pod is
gone seconds later, and the node's room is not yours until it is. The
scheduler asks again when the deletions arrive — which means a **pod deletion
has to wake the queue**, exactly as a node change does in stage 11. Without
that handler the victims die and nothing comes to take their place.

**`status.nominatedNodeName` is how the room is held.** Between the eviction
and the bind, the node looks empty to everyone — including your own next
scheduling cycle, and any other scheduler. Writing the nomination onto the pod
says *this room is spoken for*, and a scheduler that counts a nominated pod
against its nominated node will not hand the same room out twice.

Two traps live in that gap, and both cost a re-run to find:

- **A pod must not count itself.** Once nominated, the pod appears on the node
  in your own accounting, so its own 6 CPUs are in the way of its own 6 CPUs.
  Filter the pod being scheduled out of the placed list.
- **A pod must not preempt twice.** Victims take a moment to terminate. Every
  attempt in between sees the node just as full as the first one did, and a
  scheduler that simply runs preemption again empties a *second* node for the
  same pod. If the room you were promised is still being cleared — a pod on
  your nominated node with a `deletionTimestamp` — wait for it.

What a real scheduler does that this stage does not: honour
`PodDisruptionBudget`s when choosing victims, re-run the filters with the
victims removed (a pod affinity term may have been satisfied *by* a victim),
prefer victims whose loss is cheapest across several dimensions, and respect
`preemptionPolicy: Never` on a pod that wants high priority in the queue but
refuses to displace anyone.

## Go APIs

- `pod.Status.NominatedNodeName` is a plain string on the *status*, so it is
  written with `UpdateStatus` (or a status patch), not `Update`.
- `pod.DeletionTimestamp != nil` is a pod on its way out. It is still in the
  informer cache, still counted by your `fits`, and still answering a `Get`.
- Eviction here is `Pods(ns).Delete(...)`. The real scheduler deletes too — the
  Eviction API is what `kubectl drain` uses, and it is the one that consults
  PodDisruptionBudgets.
- `slices.SortFunc` with `cmp.Compare(priorityOf(a), priorityOf(b))` orders the
  candidates; `slices.DeleteFunc` on a clone gives you "the pods still there if
  these went" without touching the informer's copy — never mutate an object
  from a lister.

## Hints

<details><summary>Nudge</summary>

You already have a function that says whether a pod fits a node given a list of
pods on it. Preemption is that same function asked repeatedly, with a shorter
list each time.
</details>

<details><summary>Approach</summary>

Split your filter chain in two: the part eviction cannot change (taints,
selectors, affinity, volumes) and `fits`. Preemption walks nodes that pass the
first part, removes lower-priority pods one at a time in priority order until
`fits` turns true, and keeps the node whose answer needed the fewest.

Then: nominate, delete the victims, and return without binding. The delete
handler on your pod informer brings the pod back when the room is real.
</details>

## Further reading

- [Pod priority and preemption](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/)
- [The preemption plugin](https://github.com/kubernetes/kubernetes/tree/master/pkg/scheduler/framework/plugins/defaultpreemption)
