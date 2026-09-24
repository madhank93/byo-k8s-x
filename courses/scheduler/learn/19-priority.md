---
title: Who gets the room first
concepts: [priorityclass, schedulingqueue, starvation, requeue]
---

## Core concept

Until now the order pods were tried in was whatever order they arrived in. That
is fine while there is room for everyone. The moment there is not, order *is*
the decision: the pod tried first gets the last seat.

Kubernetes writes importance down as a cluster-scoped object:

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: business-critical}
value: 1000
---
apiVersion: v1
kind: Pod
spec:
  priorityClassName: business-critical
```

**The pod never carries the number.** A pod names a class; the `Priority`
admission plugin looks the class up and writes its `value` into
`pod.Spec.Priority`. The API server refuses a pod that tries to set that field
itself. So your scheduler compares integers and never reads a PriorityClass at
all — `spec.priority` is already there by the time you see the pod.

A pod that named no class gets zero, or whatever class is marked
`globalDefault: true`. The two built-in classes,
`system-cluster-critical` and `system-node-critical`, are why a node's own
add-ons outrank your workload.

**This is about the queue, not the node.** Priority does not change which nodes
fit: filters and scores work exactly as they did. It changes *which pod is
asked first*, and that only matters when the answer is no for someone.

So the shape of the program changes. A scheduler that handles each pod as its
event arrives has no queue and therefore no order to get right. What it needs
is:

- an **active queue** of pods to try, ordered by priority, and by how long each
  has waited when priorities are equal;
- a set of pods that **found no node**, held aside rather than retried in a
  spin;
- one loop that takes the best pod, tries it, and either binds it or moves it
  aside — **one at a time**, consulting the queue again after every decision;
- a cluster change — the node event from stage 11 — moving everything held
  aside back into the queue.

**Take the best pod each time; do not sort a copy and walk it.** The two look
equivalent and are not. Room appears part-way through a pass — a node is
uncordoned, a pod is deleted — and a program working down a list it sorted
earlier hands that room to whoever it happens to have reached. Asking the queue
again after each bind is what keeps the answer current.

**Equal priority is settled by waiting time, not by chance.** Without that
tiebreak a steady stream of same-priority pods can leave one of them passed
over indefinitely, which is the starvation the real queue's
`creationTimestamp` ordering exists to prevent.

The piece deliberately missing here: a high-priority pod that fits *nowhere*
still waits, however unimportant the pods occupying the nodes are. Taking room
away from them is preemption, and it is the next stage.

## Go APIs

- `pod.Spec.Priority` is a `*int32` — nil for a pod created before admission
  filled it in, so read it through a helper that returns 0 for nil.
- `pod.Spec.PriorityClassName` is the name the pod asked for. You do not need
  it, and a scheduler that looks the class up itself has a second source of
  truth that can disagree with the one admission already resolved.
- `pod.CreationTimestamp` is a `metav1.Time`; compare with
  `a.CreationTimestamp.Time.Before(...)`.
- A `map` plus a scan for the best entry is enough at this size.
  `container/heap` is what the real `activeQ` uses, and is worth it when the
  queue is thousands long, not nine.
- The loop and the informer handlers run on different goroutines. The queue
  needs a mutex, and the loop needs something to block on — a buffered channel
  of size one, sent to on every push, is the lazy version of a condition
  variable.

## Hints

<details><summary>Nudge</summary>

Nothing about `pick` changes. What changes is who calls it, and when: move the
call out of the event handler and into a loop of your own.
</details>

<details><summary>Approach</summary>

Two maps behind one lock — queued and unschedulable — and a goroutine that
pops the best of `queued`, schedules it, and puts it in `unschedulable` when no
node fits. The node handler moves every entry back to `queued` and wakes the
loop.

Watch the gap: if the cluster changes while a pod is mid-attempt, that pod has
already been moved back and then parks itself again on a stale answer. Count
the moves, remember the count at pop, and queue the pod again if it changed.
</details>

## Further reading

- [Pod priority and preemption](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/)
- [The scheduling queue](https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/backend/queue/scheduling_queue.go)
