---
title: Even, not only empty
concepts: [scoring, balancedallocation, plugins, resources]
---

## Core concept

Your scheduler picks the node with the most room. Here are two nodes, both with
room, and a pod that asks for 550m of CPU and 440Mi of memory:

```
worker    cpu 6600m/11000m used   memory      0 / 8.8Gi used
worker2   cpu 3850m/11000m used   memory 3.1Gi / 8.8Gi used
```

`LeastAllocated` averages what is free on each: `worker` is 35% free on CPU and
95% free on memory, which averages 65. `worker2` is 60% free on both, which
averages 60. So the pod goes to `worker` — the node whose CPU is nearly gone.

The memory nobody is using flattered the average. Put a few more of those pods
there and the CPU runs out while 8Gi of memory sits idle, unusable, because
nothing can be placed beside it.

`BalancedAllocation` is the score that notices. It ignores how much is left and
looks at how *alike* the usage is:

```
score = 100 - |cpu% - memory%|      (both percentages counted with this pod on the node)
```

A node evenly 40% through both scores 100. A node 65% through its CPU and 5%
through its memory scores 40. The aim is a cluster whose resources run out
together rather than one at a time.

Neither score is right alone:

- **Room alone** fills the lopsided node, as above.
- **Evenness alone** is perfectly happy with a node that is evenly 95% full —
  it scores 100, the same as an empty one.

So real schedulers **add them**. Each scoring plugin returns its own number for
a node, the framework sums them, and the highest total wins. That is the whole
mechanism, and you already have the shape of it: one more function, added to
the score you already compute.

```
node = argmax( leastAllocated(pod, node) + balancedAllocation(pod, node) )
```

Keep the tie-break rotation from stage 12. Two nodes can now reach the same
total by different routes, and equal totals still have to spread.

## Go APIs

- Nothing new. `node.Status.Allocatable`, the `requests` helper from stage 4,
  and integer arithmetic — the same pieces stage 12 used.
- The per-node "what is already asked of it" loop is now wanted by two scores.
  Lift it into a small helper rather than writing it twice.
- Memory quantities are large: use `Value()` or `MilliValue()` on an `int64`
  and scale to 100 before dividing, not after.

## Hints

<details><summary>Nudge</summary>

A node with 35% of its CPU left and all of its memory free has plenty of room.
What is wrong with putting a CPU-and-memory pod there?
</details>

<details><summary>Approach</summary>

Write a second score returning `100 - |cpuPercent - memoryPercent|` with this
pod counted in, add it to the least-allocated score, and pick the highest sum.
</details>

## Further reading

- [Scheduler configuration: NodeResourcesBalancedAllocation](https://kubernetes.io/docs/reference/scheduling/config/)
- [Scheduling framework: the score extension point](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/)
