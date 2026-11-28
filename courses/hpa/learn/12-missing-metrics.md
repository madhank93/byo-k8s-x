---
title: Assume the worst about missing pods
concepts: [missing metrics, conservative scaling]
---

## Core concept

A scrape misses pods. A node is slow to answer, or a pod started between two
rounds. A pod with no metric is not idle and not busy. It is unknown, and the
safe guess depends on which way the others point:

- When the rest say **scale down**, assume the missing pod uses **100% of its
  request**. Shedding pods on the strength of the ones you can see could
  starve the one you cannot.
- When the rest say **scale up**, assume it uses **zero**. Adding pods for
  load you have not seen is how a cluster fills up.

This is the same second pass as the unready pods. Fill in the guesses, work
the ratio out again, and act only if it still points the same way.

```
3 pods at 10% of a 50% target, 1 missing   → scale down
missing counted at its request: (15+50)/200 → 32% → ratio 0.64
ceil(4 × 0.64) = 3
```

There is one more guard. If the second pass would scale down but ends above
`currentReplicas`, or scale up but ends below it, keep `currentReplicas`. A
guess should only ever soften a decision, never reverse it.

## Hints

<details><summary>Nudge</summary>

A pod is missing when it is ready, in the target's selector, and absent from
the metrics list. Build that set in the same loop that sorts pods into ready
and unready.
</details>

<details><summary>Approach</summary>

The second pass now has two inputs: missing pods filled with their request
or with zero, depending on the first ratio, and unready pods filled with
zero when scaling up. Recompute, then apply the two guards in order. First,
the new ratio inside the tolerance or on the other side of 1.0 keeps
`currentReplicas`. Second, a count that moved against the ratio's direction
keeps it too. Count the pods for `ceil(newRatio × n)` from the filled-in map,
not from the ready pods.
</details>

## Further reading

- [Algorithm details](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details)
