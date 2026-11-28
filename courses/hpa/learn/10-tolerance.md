---
title: Leave small differences alone
concepts: [tolerance, flapping]
---

## Core concept

Metrics are noisy. A service holding steady at its target reads a little
above it one sync and a little below it the next. If every wobble changed the
replica count, the autoscaler would add a pod, remove it, and add it again,
and every new pod would cost a cold start.

So a ratio close to 1.0 changes nothing:

```
if |1 − ratio| ≤ 0.1:   desired = currentReplicas
```

0.1 is the real controller's default (`--horizontal-pod-autoscaler-tolerance`).
Four pods at 54% against a 50% target is a ratio of 1.08, so they stay four.
At 58% the ratio is 1.16 and `ceil(4 × 1.16)` = 5.

The tolerance applies to the ratio, before rounding. A ratio of 1.05 on 40
pods would round up to 42. The tolerance is there to stop exactly that.

## Hints

<details><summary>Nudge</summary>

Return `currentReplicas` itself, not the recomputed count. The point is that
nothing moves.
</details>

## Further reading

- [Algorithm details](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details)
