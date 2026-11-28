---
title: Several metrics, one answer
concepts: [multiple metrics, invalid metrics, safe scale-down]
---

## Core concept

An Autoscaler can list several metrics, say CPU and requests per second.
Each makes its own recommendation, and the **largest** wins. Whichever
resource runs out first is the one that needs the pods.

```
cpu at 100% of 50%           → 8
requests_per_second 15 of 10 → 6
desired                      → 8
```

When one metric cannot be read, the others still count, but only one way.
The unknown metric might have asked for more, so:

- If every metric failed, change nothing.
- If some failed and the rest say **scale down**, change nothing. A
  scale-down is only safe when every metric agrees.
- If some failed and the rest say **scale up**, scale up. More pods can
  only help.

"Change nothing" means `desiredReplicas` keeps its last value. Still report
the metrics you could read in `currentMetrics`. That is how anyone looking
at the Autoscaler sees what it knows.

## Hints

<details><summary>Nudge</summary>

Collect the errors rather than returning on the first. `errors.Join` turns
them into one for the log once you have decided.
</details>

## Further reading

- [Scaling on multiple metrics](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#scaling-on-multiple-metrics)
