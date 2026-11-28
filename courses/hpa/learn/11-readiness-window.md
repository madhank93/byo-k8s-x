---
title: Do not trust a pod that is not ready
concepts: [pod readiness, unready pods, CPU warm-up]
---

## Core concept

A pod that has just started is often at its busiest: it loads caches,
compiles, and warms up. That burn says nothing about the load it will carry.
For CPU, then, an unready pod's usage is not evidence.

Split the target's pods before averaging:

- **ignored**: being deleted, or failed. Leave them out entirely.
- **unready**: `Pending`, or (for CPU) not `Ready`. Leave out their usage.
- **ready**: everything else with a metric.

Work out the ratio from the ready pods alone. Then:

- If it says **scale down**, or holds steady, use it as it is, with
  `ceil(ratio × ready pods)`.
- If it says **scale up**, count every unready pod as using **zero** and work
  the ratio out again. If the new ratio no longer says scale up, or is inside
  the tolerance, keep `currentReplicas`. Otherwise use
  `ceil(newRatio × pods counted)`.

Two ready pods at 100% of a 50% target say scale up. Two more are warming up
at 150%. Counted as idle, the four average 50%, a ratio of 1.0, so nothing
changes. The warm-up never gets to talk the autoscaler into more pods.

Report the first utilization, from the ready pods. That is what the pods
serving traffic are doing.

The real controller is a little gentler. During a pod's first five minutes it
also distrusts a sample taken too soon after the pod turned ready. "Not
`Ready`" is the core of the rule.

## Go APIs

- `pod.Status.Conditions`, the condition of type `corev1.PodReady`.
- `pod.DeletionTimestamp != nil` for a pod on its way out.

## Hints

<details><summary>Nudge</summary>

Keep the usage in a map by pod name, and the requests in another. Setting
`metrics[name] = 0` for each unready pod and calling the same utilization
function again is the whole second pass.
</details>

<details><summary>Approach</summary>

One loop over the target's pods builds three things: requests by pod name,
usage by pod name for the ready pods, and a list of unready names. Work the
ratio out from the usage map and decide. Only when the decision is to scale
up with unready pods present do you add zeros and decide again. Guard
against a reversal: if the second ratio falls to 1.0 or below, keep
`currentReplicas`.
</details>

## Further reading

- [Algorithm details](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details)
