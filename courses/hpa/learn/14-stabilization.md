---
title: Hold the highest recent recommendation
concepts: [stabilization window, behavior, scale-down delay]
---

## Core concept

Load comes in waves. A dip of thirty seconds between two peaks is not a
reason to shed half the pods, only to start them again a minute later. So the
autoscaler remembers what it has recommended:

```yaml
behavior:
  scaleDown:
    stabilizationWindowSeconds: 300   # the default
  scaleUp:
    stabilizationWindowSeconds: 0     # the default
```

Keep every raw recommendation, the replica count straight from the metrics,
with the time you made it. Then:

```
low  = the lowest recommendation within the scale-up window
high = the highest recommendation within the scale-down window
desired = currentReplicas, raised to low, lowered to high
```

After a peak that wanted 16, a dip that wants 2 leaves `high` at 16 for the
next twenty seconds of a 20s window. `currentReplicas` is already below that,
so nothing changes. Once the peak ages out of the window, the low
recommendation stands.

Store the raw number, before clamping to `maxReplicas`. A window full of 8s
because 8 is the maximum would forget how far over the load really was.

## Hints

<details><summary>Nudge</summary>

The history belongs to one Autoscaler. Key it by the object's UID, so an
Autoscaler deleted and created again under the same name starts clean.
Drop entries older than the longer window.
</details>

## Further reading

- [Stabilization window](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#stabilization-window)
