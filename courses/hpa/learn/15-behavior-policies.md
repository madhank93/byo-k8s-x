---
title: Limit how fast it grows
concepts: [scaling policies, selectPolicy, periodSeconds]
---

## Core concept

Even a correct recommendation can be too sudden. Going from 2 pods to 200 at
once can flatten a database. Scaling policies cap the change per period:

```yaml
behavior:
  scaleUp:
    selectPolicy: Max            # Max (the default), Min, or Disabled
    policies:
    - {type: Pods,    value: 2,  periodSeconds: 60}   # at most 2 more per minute
    - {type: Percent, value: 50, periodSeconds: 60}   # at most 50% more per minute
```

Each policy gives a limit, counted from the replicas the target had **when the
period began**, not from now:

```
Pods     start + value
Percent  ceil(start × (1 + value/100))
```

From 2 replicas, the policies above allow 4 and 3. `Max` takes the most
generous, 4. `Min` takes the least, 3. `Disabled` forbids scaling that way at
all. Scaling down mirrors this: `start − value`, and the most generous limit
is the lowest.

Counting from the start of the period is what makes it a rate. Counting from
the current replicas would let a policy of "2 per minute" add 2 every sync.

To know the start, record every scaling you make, with its time and the
replicas before it. The start of a period is the "before" of the oldest
scaling inside it, or the current replicas if there was none.

With no `behavior`, the real defaults apply. Scaling up allows 4 pods or
100% per 15s, whichever is more, and scaling down allows 100% per 15s.

## Hints

<details><summary>Nudge</summary>

Apply the limit after stabilization and before clamping to
`[minReplicas, maxReplicas]`. A limit never forces a change, so going up it
is `min(desired, max(current, limit))`.
</details>

## Further reading

- [Scaling policies](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#scaling-policies)
