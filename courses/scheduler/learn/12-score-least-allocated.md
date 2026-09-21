---
title: Not just anywhere it fits
concepts: [scoring, leastallocated, requests, allocatable]
---

## Core concept

Pin a pod to one worker so that node is carrying 8000m of its 11000m, then
hand your scheduler three small pods. Watch where they go:

```
small-1   byok8s-worker2     (10800m free)
small-2   byok8s-worker3     (10800m free)
small-3   byok8s-worker      (2800m free)
```

The third one went to the fullest node in the cluster while two nearly empty
ones sat there. Nothing is broken — it *fits*, and your filters said yes. But
fitting is a yes-or-no question, and you have been treating it as the whole
answer.

A real scheduler runs in two halves. **Filtering** removes the nodes that
cannot take the pod — everything you built in stages 3 through 9. **Scoring**
ranks the ones that are left and picks the best. Filtering is a predicate;
scoring is a preference.

The default strategy is `LeastAllocated`: prefer the node with the most room
left over once this pod is on it.

```
score = ((allocatable - requested) / allocatable) averaged over cpu and memory
```

`requested` is what the pods already on that node asked for, plus what this pod
asks for. Not usage — requests. A node running an idle pod that reserved 8 CPUs
is 8 CPUs full, however little it is actually doing, because that is the promise
the cluster made.

Two details worth getting right:

- **Score every node you kept, then take the best** — do not stop at the first
  one that fits. The point of scoring is comparison.
- **Ties still have to spread.** Pods with no resource requests score
  identically on an empty cluster, and if you break ties by name every one of
  them lands on the same node. Keep taking tied nodes in turn, the way stage 3
  did — the rotation is now the tie-break rather than the whole decision.

A pod with no requests scores every node equally, which is exactly right: you
told the cluster nothing about what it needs, so nothing can be preferred.

One thing your scores depend on that is not obvious: `Bind` returns before the
pod comes back to you through the watch, so a scheduler that binds
asynchronously scores its next pod against a node that still looks empty. The
real scheduler keeps what it has bound in an *assume cache* and counts it until
the informer catches up. Binding on the same goroutine that decides, as you do
here, closes that window — each bind has returned before the next node is
scored — which is why the arithmetic above is enough.

## Go APIs

- `node.Status.Allocatable[corev1.ResourceCPU]` and `…ResourceMemory`, both
  `resource.Quantity` — `MilliValue()` gives you an `int64` to do arithmetic on.
- The `requests` helper you wrote in stage 4 already sums a pod's container
  requests; reuse it for both the candidate pod and the pods already placed.
- Integer arithmetic is enough: scale to 100 and compare, rather than reaching
  for floats.

## Hints

<details><summary>Nudge</summary>

You have a list of nodes that all fit. What makes one of them a better answer
than another?
</details>

<details><summary>Approach</summary>

Give each surviving node a score out of 100 from its free capacity after this
pod lands, keep the nodes sharing the highest score, and rotate among those.
</details>

## Further reading

- [Scheduler configuration: NodeResourcesFit scoring strategies](https://kubernetes.io/docs/reference/scheduling/config/)
- [Managing resources for containers: requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
