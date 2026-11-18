---
title: Good enough, and moving on
concepts: [percentageofnodestoscore, throughput, sampling, latency]
---

## Core concept

Every stage so far has scored every node. On three workers that is free. On
five thousand it is the reason pods sit `Pending` while the scheduler works
through a list, and the pod at the back of the queue pays for the perfect
answer given to the pod at the front.

So the real scheduler stops early:

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
percentageOfNodesToScore: 10
```

*Find this share of the cluster's nodes that fit, then stop looking and bind
the best of those.* It is a deliberate trade: a worse node, chosen sooner.

**Feasible nodes found, not nodes examined.** The limit counts nodes that
passed every filter. A node the filters turn down is skipped and costs nothing
towards the quota — otherwise a run of tainted nodes would end the search with
nothing to show for it, and the pod would be declared unschedulable while half
the cluster sat idle.

**The scan has to move.** If every cycle started at the front of the list, the
same handful of nodes would be scored for every pod in the cluster and the rest
of it would stay empty. The next cycle carries on where the last one stopped,
wrapping around the end — so the sample sweeps the cluster rather than
resampling its first few names.

**And it only means anything over a stable order.** A lister hands objects back
in whatever order it holds them; sort the nodes before you index into them, or
"where the last one stopped" points somewhere new each time.

**Finding nowhere still means looking everywhere.** Stopping early is how you
place a pod. It is never how you conclude a pod cannot be placed: that verdict
is only honest after every node has said no, which is exactly what the
`0/N nodes are available` message claims. The two live in the same loop and end
differently — one stops when the quota is full, the other runs out of nodes.

The real formula is adaptive rather than a flat percentage. Below 100 nodes it
scores all of them; above that it scales the share down as the cluster grows,
with a floor of 100 nodes and 5%. So on a small cluster the default behaviour
is exactly what you have been doing since stage 12 — which is why this stage
sets the percentage by hand to see the difference at all.

## Go APIs

- `flag.Int("percentage-of-nodes", 100, …)`. A default of 100 keeps every
  earlier stage behaving as it did.
- `slices.SortFunc(nodes, func(a, b *corev1.Node) int { return cmp.Compare(a.Name, b.Name) })`
  gives the stable order the rotation needs. Sort your own slice — never the
  one a lister handed you, whose backing array belongs to the informer.
- `all[(start+i)%len(all)]` walks from an offset and wraps. Keep the cursor
  next to the tie-break counter from stage 3: both are read and written by the
  one goroutine that schedules, so neither needs a lock.
- Integer division truncates, so a small cluster with a small percentage gives
  zero. Floor it at one node, the way the real scheduler floors at a hundred.

## Hints

<details><summary>Nudge</summary>

The filter loop already walks every node and collects the ones that pass. It
needs two more things: where to start, and when to stop.
</details>

<details><summary>Approach</summary>

Work out how many feasible nodes you want before the loop. Walk the sorted
nodes from a saved offset, counting how many you looked at, and break once you
have enough candidates. Afterwards, advance the offset by the number examined,
modulo the node count.
</details>

## Further reading

- [Scheduler performance tuning](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduler-perf-tuning/)
- [`findNodesThatPassFilters`](https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/schedule_one.go)
