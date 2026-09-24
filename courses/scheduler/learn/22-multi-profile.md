---
title: Two schedulers, one program
concepts: [profiles, schedulername, weights, fieldselector]
---

## Core concept

A cluster rarely wants one scheduling policy. Batch jobs want to be packed
tight so idle nodes can be handed back; services want to be spread so that one
machine's death is survivable. Running two schedulers to get two answers means
two caches, two queues, and two programs racing to bind the same node's last
CPU.

Kubernetes does not do that. One scheduler serves several **profiles**, and a
pod picks one by name:

```yaml
profiles:
  - schedulerName: byok8s
    pluginConfig:
      - name: NodeResourcesFit
        args: {scoringStrategy: {type: LeastAllocated}}
  - schedulerName: byok8s-packing
    pluginConfig:
      - name: NodeResourcesFit
        args: {scoringStrategy: {type: MostAllocated}}
```

```yaml
spec:
  schedulerName: byok8s-packing
```

**One cache, one queue, one set of filters.** The filters are the cluster's
rules — a taint is a taint, a volume is where it is — and they do not vary by
profile. What varies is the weighing: which scores are consulted, and how much
each one counts. That is why stage 21 had to come first. Once the scores are a
named, weighted list, a profile is just a different list.

**MostAllocated is LeastAllocated read backwards.** The same arithmetic over
requests and allocatable, subtracted from 100: prefer the node already in use,
so the empty ones stay empty and can be scaled away. Nothing else belongs in
that profile — spreading replicas would pull straight against it.

### The field selector has to go

Since stage 1 the informer has narrowed the stream server-side:

```go
fields.AndSelectors(
    fields.OneTermEqualSelector("spec.schedulerName", schedulerName),
    fields.OneTermEqualSelector("spec.nodeName", ""),
)
```

A field selector compares **one field to one value**. There is no "in" and no
"or", so it cannot say *either of these two names*. Keep the `spec.nodeName=""`
half — that is still one field and one value, and it is the half that matters
for volume — and decide in your own code which pods are yours.

That last part is not a detail. The selector was doing real work: it kept every
other scheduler's pods out of your program. Without it your handler sees every
unbound pod in the cluster, including the ones the default scheduler is about
to place, and a program that binds them all is a program that fights the
cluster. A pod naming a scheduler you do not serve is not yours — drop it
before it reaches the queue.

**Two schedulers may not both serve a name.** If you run this program while
another scheduler claims `byok8s-packing`, both will bind pods with that name,
and the loser's Binding is rejected for a pod that already has a node. Profile
names are cluster-wide, and the API server's refusal of a second binding is the
only thing that stops the damage.

The events are worth updating too: a pod's `reportingController` should say
which profile decided, not which binary. When someone asks why a pod is packed
onto a full node, the answer is in the name they asked for.

## Go APIs

- `map[string][]scorePlugin` keyed by scheduler name is the whole of it. The
  lookup doubles as the ownership check — `_, ours := profiles[name]`.
- `pod.Spec.SchedulerName` is always set by the time you see it: the API server
  defaults it to `default-scheduler`.
- `fields.OneTermEqualSelector("spec.nodeName", "")` still works, and the only
  fields the pod registry indexes for selection are a short list —
  `spec.nodeName`, `spec.schedulerName`, `spec.restartPolicy`,
  `status.phase` and `metadata.name`/`namespace`. A selector on anything else
  is rejected, not quietly ignored.
- `informers.WithTweakListOptions` applies to every informer that factory
  makes, which is why the nodes come from a second factory.

## Hints

<details><summary>Nudge</summary>

Nothing in the scheduling cycle changes except the line that sums the scores:
it reads the list belonging to the pod's own scheduler name instead of the one
global list.
</details>

<details><summary>Approach</summary>

Declare the profiles as a map from scheduler name to score list. In the pod
handler, look the pod's name up in that map and return early if it is not
there. In the cycle, score with the list you found.
</details>

## Further reading

- [Multiple profiles](https://kubernetes.io/docs/reference/scheduling/config/#multiple-profiles)
- [KubeSchedulerConfiguration](https://kubernetes.io/docs/reference/config-api/kube-scheduler-config.v1/)
