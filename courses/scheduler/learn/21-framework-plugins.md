---
title: Every check with a name on it
concepts: [schedulingframework, extensionpoints, plugins, weights, snapshot]
---

## Core concept

You have twenty stages of scheduling logic in one long `if`. It works, and it
cannot tell you anything: when a pod goes nowhere, the answer is "no node
fits", which is the one thing the person reading already knew.

The real scheduler is not built from a long condition. It is built from
**plugins**: small named things, registered at **extension points**, run in a
fixed order by a framework that knows nothing about scheduling.

```go
type filterPlugin struct {
    name  string
    allow func(pod *corev1.Pod, node *corev1.Node, s snapshot) bool
}

type scorePlugin struct {
    name   string
    weight int64
    score  func(pod *corev1.Pod, node *corev1.Node, s snapshot) int64
}
```

Nothing in your logic changes. Where it lives does, and three things fall out
of that which the long `if` could not give you.

**A rejection has a name.** Each node is counted against the *first* plugin to
turn it down, and the counts become the message on the pod:

```
0/4 nodes are available: 2 NodeResourcesFit, 1 NodeUnschedulable, 1 TaintToleration.
```

That is the line everyone who has run `kubectl describe pod` on a pending pod
has read. It is only writable because every check has a name and the nodes it
refused are counted under it.

**A score has a weight.** The sum stops being `a + b + c + d` and becomes
`Σ weightᵢ × scoreᵢ`. With every weight at 1 the behaviour is what it was —
and the weights are the whole of what a scheduling *profile* changes, which is
the next stage.

**Every plugin reads the same snapshot.** The nodes and pods are listed once
per scheduling cycle and handed to all of them, so two plugins cannot disagree
about what is on a node because one of them listed a moment later.

### The order is part of the contract

The filters run cheapest-and-most-absolute first, and a node stops at the
first no. Use this order, so the counts in your message are the same ones this
stage expects:

`NodeReady` · `NodeUnschedulable` · `TaintToleration` · `NodeAffinity` ·
`VolumeBinding` · `PodTopologySpread` · `InterPodAffinity` · `NodePorts` ·
`NodeResourcesFit`

Two notes on those names. `NodeUnschedulable` comes **before**
`TaintToleration`, as in the real scheduler: cordoning a node sets
`spec.unschedulable` *and* adds a taint, and it is reported as closed rather
than as tainted. `NodeResourcesFit` covers cpu and memory only — host ports are
`NodePorts`, a separate plugin, which is why this stage splits the `fits` you
wrote in stage 4 in two.

`NodeReady` is this course's own: real Kubernetes has no such plugin, because
an unready node carries a `node.kubernetes.io/not-ready` taint and
`TaintToleration` handles it.

### The extension points you now have, and the ones you do not

`Filter` and `Score` are two of about a dozen. The others are worth knowing by
name even though this course leaves them out: `PreFilter` (compute once per
pod, not once per node), `PreScore`, `Reserve` and `Unreserve` (claim a
resource before binding, give it back if the bind fails), `Permit` (hold a pod
back — gang scheduling lives here), `PreBind`, `Bind` and `PostBind`.
`PostFilter` you already wrote: preemption is a `PostFilter` plugin.

One shape is worth copying even when you do not need the rest: a real plugin
returns a `*framework.Status` rather than a bool, carrying a code
(`Success`, `Unschedulable`, `Error`, `UnschedulableAndUnresolvable`) and a
message. The last code is the interesting one — it means *retrying will not
help*, and it is how the queue knows a pod is not worth waking for a node
update.

## Go APIs

- A slice of structs with a name and a func field is the whole registry. Go's
  method values and closures mean you do not need an interface for this.
- `maps.Keys` with `slices.Sorted` gives a deterministic order over the counted
  reasons; sort by count descending with `slices.SortStableFunc` and
  `cmp.Compare` so the message reads the same way twice.
- Keep the snapshot a value, not a pointer, if plugins should not be able to
  change what the next one sees.
- The scores still return 0–100 each. Weights multiply; they do not normalise.

## Hints

<details><summary>Nudge</summary>

Do not rewrite any of the logic. Every function you need already exists — give
each one a name and a place in a slice, and replace the long condition with a
loop over that slice.
</details>

<details><summary>Approach</summary>

Two package-level slices, `filters` and `scores`. One `runFilters` that returns
the name of the first plugin to refuse, or the empty string. In the scheduling
cycle: count the names as you go, and when nothing survives, format the counts
into the note for the `FailedScheduling` event you have written since stage 10.
</details>

## Further reading

- [Scheduling framework](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/)
- [The framework interface](https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/framework/interface.go)
