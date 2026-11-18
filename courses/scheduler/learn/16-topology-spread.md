---
title: Spread the pod asked for
concepts: [topologyspreadconstraints, maxskew, domains, filtering]
---

## Core concept

Stage 15 spread replicas because you decided to. This one spreads them because
the pod said to, and said exactly how:

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels: {app: web}
```

Read it as: *counting pods labelled `app=web`, no zone may end up with more
than one more than the emptiest zone.*

**The unit is the domain, not the node.** A `topologyKey` names a node label,
and every node sharing a value for it is one domain. Two workers in `zone=near`
are one domain holding both their pods; a single worker in `zone=far` is
another. Skew is counted between domains, so moving a pod between two nodes of
the same zone changes nothing.

**Skew is what the placement would cause:**

```
skew = (matching pods in the candidate's domain) + 1 − (fewest in any domain)
```

with the counts taken before this pod lands. `near` holding 2 and `far`
holding 0: placing in `near` gives 2+1−0 = 3, over a maxSkew of 1, so `near` is
out. Placing in `far` gives 0+1−0 = 1, which is allowed.

**`DoNotSchedule` is a filter, not a score.** This is the part that catches
people. Everything since stage 12 has been a preference — a node that scores
badly still gets the pod when nothing better exists. A required constraint
removes the node from the list entirely, and a pod with no nodes left waits,
exactly as one that fits nowhere does. `ScheduleAnyway` is the preference
version, and belongs with the scores.

**A node with no value for the key is in no domain at all.** It is not an empty
domain and it is not a free pass: a pod spreading by `zone` cannot go to a node
that has no `zone` label. Treating the missing label as "no constraint here" is
the usual bug, and it quietly puts every pod on the one unlabelled node.

Count only pods in the pod's own namespace, and match them with the
constraint's own `labelSelector` — not the pod's labels, though they are
usually the same thing.

## Go APIs

- `pod.Spec.TopologySpreadConstraints` is `[]corev1.TopologySpreadConstraint`,
  with `MaxSkew int32`, `TopologyKey string`, `WhenUnsatisfiable`, and
  `LabelSelector *metav1.LabelSelector`.
- `metav1.LabelSelectorAsSelector(c.LabelSelector)` turns it into the
  `labels.Selector` you have used since stage 6. A nil selector matches
  nothing, which `labels.Nothing()` gives you.
- Build the domain counts from *all* nodes, not just the candidates: a domain
  holding nothing is invisible otherwise, and it is the one that makes a skew
  too large.

Left out here, and worth knowing they exist: `minDomains`,
`nodeAffinityPolicy` and `nodeTaintsPolicy`, which decide whether nodes a pod
could not use anyway still count as domains.

## Hints

<details><summary>Nudge</summary>

This is the first thing since stage 9 that can leave a pod with nowhere to go.
Where in your program do nodes get removed rather than ranked?
</details>

<details><summary>Approach</summary>

For each required constraint, group the nodes by their value for the topology
key, count the matching pods per group, and drop any candidate whose group
would exceed the fewest by more than maxSkew — along with every node that has
no value for the key.
</details>

## Further reading

- [Pod topology spread constraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/)
- [Well-known labels: topology.kubernetes.io/zone](https://kubernetes.io/docs/reference/labels-annotations-taints/)
