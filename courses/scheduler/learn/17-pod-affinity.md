---
title: Beside, or away from, other pods
concepts: [podaffinity, podantiaffinity, topologykey, filtering]
---

## Core concept

Node affinity, back in stage 7, asked about the node: its labels, nothing
else. Inter-pod affinity asks about the *company*: which pods are already
there.

```yaml
affinity:
  podAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels: {app: cache}
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels: {app: web}
```

Read it as: *put me where a pod labelled `app=cache` already runs, and never
where a pod labelled `app=web` does.* Affinity is the pod that wants to be
near its cache; anti-affinity is the pod that refuses to share a machine with
another copy of itself.

**Both are counts in a domain, not on a node.** The `topologyKey` works
exactly as it did in stage 16: every node sharing a value for that label is
one domain. With `kubernetes.io/hostname` the domain is the single node, which
is the common case; with `topology.kubernetes.io/zone` one matching pod
anywhere in the zone satisfies an affinity term, and forbids the whole zone
for an anti-affinity one.

The rule per term is then short:

- **affinity**: at least one matching pod in the candidate's domain, or the
  node is out.
- **anti-affinity**: no matching pod in the candidate's domain, or the node is
  out.

**Required means filter.** A pod requiring company goes to the node holding it
even when that node is the fullest one; a pod refusing company leaves the
emptiest node unused. The `preferred…` lists beside these are the scoring
version, with a `weight` each, and are not part of this stage.

**Terms are ANDed, unlike node affinity's `nodeSelectorTerms`**, which are
ORed. Every required term has to hold.

**Namespaces matter here** in a way they did not before. A term's `namespaces`
list says where to look, and an empty list means the pod's own namespace —
not the whole cluster. There is a `namespaceSelector` too, which this stage
leaves alone.

The first-pod problem is worth thinking through: five replicas that all
require `app=web` company, with none running yet, place nothing at all —
affinity finds no one to join, forever. Anti-affinity has no such trouble, and
is why it is the one people actually reach for.

## Go APIs

- `pod.Spec.Affinity.PodAffinity` and `.PodAntiAffinity`, each with
  `RequiredDuringSchedulingIgnoredDuringExecution []corev1.PodAffinityTerm`.
  Every level can be nil — a pod that asked for nothing.
- `corev1.PodAffinityTerm` carries `TopologyKey`, `LabelSelector`, and
  `Namespaces`.
- `metav1.LabelSelectorAsSelector` again. A **nil** selector matches nothing;
  an **empty** one (`{}`) matches every pod, which for anti-affinity means "no
  other pod at all here".
- The pod being placed is in your pod lister too, once bound. Skip it by UID,
  or a pod that selects its own labels refuses every node the moment it lands.

## Hints

<details><summary>Nudge</summary>

You already have the domain machinery from stage 16. The only new question is
how many selected pods a domain holds — and then whether you want that number
to be zero or non-zero.
</details>

<details><summary>Approach</summary>

Write one helper that counts the pods a term selects in the candidate's
domain, and call it twice: affinity fails when the count is zero, anti-affinity
fails when it is not.
</details>

## Further reading

- [Inter-pod affinity and anti-affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#inter-pod-affinity-and-anti-affinity)
- [InterPodAffinity plugin](https://github.com/kubernetes/kubernetes/tree/master/pkg/scheduler/framework/plugins/interpodaffinity)
