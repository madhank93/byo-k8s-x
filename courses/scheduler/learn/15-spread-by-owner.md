---
title: Replicas that do not fail together
concepts: [spreading, ownerreferences, controllers, normalization]
---

## Core concept

A Deployment asks for five replicas because one machine is allowed to die. Your
scheduler, scoring only room and evenness, can put all five on the same worker
— and often will, because that worker is the emptiest right up until the moment
it holds all of them.

```
byok8s-worker    web-1 web-2 web-3 web-4 web-5
byok8s-worker2
byok8s-worker3
```

That is five replicas with the availability of one. Every scoring rule so far
looked at a node in isolation; this one is about the pods that are already
there and who they belong to.

**Who they belong to** is `metadata.ownerReferences`, and specifically the
entry with `controller: true`:

```yaml
ownerReferences:
  - apiVersion: apps/v1
    kind: ReplicaSet
    name: web-7d8f
    uid: 3f2a...
    controller: true
```

Two pods with the same controller uid are replicas of one thing. A pod with no
controller — one somebody created by hand — has no siblings and nothing to
spread from.

**Normalization is the new idea.** Every score so far could be worked out from
one node. This one cannot: three siblings on a node is terrible if the other
nodes have none and unremarkable if they each have three of their own. So count
first, across every node that survived filtering, then score against the worst:

```
worst = max siblings on any candidate node
score = 100                                  when worst = 0
      = (worst - siblings[node]) / worst * 100
```

The node carrying the most siblings scores 0, a node with none scores 100, and
when no node has any they all score the same and nothing is disturbed. That
last case is most of your cluster, which is why adding this score does not
change where anything else lands.

Count siblings, not pods. A node busy with other people's work is a question
for the resource scores, which already asked it.

## Go APIs

- `pod.OwnerReferences` is `[]metav1.OwnerReference`; `ref.Controller` is a
  `*bool`, so check for nil before dereferencing it.
- Compare `ref.UID`, not the owner's name: names are reused across a
  Deployment's rollouts, uids are not.
- Exclude the pod being scheduled from its own sibling count. It is in the
  cache too once it is bound, and on a retry you would be counting it against
  itself.

## Hints

<details><summary>Nudge</summary>

The five pods of a ReplicaSet all have something in common that the scheduler
can read. What is it, and which node has the fewest of them?
</details>

<details><summary>Approach</summary>

Find the controller uid of the pod being placed, count how many pods on each
candidate node share it, and score each node against the highest count you
found.
</details>

## Further reading

- [Scheduler configuration: the legacy SelectorSpread priority](https://kubernetes.io/docs/reference/scheduling/config/)
- [Owners and dependents](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/)
