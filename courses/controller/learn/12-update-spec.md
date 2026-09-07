---
title: Roll out a spec change
concepts: [desired state, rollouts, merge keys]
---

## Core concept

Editing the Website changes the wish, and the same repair loop that fixed a
hand-scaled Deployment now rolls out a new image. There is no separate "handle
an update" path — that is the payoff for writing the pass as a comparison
rather than as a reaction.

What you are doing is handing the difference to a controller one layer down.
Change the pod template and the Deployment controller creates a new ReplicaSet,
scales it up, scales the old one down, and respects whatever rollout strategy
is configured. Controllers stack: yours states intent at one level, and
something else turns that into intent at the next.

The merge key is the subtlety. A strategic merge patch matches list entries by
a declared key — for containers, `name` — so a patch that names the container
edits it instead of replacing the whole list with one entry.

## Go APIs

- `types.StrategicMergePatchType` with `containers: [{name: ..., image: ...}]`.
- `existing.Spec.Template.Spec.Containers[0].Image` to compare first.

## Hints

<details><summary>Nudge</summary>

Patch both fields in one call. Two patches mean two writes, two events and two
more passes for a single change.
</details>

<details><summary>Implementation</summary>

```go
patch := fmt.Sprintf(
    `{"spec":{"replicas":%d,"template":{"spec":{"containers":[{"name":"web","image":%q}]}}}}`,
    replicas, image)
```
</details>

## Further reading

- [Deployment rollouts](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#updating-a-deployment)
- [Strategic merge patch](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-api-machinery/strategic-merge-patch.md)
