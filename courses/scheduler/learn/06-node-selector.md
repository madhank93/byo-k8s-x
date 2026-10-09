---
title: Only the nodes a pod asks for
concepts: [nodeselector, labels, kubelet-admission, nodeaffinity]
---

## Core concept

So far every node that is Ready and has room is as good as any other. A pod
can narrow that down itself. `spec.nodeSelector` is a map of label keys to
values, and a node is a candidate only if it carries **every** pair exactly:

```yaml
nodeSelector:
  disk: ssd
  zone: west
```

A node labelled `disk: ssd` alone does not match; one labelled `disk: ssd,
zone: west, gpu: "true"` does — extra labels on the node are fine, missing or
different ones are not. A pod with no selector matches every node.

This is a filter, the same shape as Ready and fit: it removes candidates and
never ranks them. If it removes all of them, the pod waits.

The kubelet checks this one too. Bind a pod to a node whose labels do not
match its selector and the API server accepts it, and then:

```
Pod was rejected: Predicate NodeAffinity failed: node(s) didn't match Pod's
node affinity/selector
```

The reason is `NodeAffinity` even though you used a selector: to the kubelet a
node selector is the simplest form of node affinity, which is the next stage.

A selector is a condition on placement, not on staying. Change a node's labels
after a pod is running there and nothing moves it; the name of the selector's
richer sibling, `requiredDuringSchedulingIgnoredDuringExecution`, says exactly
that.

For this stage the tester labels one worker for the length of the stage. Pods
that select that label must land on it, a pod whose selector matches no node
must be left waiting, and pods with no selector go anywhere they fit, as
before.

## Go APIs

- `pod.Spec.NodeSelector` (a `map[string]string`) and `node.Labels`.
- `labels.SelectorFromSet(pod.Spec.NodeSelector).Matches(labels.Set(node.Labels))`
  from `k8s.io/apimachinery/pkg/labels` — an empty set selects everything.

## Hints

<details><summary>Nudge</summary>

Does a node need *only* the labels the pod names, or *at least* them?
</details>

<details><summary>Approach</summary>

Add the selector check beside Ready and fit when you build the candidate list.
Build the selector from the pod's map once per pod, not once per node.
</details>

## Further reading

- [Assign pods to nodes: nodeSelector](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector)
- [Labels and selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
