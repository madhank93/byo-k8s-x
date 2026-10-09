---
title: Nodes that say keep off
concepts: [taints, tolerations, noschedule, noexecute]
---

## Core concept

Every filter so far has been the pod's idea: its selector, its affinity, its
requests. A **taint** is the node's. It says "nothing runs here unless it says
otherwise", and the pod's answer is a **toleration**.

```yaml
# on the node
taints:
  - {key: dedicated, value: gpu, effect: NoSchedule}
# on the pod
tolerations:
  - {key: dedicated, operator: Equal, value: gpu, effect: NoSchedule}
```

A pod may be placed on a node only if **every** taint on that node is tolerated
by at least one of the pod's tolerations. One untolerated taint is enough to
rule the node out — which is why your own cluster's control plane has been a
candidate all course and should stop being one now: it carries
`node-role.kubernetes.io/control-plane:NoSchedule`, and nothing the tester
creates tolerates it.

A toleration matches a taint when the effects match and:

- `operator: Equal` (the default) — same key and same value.
- `operator: Exists` — same key, any value. With **no key at all**, it tolerates
  every taint, which is how DaemonSets run everywhere.
- An **empty effect** matches every effect of that key.

The three effects differ in who enforces them, and that is the part worth
remembering:

- `NoSchedule` — a filter, and nobody downstream enforces it. Bind a pod to a
  `NoSchedule` node it does not tolerate and the API server accepts it and the
  kubelet runs it, exactly as with resources and selectors. If your scheduler
  does not check, nothing does.
- `PreferNoSchedule` — not a filter at all: a preference, which belongs with
  the scoring stages later.
- `NoExecute` — a filter *and* an eviction. A pod already running on a node
  that gains an untolerated `NoExecute` taint is deleted, at once: the event
  reads `TaintManagerEviction: Marking for deletion`. (The 300-second grace
  people remember belongs to the built-in `not-ready` and `unreachable` taints,
  which the API server adds a toleration for; a taint of your own has no such
  delay unless the pod asks for `tolerationSeconds`.)

For this stage, treat `NoSchedule` and `NoExecute` as filters: a node is a
candidate only if the pod tolerates every taint it carries. The tester taints a
worker and checks that pods go elsewhere, that a pod tolerating it may still
land there, and — now that the rule is in — that nothing of yours lands on the
control plane.

## Go APIs

- `node.Spec.Taints`, each a `corev1.Taint{Key, Value, Effect}`.
- `pod.Spec.Tolerations`, each a `corev1.Toleration{Key, Operator, Value, Effect}`.
- `corev1.TaintEffectNoSchedule`, `…NoExecute`, `…PreferNoSchedule`;
  `corev1.TolerationOpExists`, `…OpEqual`.

## Hints

<details><summary>Nudge</summary>

Which node has been in your candidate list all course that no pod ever asked
for?
</details>

<details><summary>Approach</summary>

One function: does this pod tolerate this taint. Then a node passes when every
taint with effect `NoSchedule` or `NoExecute` is tolerated by some toleration
the pod carries. An empty toleration key means "any key"; an empty effect means
"any effect"; `PreferNoSchedule` is not your business yet.
</details>

## Further reading

- [Taints and tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/)
- [Taint-based evictions](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/#taint-based-evictions)
