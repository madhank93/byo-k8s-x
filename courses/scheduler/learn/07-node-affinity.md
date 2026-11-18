---
title: Rules richer than equality
concepts: [nodeaffinity, nodeselectorterms, matchexpressions, operators]
---

## Core concept

A node selector can only say "this label, with exactly this value". Node
affinity says more. Its required form lives at

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - {key: disk, operator: In, values: [ssd, nvme]}
            - {key: zone, operator: NotIn, values: [east]}
        - matchExpressions:
            - {key: gpu, operator: Exists}
```

and two rules decide how it combines:

- **Terms are ORed.** A node matches if *any one* term matches. Above, a node
  with a GPU qualifies whatever its disk or zone.
- **Expressions inside a term are ANDed.** A term matches only if *every* one
  of its expressions does.

Each expression takes an operator:

| Operator | A node matches when |
|---|---|
| `In` | it has the key, with one of the values |
| `NotIn` | it does not have the key with any of the values |
| `Exists` | it has the key, whatever the value |
| `DoesNotExist` | it does not have the key |
| `Gt`, `Lt` | it has the key, and the value, read as an integer, is greater or less than the single value given |

Read `NotIn` carefully, because it is the one that surprises people: a node
that does not carry the key at all **matches** `NotIn`. "Not in east" is true of
a node that has no zone. The same goes for `DoesNotExist`, by definition. A
missing key fails `In`, `Exists`, `Gt` and `Lt`.

The kubelet enforces all of this at admission, just as it did for the plain
selector — same reason, `NodeAffinity`. And if a pod has both a
`nodeSelector` and required node affinity, a node must satisfy **both**.

A term with no expressions matches nothing — the kubelet rejects a pod bound by
one — and the API refuses a required affinity with no terms at all. Neither is
a way to say "anywhere"; leave the affinity out for that.

The `preferred...` half of node affinity is not a filter at all but a score,
and it waits for the scoring stages. For this one, implement the required
half as another filter beside Ready, fit and the selector.

## Go APIs

- `pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution`
  — every pointer on that path may be nil, and nil means no constraint.
- `corev1.NodeSelectorTerm.MatchExpressions`, each a
  `corev1.NodeSelectorRequirement{Key, Operator, Values}`.
- `corev1.NodeSelectorOpIn`, `…OpNotIn`, `…OpExists`, `…OpDoesNotExist`,
  `…OpGt`, `…OpLt`; `strconv.ParseInt` for the last two.

## Hints

<details><summary>Nudge</summary>

A node with no `zone` label: is it "not in east"?
</details>

<details><summary>Approach</summary>

Three small functions: one expression against a node's labels, one term (all
of its expressions), and the whole affinity (any of its terms, with nil
meaning "matches"). Call the last one from the candidate filter.
</details>

## Further reading

- [Assign pods to nodes: node affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity)
- [NodeSelectorRequirement (API reference)](https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/node-selector-requirement/)
