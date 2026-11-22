---
title: Run hooks in order
concepts: [hook weights, stable sort, stored vs execution order, typed annotations]
---

## Core concept

An event can have many hooks, and some must go first: the ServiceAccount
before the Job that uses it, the schema migration before the data one. A
hook says where it goes with a weight:

```yaml
metadata:
  annotations:
    "helm.sh/hook": pre-install
    "helm.sh/hook-weight": "-5"
```

**Execution order** for one event:

1. Lowest weight first, compared as numbers: `-10`, `2`, `10`, `100`.
2. Equal weights: by name, byte order (`a-cm` before `aa`).
3. Equal weight and name, which different kinds can share: the order kinds
   install in (stage 10), so a ServiceAccount `aa` goes before a ConfigMap
   `aa`.

The third rule comes free. Helm keeps hooks in kind-then-path order and
then sorts them *stably* by weight and name, so whatever the sort leaves
tied stays in kind order.

**The weight is `strconv.Atoi` of the annotation**, and an error is
swallowed as 0. So `"+3"` is 3, but `"1.5"`, `"abc"` and `" 1"` (a leading
space) all run at 0, among the hooks with no weight at all. The record
keeps the parsed number, and leaves `weight` out when it is 0.

**Stored order is not execution order.** The record's `hooks`, and so
`helm get hooks` and `helm template`, keep kind-then-path order. Sort a
copy to run them, not the record's list. `get hooks NAME
[--revision N]` prints each stored hook as

```
---
# Source: hw/templates/01-z-cm.yaml
<the hook's manifest>
```

**The annotation must be a string.** `helm.sh/hook-weight: 3`, unquoted,
renders and templates fine, but annotations are `map[string]string` to the
API. Helm finds out when it decodes the hook to delete it before creating
it, and the install fails, recorded as failed:

```
Error: INSTALLATION FAILED: failed pre-install: unable to build kubernetes object for deleting hook hwint/templates/x.yaml: unable to decode "": json: cannot unmarshal number into Go struct field ObjectMeta.metadata.annotations of type string
```

## Go APIs

- `sort.SliceStable` keeps ties in the order they came in; `sort.Slice`
  does not promise to.
- `strconv.Atoi` accepts a leading `+` or `-` and nothing else around the
  digits.
- `json.Unmarshal` into a `metav1.ObjectMeta` gives the API's own type
  error for a number where a string belongs.

## Hints

<details><summary>Nudge</summary>

If stage 17 already sorts by weight, check the tie-breaks: name, then
nothing, with the sort stable. A non-stable sort passes most of the time,
which is the worst way to be wrong.
</details>

<details><summary>Approach</summary>

The less function is two comparisons: weight, then name. `get hooks` reuses
whatever prints hooks for `template`.
</details>

## Further reading

- [Hook weights](https://helm.sh/docs/topics/charts_hooks/#hook-weights)
- [Helm's hook sorter](https://github.com/helm/helm/blob/v3.18.6/pkg/action/hooks.go)
