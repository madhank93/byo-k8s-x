---
title: The way everything finds everything
concepts: [labels, label-selectors, set-based-requirements, loose-coupling]
---

## Core concept

A field selector asks about what the server indexes. **A label selector asks
about what the client wrote**, and that one difference explains everything else
about it: the server cannot have a list of acceptable keys, the match is a scan,
and the syntax is richer to make the scan worth doing.

```
?labelSelector=app=web                  equality
?labelSelector=app!=web                 inequality — and objects with no app label
?labelSelector=app                      existence
?labelSelector=!app                     absence
?labelSelector=app in (web,db)          set membership
?labelSelector=tier notin (frontend)    outside the set — and objects with no tier label
?labelSelector=app=web,tier=frontend    AND
```

**This is how Kubernetes is wired together.** A Service has no list of pod
names; it has `spec.selector`, and the endpoints controller turns that into this
request. A Deployment does not own ReplicaSets by name. A `NetworkPolicy` picks
its subjects this way. The coupling between two objects in Kubernetes is
almost always a label selector, which is why you can add a pod to a Service by
editing the pod.

**The negative forms match missing labels.** `app!=web` selects an object with
no `app` label at all, and so does `tier notin (frontend)`. This is the rule
people get wrong, and it is right: "everything that is not in production" has to
include the things nobody labelled. Only `!app` and `app` are about the label's
presence as such.

**Commas mean two things, so parse before you split.** The comma between terms
is an AND; the commas inside `in (a,b)` belong to the set. Split on every comma
and `app in (web,db)` becomes two terms that are each nonsense.

**Nonsense is a 400.** A trailing comma leaves an empty term, and an empty term
is not "match everything" — a selector the server cannot parse is refused, for
the same reason as the last stage: a filter dropped on the floor sends the whole
collection to a client that asked for part of it.

**Labels are not annotations.** Labels are for selecting and are constrained
(63 characters, a restricted alphabet, optional DNS-subdomain prefix).
Annotations hold anything and are selectable by nothing. If you find yourself
wanting to select on an annotation, the field belongs in a label.

## Go APIs

- `strings.Fields(term[:open])` splits `key in` cleanly; `strings.CutPrefix` gets
  the `!` form.
- Build the same `func(object) bool` shape as the field selector so one
  `matchAll` combines both — a request may carry both, and an object has to pass
  both.
- Labels come out of the stored JSON as `map[string]any`; read each value with a
  `string` type assertion rather than `fmt.Sprint`.
- The real implementation is `k8s.io/apimachinery/pkg/labels` — worth reading
  after, not before.

## Hints

<details><summary>Nudge</summary>

Two functions: one that cuts the selector into terms without cutting inside
parentheses, and one that turns a single term into a test. The handler already
has somewhere to put the result.
</details>

<details><summary>Approach</summary>

Walk the raw string tracking parenthesis depth and cut on commas at depth 0. For
each term, in order: a `(` means a set term — `strings.Fields` the head for the
key and `in`/`notin`, split the inside on commas; a leading `!` is absence; no
`=` or `!` anywhere is existence; otherwise reuse the key/op/value split from the
field selector. For `!=` and `notin`, write the test so a missing label counts as
a match. AND the tests, AND that with the field selector's, and answer 400 with
the parse error as the message.
</details>

## Further reading

- [Labels and selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
- [Service: defining a selector](https://kubernetes.io/docs/concepts/services-networking/service/)
- [`k8s.io/apimachinery/pkg/labels`](https://pkg.go.dev/k8s.io/apimachinery/pkg/labels)
