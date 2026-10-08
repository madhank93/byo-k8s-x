---
title: Merge three ways
concepts: [client-side apply, three-way merge, last-applied-configuration, json merge patch]
---

## Core concept

Before server-side apply existed, `kubectl apply` worked out the change on
your machine, and it needed three inputs to do it:

```
  last applied (annotation)   {a: 1, b: 2}
  live (cluster)              {a: 1, b: 2, c: 3}     ← c was added by someone else
  desired (manifest)          {a: 1x}

  patch = {a: "1x", b: null}                          ← c is not mentioned
```

The rules are asymmetric, and the asymmetry is the point:

- **Changes and additions** come from comparing *live* with *desired*:
  anything the manifest sets to a value the cluster does not have.
- **Deletions** come only from comparing *last applied* with *desired*:
  a field is removed only if this manifest used to set it and no longer does.

Two-way merges get one of those wrong. Diffing live against desired deletes
`c`, stamping on whoever added it. Diffing nothing against desired never
deletes `b`, so removed fields linger forever.

The result is a **JSON merge patch** (RFC 7386): a partial object where a
value means "set this" and `null` means "delete this". kubectl uses a
*strategic* merge patch for built-in kinds, which can merge lists by key; for
the maps this stage uses, the two agree.

The last-applied record lives in the
`kubectl.kubernetes.io/last-applied-configuration` annotation. It is
bookkeeping, not content: leave it out of all three inputs so it never shows
up as a change of its own.

For this stage, add:

```
app patch -f FILE
```

printing one line per object: `Kind/name create` if it does not exist, or
`Kind/name` followed by a space and the merge patch as JSON — `{}` when
nothing would change.

## Go APIs

- `jsonmergepatch.CreateThreeWayJSONMergePatch(original, modified, current)`
  from `k8s.io/apimachinery/pkg/util/jsonmergepatch`.
- `unstructured.RemoveNestedField(obj, "metadata", "annotations", key)`.
- An object with no annotation has an empty last-applied record: `{}`.

## Hints

<details><summary>Nudge</summary>

The annotation holds a JSON object. Which of the three inputs is it?
</details>

<details><summary>Approach</summary>

Build the three inputs as JSON — the annotation, the manifest object, the live
object — each with the annotation itself removed, and hand them to the
three-way function in that order.
</details>

<details><summary>Implementation</summary>

```go
original := []byte(live.GetAnnotations()[lastApplied]) // or "{}"
modified, _ := json.Marshal(strip(desired).Object)
current, _ := json.Marshal(strip(live).Object)
patch, err := jsonmergepatch.CreateThreeWayJSONMergePatch(original, modified, current)
```
</details>

## Further reading

- [How apply calculates differences and merges changes](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/declarative-config/#how-apply-calculates-differences-and-merges-changes)
- [RFC 7386: JSON Merge Patch](https://www.rfc-editor.org/rfc/rfc7386)
