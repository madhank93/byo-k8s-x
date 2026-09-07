---
title: Let someone pause you
concepts: [annotations, operability, escape hatches, incident response]
---

## Core concept

At three in the morning, with a bad spec rolling out, the only lever a person
has is your controller. If the only way to stop it touching one object is to
delete the whole controller, every other object stops being managed at the
worst possible moment.

An annotation is the smallest possible escape hatch: `byok8s.dev/paused:
"true"` and the pass returns immediately for that object and nothing else. It
is deliberately an annotation rather than a spec field, because it is an
instruction to the controller, not part of what the user is asking for.

Say so in the output. A paused object that silently does nothing looks exactly
like a broken controller, and the person who paused it is rarely the person
debugging it an hour later.

## Go APIs

- `site.GetAnnotations()` — a nil map indexes fine in Go, so no nil check.
- Check it after fetching the object and before doing any work.

## Hints

<details><summary>Nudge</summary>

Where the check goes matters. Ahead of the children means "change nothing";
ahead of the status write means the object also stops reporting, which is
usually not what you want.
</details>

<details><summary>Implementation</summary>

```go
if site.GetAnnotations()[pausedAnnotation] == "true" {
    fmt.Printf("paused %s\n", key)
    return nil
}
```
</details>

## Further reading

- [Annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)
- [Cluster API's paused field, the same pattern in the wild](https://cluster-api.sigs.k8s.io/developer/providers/contracts/clusterctl)
