---
title: Find what you left behind
concepts: [pruning, apply sets, label selectors, discovery]
---

## Core concept

Delete an object from your manifest and apply it: the object is still in the
cluster. Apply only ever talks about the objects it is given; an object it is
not given is simply not mentioned. Removing what a manifest *used* to contain
is **pruning**, and it starts with a harder question than it looks: which of
the objects in this namespace are yours?

"Everything not in the manifest" is the wrong answer: that is every other
team's work too. The only safe answer is a record you wrote yourself. So apply
now stamps a label on everything it writes, naming the **set** it belongs to:

```yaml
metadata:
  labels:
    byok8s.dev/set: web
```

A prune candidate is then an object that carries your set's label and is not
in the manifest:

```
  in the cluster                     label           in manifest   prune?
  ────────────────────────────────   ─────────────   ───────────   ──────
  ConfigMap/keep                     set=web         yes           no
  ConfigMap/gone-cm                  set=web         no            yes
  Secret/gone-secret                 set=web         no            yes
  ConfigMap/stranger                 (none)          no            no
  ConfigMap/other-set                set=api         no            no
```

Notice `Secret/gone-secret`: the new manifest may contain no Secrets at all,
so you cannot work out which kinds to search from the manifest. Ask discovery
for every namespaced kind you can list and delete, and list each with a label
selector. kubectl's ApplySet avoids that sweep by recording the set's kinds on
a parent object; for a cluster this size, the sweep is fine.

The sweep touches every API on the cluster, including ones that are broken.
An aggregated API whose backend is down can still be in discovery, and
listing its kinds fails with a 503. That is no reason to stop: every other
kind's leftovers are still right. So skip a kind whose list fails with
ServiceUnavailable, NotFound or MethodNotSupported, and do the same for the
groups discovery itself could not reach. Say so on stderr, naming the group.
A kind you could not list is a kind whose leftovers you cannot see, and the
person running prune needs to know the answer is incomplete.

Warn, don't refuse. A missed leftover is only kept, never deleted, so
continuing is safe. Refusing would let one broken API that belongs to someone
else block every prune on the cluster.

This stage only **plans**. `prune -f FILE --set NAME` prints one `Kind/name`
line per candidate, sorted, and deletes nothing. Without `--set` there is no
way to tell yours from anyone else's, so refuse.

## Go APIs

- `discovery.ServerPreferredNamespacedResources()`: a
  `*discovery.ErrGroupDiscoveryFailed` (use `errors.As`) means some groups
  could not be discovered. Its `Groups` map names them, and the lists still
  hold every group that did resolve.
- `apierrors.IsServiceUnavailable`, `IsNotFound`, `IsMethodNotSupported`: a
  kind in discovery that cannot be listed right now.
- `discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"list", "delete"}}, lists)`.
- `metav1.ListOptions{LabelSelector: "byok8s.dev/set=web"}`.

## Hints

<details><summary>Nudge</summary>

The label goes on in `setup`, before any command sends anything, so diff and
apply agree on what the object will look like.
</details>

<details><summary>Approach</summary>

Key every manifest object by group, kind, namespace and name. Then, for every
listable kind and every namespace the manifest touches, list by label and keep
what is not keyed. Some kinds are served by two groups (`events`), so dedupe by
UID.
</details>

<details><summary>Implementation</summary>

```go
for _, l := range lists {
    gv, _ := schema.ParseGroupVersion(l.GroupVersion)
    for _, r := range l.APIResources {
        items, err := c.dyn.Resource(gv.WithResource(r.Name)).Namespace(ns).
            List(ctx, metav1.ListOptions{LabelSelector: setLabel + "=" + set})
        // skip what the manifest names, and UIDs already seen
    }
}
```
</details>

## Further reading

- [kubectl apply --prune and ApplySets](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/declarative-config/#alternative-kubectl-apply-f-directory-prune)
- [KEP-3659: ApplySet](https://github.com/kubernetes/enhancements/tree/master/keps/sig-cli/3659-kubectl-apply-prune)
