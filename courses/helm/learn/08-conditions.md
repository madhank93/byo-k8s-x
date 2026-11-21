---
title: Switch subcharts on and off
concepts: [dependencies, condition, tags, value paths]
---

## Core concept

A chart that bundles a database rarely wants it in every install: some
users bring their own. `Chart.yaml` lists the subcharts as
`dependencies`, and each entry can say when it applies:

```yaml
dependencies:
  - name: db
    version: 0.3.0
    condition: db.enabled
  - name: cache
    version: 0.1.0
    condition: cache.enabled,global.cache
    tags: [frontend]
  - name: metrics
    version: 0.1.0
    tags: [backend, ops]
```

Helm's rule, for each subchart:

1. **Condition first.** `condition` is a comma-separated list of value
   paths. The first path that holds a **boolean** decides. A path that is
   missing, or holds something else — `"yes"` is a string — is skipped
   (Helm prints a warning).
2. **Then tags.** Each tag is looked up under the top-level `tags:` map of
   values. If any listed tag is `true`, the subchart is on; if none is true
   and at least one is `false`, it is off.
3. **Otherwise on.** A subchart that no condition or tag decides — and any
   subchart in `charts/` that `dependencies` does not mention — renders.

Two details decide what "the values" are:

- Conditions are judged against values merged with **every** subchart
  present, so a subchart's own default counts: `queue.enabled: true` in
  `charts/queue/values.yaml` keeps queue on even when a tag says off.
- A disabled subchart is then dropped, and the values are merged again
  without it. Its templates do not render, and its defaults and globals
  never reach the parent: `.Values.db` in the parent is only what the
  parent's own values say.

Tags always come from the top chart's `tags:`; nested subcharts are judged
by the same map.

## Go APIs

- `strings.Split(cond, ",")` and `strings.TrimSpace` for the paths.
- A path is a walk through nested `map[string]any`, one `.`-separated key
  at a time; a type assertion `v.(bool)` tells a boolean from the rest.

## Hints

<details><summary>Nudge</summary>

You already have the function that merges a chart's values with its
subcharts'. Call it twice.
</details>

<details><summary>Approach</summary>

Merge once with every subchart, decide each subchart from that result, drop
the disabled ones from the chart, recurse into the ones left, then merge
again for rendering.
</details>

## Further reading

- [Charts: tags and condition fields in dependencies](https://helm.sh/docs/topics/charts/#tags-and-condition-fields-in-dependencies)
