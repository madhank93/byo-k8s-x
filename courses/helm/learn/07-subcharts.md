---
title: Render a chart inside a chart
concepts: [subcharts, value scoping, global values, template sets]
---

## Core concept

A chart can carry other charts in its `charts/` directory. Each subchart is
a complete chart — `Chart.yaml`, `values.yaml`, `templates/` — rendered as
part of its parent, in the same release:

```
shop/
  Chart.yaml
  values.yaml
  templates/app.yaml
  charts/
    db/
      Chart.yaml
      values.yaml
      templates/cm.yaml
```

**Values are scoped.** A subchart's templates see only their own part of
the tree: what the parent's values hold under the subchart's name, laid over
the subchart's own defaults. The parent configures its subchart without the
subchart knowing it has a parent:

```yaml
# shop/values.yaml          # shop/charts/db/values.yaml
db:                         port: 5432
  port: 6543                name: orders
```

`db`'s templates see `.Values.port` as 6543 and `.Values.name` as `orders`.
The merge happens in place, so the parent sees the result too:
`.Values.db.name` in `shop`'s templates is `orders`.

**`global` is the exception.** The parent's `global` map is copied into
every subchart's `global`, the parent winning on a clash, and the
subchart's own `global` defaults fill in the rest. A subchart's globals do
not flow back up to the parent's `.Values.global`.

The order Helm merges in is what makes those rules hold, so copy it:

1. Lay the user's values over the parent's defaults.
2. For each subchart, take that result's `<sub>` map (or an empty one),
   copy the parent's `global` into it (every subchart gets a `global` map,
   empty if no chart above sets one), and merge the subchart in the same
   way, recursively. Store the merged map back under `<sub>`.

A null under a subchart's key survives step 1, even where the parent has a
default to delete, so that step 2 deletes the subchart's own default too:
`--set db.port=null` leaves db with no `port` at all.

**Everything else is per chart.** A subchart's templates see the
subchart's own `.Chart` and `.Files`; `.Release` is shared. Its templates
are named under the parent's path — `shop/charts/db/templates/cm.yaml` —
which is both its `# Source:` header and its `.Template.Name`, and
`.Template.BasePath` is `shop/charts/db/templates`.

**There is still one template set.** Every chart's templates are parsed
into it, so a parent can `include` a subchart's define. When both define
the same name, stage 6's parse order decides, and since the parent's paths
are shallower, the parent's define wins.

The output is every chart's templates together, sorted by that full path,
which puts `shop/charts/…` before `shop/templates/…`.

Helm tries to load every entry of `charts/` as a chart, so anything else
there is an error, with three exceptions: entries whose names start with
`.` or `_` (a stray `.DS_Store`) are skipped, and a `.prov` provenance file
becomes one of the parent's `.Files`.

**Library charts** (`type: library`) render nothing. A library subchart
lends its `_`-prefixed files' defines to the set and its other templates
are left out; a library chart rendered on its own is refused with
`library charts are not installable`.

## Go APIs

- `os.ReadDir` lists a directory's entries, sorted by name, with `IsDir()`.
- Loading a subchart is loading a chart: the function from stage 1, called
  on `charts/<name>`.

## Hints

<details><summary>Nudge</summary>

Give each template the chart it came from and the values map that chart
sees. Rendering is then the loop you already have.
</details>

<details><summary>Approach</summary>

Make `load` recursive: after the walk (which now skips `charts/`), load each
directory under `charts/` as a child. Merge values chart by chart as above.
Then flatten: walk the tree with a path prefix (`shop`, `shop/charts/db`),
collecting every template with its chart, values and full name; parse them
all into one set in Helm's order, and render each with its own data.
</details>

## Further reading

- [Subcharts and global values](https://helm.sh/docs/chart_template_guide/subcharts_and_globals/)
- [Charts: the charts/ directory](https://helm.sh/docs/topics/charts/#managing-dependencies-manually-via-the-charts-directory)
