---
title: Label everything, selectors too
concepts: [commonLabels, labels, selectors, field specs]
---

## Core concept

A label on a Deployment is only half the job. Its `spec.selector` decides
which pods it owns, and its pod template decides what labels those pods get;
if the three disagree, the API server rejects the Deployment or, worse, it
adopts pods it should not. So `commonLabels` writes its pairs into every
place that must agree:

| kind | fields |
|---|---|
| everything | `metadata/labels` |
| Service | `spec/selector` |
| Deployment, ReplicaSet, DaemonSet, StatefulSet | `spec/selector/matchLabels`, `spec/template/metadata/labels` |
| StatefulSet | `spec/volumeClaimTemplates[]/metadata/labels` |
| ReplicationController | `spec/selector`, `spec/template/metadata/labels` |
| Job | `spec/template/metadata/labels` (and `spec/selector/matchLabels` if present) |
| CronJob | `spec/jobTemplate/metadata/labels`, the job's template, its selector if present |
| PodDisruptionBudget, NetworkPolicy | their pod selectors, if present |
| Deployment, StatefulSet | affinity and topology spread selectors, if present |

That table — a group, a kind, a slash path, and whether to **create** the
field when missing — is what kustomize calls a *field spec*. A required field
is created; an optional one, like a Job's selector, is only extended when it
is already there. A path segment that meets a list applies to every item. A
kind kustomize does not know gets `metadata/labels` and nothing else.

`labels` is the newer field, a list of entries, each with `pairs` and two
switches: neither switch means `metadata/labels` only; `includeTemplates`
adds the template paths but no selector; `includeSelectors` is everything,
like `commonLabels`.

Label values are strings: an unquoted number is refused.

## Hints

<details><summary>Nudge</summary>

Write one function that walks a slash path through maps and lists and calls
back at the end. Annotations, images and references will reuse it.
</details>

<details><summary>Approach</summary>

Keep the table as data, `{group, kind, path, create}`, and filter it by
`includeSelectors`/`includeTemplates` rather than writing separate code for
each.
</details>

## Further reading

- [commonLabels](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/commonlabels/)
- [labels](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/labels/)
