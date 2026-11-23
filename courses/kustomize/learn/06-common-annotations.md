---
title: Annotate everything
concepts: [commonAnnotations, pod templates]
---

## Core concept

`commonAnnotations` is `commonLabels` without the selectors. Annotations do
not select anything, so they go only on:

- every object's `metadata/annotations`, and
- every pod template kustomize knows of — Deployments, ReplicaSets,
  DaemonSets, StatefulSets, ReplicationControllers, Jobs, and both the job
  template and the pod template of a CronJob — created when missing.

Annotating the pod template is the useful part: a changed template annotation
makes a Deployment roll its pods, which is how teams force a restart when a
config they mount has changed.

An annotation already present keeps its neighbours and loses its value, and
in a nested build the outer kustomization has the last word. Values are
strings: `true` unquoted is a boolean, and kustomize refuses it.

## Hints

<details><summary>Nudge</summary>

This is one more table for the field-spec walker from stage 5, with every
entry created when missing.
</details>

## Further reading

- [commonAnnotations](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/commonannotations/)
