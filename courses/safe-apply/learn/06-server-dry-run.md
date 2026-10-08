---
title: Let the server do the merge
concepts: [server-side apply, dry-run, defaulting]
---

## Core concept

Your local merge is a prediction, and it is wrong in a way you cannot fix
from the client. The server also **defaults** fields (a Deployment gets
`revisionHistoryLimit: 10` and `progressDeadlineSeconds: 600` whether you
asked or not), runs **admission**, and since server-side apply it does the
**merge** itself, from `managedFields` rather than an annotation.

So ask it. Every write accepts `dryRun=All`: the request goes through the
whole pipeline — authentication, defaulting, admission, the apply merge,
validation — and stops just before storage. The object in the response is
exactly what you would have got.

```
  PATCH /apis/apps/v1/namespaces/web/deployments/api
        ?fieldManager=byok8s&dryRun=All
  Content-Type: application/apply-patch+yaml

  <the manifest, unchanged>
```

That makes `diff` honest: *after* is now the server's answer, and the
comparison with live is the change the server would really make. The
three-way merge you wrote in stage 3 is still the right model — server-side
apply does the same thing, with per-field ownership in place of the
annotation.

For this stage, compute *after* with a server-side apply dry-run as the field
manager `byok8s`. The output format and exit codes do not change. A dry-run
must never be stored: after `diff`, the cluster is exactly as it was.

## Go APIs

- `res.Patch(ctx, name, types.ApplyPatchType, body, metav1.PatchOptions{FieldManager: "byok8s", DryRun: []string{metav1.DryRunAll}})`.
- The body is the manifest object as JSON; apply addresses it by name, and a
  create is the same call.

## Hints

<details><summary>Nudge</summary>

Your diff already compares two objects. Only one of them needs to come from
somewhere else.
</details>

<details><summary>Approach</summary>

Write one `apply(obj, dryRun bool)` helper. The diff calls it with `true`
now; a later stage calls it with `false`, and the two must not drift apart.
</details>

## Further reading

- [Dry-run](https://kubernetes.io/docs/reference/using-api/api-concepts/#dry-run)
- [Server-Side Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/)
