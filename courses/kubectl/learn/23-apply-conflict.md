---
title: Two managers, one field
concepts: [server-side apply, conflicts, errors, ownership]
---

## Core concept

When your apply sets a field another manager already owns, the server answers
**409 Conflict** and changes nothing. This is the payoff of stage 22: without
recorded ownership there is nothing to conflict *with*, and the last writer
would simply win in silence.

A conflict is a **question, not a fault**. The right answer depends on
something the server cannot know:

| The other manager is | Forcing is |
|---|---|
| a human's earlier `kubectl edit` | usually right — you are correcting it |
| an HPA or another controller | usually wrong — it will set it back within seconds, and now you own a field you are losing a fight over |

So `--force-conflicts` exists, and the decision belongs to whoever ran the
command. Your job is to make the error legible: say that another manager owns a
field this manifest sets, and name the way forward. An error message that
teaches the flag is worth more than one that dumps the status object.

Forcing means `Force: &true` in the patch options: the server transfers
ownership of the disputed fields to you.

## Go APIs

- `apierrors.IsConflict(err)` from `k8s.io/apimachinery/pkg/api/errors`.
- `metav1.PatchOptions{FieldManager: "byok8s", Force: &force}` — a `*bool`,
  because unset and false differ in the API.
- `fmt.Errorf("%w\n\n...", err)` to keep the original error wrapped while
  adding the advice.

## Hints

<details><summary>Nudge</summary>

The server already returns the right error. The stage is about recognising it
and about which layer decides what to do.
</details>

<details><summary>Approach</summary>

Add the flag, thread it into `PatchOptions.Force`, and special-case
`apierrors.IsConflict` in the error path. Do not retry automatically — an
automatic force is a policy your caller did not choose.
</details>

<details><summary>Implementation</summary>

```go
if apierrors.IsConflict(err) {
    return fmt.Errorf("%w\n\nanother field manager owns a field this manifest sets."+
        "\nre-run with --force-conflicts to take ownership", err)
}
```

To see one, have another manager set the field first:
`kubectl patch ... --field-manager=someone-else`.
</details>

## Further reading

- [Conflicts in server-side apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/#conflicts)
