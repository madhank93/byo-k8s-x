---
title: The rule, as data
concepts: [validatingadmissionpolicy, paramkind, paramref, parameternotfoundaction, customresource]
---

## Core concept

Stage 19 moved the owner rule into the API server. It is still a rule written
down in code, though: the program builds the policy, so changing what the
policy accepts means changing the program and rolling it out again.

Most rules have a part that is not logic but data — which teams may own pods
here, which registries are trusted, how many replicas is too many. A
ValidatingAdmissionPolicy can read that part from an object at admission time:

```yaml
# on the policy
paramKind: {apiVersion: byok8s.dev/v1, kind: OwnerList}
validations:
  - expression: "object.metadata.labels['owner'] in params.spec.owners"
# on the binding
paramRef:
  name: byok8s-owners
  parameterNotFoundAction: Deny
```

`paramKind` says what type the parameters are; `paramRef` on the binding says
which object. Leave its `namespace` out and the API server looks the name up in
the namespace of the object being admitted, so every namespace can carry its
own allowlist under the same name and one policy serves them all.

Edit the params object and the next admission sees it, typically within a
second. No rollout and no restart — and in this stage the grader checks it
with your program already gone, so no program at all.

The field worth pausing on is `parameterNotFoundAction`. It is required,
because neither answer is safe by default. `Allow` means a missing params
object turns the rule off: delete one object and anyone may create anything.
`Deny` means a missing one refuses everything the policy matches. For a rule
that exists to keep things out, `Deny` is the only honest choice — and it
means the params have to exist before the binding does, or the binding
refuses its first pod.

### Why not a ConfigMap

The obvious params object is a ConfigMap, and it works — the first time. The
API server only watches a param type while some policy uses it. For a type it
has built in, it borrows its own shared watch, and stops that watch when the
last policy using the type goes away; the shared machinery never starts a
stopped watch again. From then until the API server restarts, every new policy
with ConfigMap params reads a frozen snapshot: ConfigMaps created since are
invisible, and with `Deny` every pod is refused with `no params found`.

That is how the 1.35 API server this course runs on behaves; check yours
before relying on a ConfigMap. This course deletes and recreates its policy at
every stage, so it would hit the cliff at the second one. A custom resource
gets a fresh watch each time a policy starts using it, and has no such cliff.

So the program brings its own type: an `OwnerList` CustomResourceDefinition,
created if absent, and one `OwnerList` per namespace, seeded only if absent.
Once a list exists it belongs to whoever runs the namespace, and a restart must
not quietly undo their edit. A type that was just created takes a moment
before the API server serves it, so the seed retries while it is not found.

The course fixes the shape so the grader can edit it: the CRD
`ownerlists.byok8s.dev`, version `v1`, kind `OwnerList`, namespaced, with
`spec.owners` a list of strings seeded with `platform`. The list's name is
yours — the grader reads it from the binding.

## Go APIs

- `k8s.io/client-go/dynamic` and `unstructured.Unstructured` create a CRD and
  its objects without a generated client.
- `ValidatingAdmissionPolicySpec.ParamKind`:
  `&admissionregistrationv1.ParamKind{APIVersion: "byok8s.dev/v1", Kind: "OwnerList"}`.
- `ValidatingAdmissionPolicyBindingSpec.ParamRef`: a `Name`, and
  `ParameterNotFoundAction` pointing at `admissionregistrationv1.DenyAction`.
- In CEL, `params` is the OwnerList: `params.spec.owners`.
- A refusal because the params object is missing says `no params found for
  policy binding`; one from the expression carries your message.

## Hints

<details><summary>Nudge</summary>

What in the stage 19 rule would a platform team want to change without asking
you first?
</details>

<details><summary>Approach</summary>

In order: the CRD, then the OwnerList (retrying while the new type is not yet
served), then the policy and binding. Create, never Update. Keep the stage 19
validation and add a second one for the allowlist, so a pod with no owner
still gets the old message.
</details>

## Further reading

- [Validating Admission Policy: parameter resources](https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/#parameter-resources)
