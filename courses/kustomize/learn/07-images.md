---
title: Swap the images
concepts: [image references, tags, digests]
---

## Core concept

An image reference has up to three parts:

```
registry.example.com/team/web:1.2@sha256:abc…
└────────── name ──────────┘ └tag┘ └─ digest ─┘
```

The tag follows the *last* colon after the last slash — `localhost:5000/web`
has a port, not a tag. The digest follows `@`.

An `images` entry picks references by **whole name** — registry and path
included, so `nginx` does not match `docker.io/library/nginx` — and changes
them:

| entry sets | result |
|---|---|
| `newName` | the name; tag and digest kept |
| `newTag` | the tag; any digest dropped |
| `digest` | the digest; any tag dropped |
| `newTag` and `digest` | both |

It applies to the `image` of every item of every `containers` and
`initContainers` list, wherever those lists sit in the object — a pod, a
template, a CronJob's job template, a custom resource. Other fields named
`image` and `ephemeralContainers` are left alone.

Entries apply in the order listed, so a later entry sees what an earlier one
renamed, and an outer kustomization's entries apply after the inner one's.
`newTag` is a string: `1.10` unquoted is the number 1.1, and kustomize refuses
it rather than print the wrong tag.

## Hints

<details><summary>Nudge</summary>

Walk the whole object recursively and stop at any key called `containers` or
`initContainers`.
</details>

<details><summary>Approach</summary>

Cut the digest at `@` first, then the tag at the last `:` only if it comes
after the last `/`.
</details>

## Further reading

- [images](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/images/)
- [Image names](https://kubernetes.io/docs/concepts/containers/images/#image-names)
