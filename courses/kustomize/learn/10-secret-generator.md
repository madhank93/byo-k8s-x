---
title: Generate a Secret
concepts: [secretGenerator, base64, Secret types]
---

## Core concept

`secretGenerator` reads exactly the same sources as `configMapGenerator`, into
a Secret instead:

- every value goes under `data`, **base64-encoded** — text and binary alike,
  so a Secret has no `binaryData`;
- the Secret has a `type`, `Opaque` unless the generator says otherwise
  (`kubernetes.io/tls`, `kubernetes.io/dockerconfigjson`, …), and `data` is
  present even when empty.

Options, namespace and prefixes reach a Secret as they reach a ConfigMap. A
ConfigMap and a Secret may share a name — they are different kinds — and a
Secret refuses a repeated key the same way.

`type` belongs to `secretGenerator` alone: a `configMapGenerator` entry with a
`type` is an unknown field, refused by the strict decoding from stage 1.

Base64 is an encoding, not encryption. Anyone who can read the Secret can read
the values.

## Go APIs

- `base64.StdEncoding.EncodeToString`.

## Hints

<details><summary>Nudge</summary>

One generator function serves both: a non-empty type means Secret.
</details>

## Further reading

- [secretGenerator](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/secretgenerator/)
- [Secret types](https://kubernetes.io/docs/concepts/configuration/secret/#secret-types)
