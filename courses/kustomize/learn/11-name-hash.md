---
title: Name a generated object by its content
concepts: [content hash, immutable config, rollouts]
---

## Core concept

Change a ConfigMap that a Deployment mounts and nothing happens: the pods keep
running with the old files until something restarts them. kustomize's answer
is to put the content in the **name**. A generated ConfigMap is called
`settings-t82mkhg8fd`; change one value and it becomes a different object with
a different name, every reference to it changes (stage 12), the pod template
changes with it, and the Deployment rolls out.

The suffix is computed from:

```json
{"data":{"mode":"fast"},"kind":"ConfigMap","name":""}
{"data":{"password":"czNjcjN0"},"kind":"Secret","name":"","type":"Opaque"}
```

— the object's `kind`, `data` (an empty string when there is none),
`binaryData` and `type` if present, and an **empty** name, encoded as JSON
with sorted keys. Labels, annotations and the real name are not content, so
two empty ConfigMaps share a suffix. Take the SHA-256 of that JSON, keep the
first ten hex characters, and respell them so the result cannot read as a
word or a number: `0→g`, `1→h`, `3→k`, `a→m`, `e→t`.

The suffix goes on **once, at the very end** of the whole build, after every
kustomization's prefixes and suffixes: `prod-settings-v2-t82mkhg8fd`, never
`prod-settings-t82mkhg8fd-v2`. `disableNameSuffixHash` turns it off.

## Go APIs

- `crypto/sha256.Sum256` and `fmt.Sprintf("%x", …)`.
- `encoding/json` sorts map keys, which is what makes the hash stable.
- `strings.NewReplacer` does the respelling in one pass.

## Hints

<details><summary>Nudge</summary>

Mark a generated resource as wanting a hash when it is generated, and add the
suffix in `main`, after the top-level build returns.
</details>

<details><summary>Approach</summary>

Check your encoding against kustomize's on one ConfigMap before anything else:
the hash is all or nothing, and a missing `"name":""` is enough to miss.
</details>

## Further reading

- [Generators and the name suffix hash](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/configmapgenerator/)
- [kustomize's hasher](https://github.com/kubernetes-sigs/kustomize/blob/master/api/hasher/hasher.go)
