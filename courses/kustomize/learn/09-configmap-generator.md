---
title: Generate a ConfigMap
concepts: [generators, literals, env files, binaryData, generator options]
---

## Core concept

Writing a ConfigMap by hand means pasting a config file into YAML. A
generator reads it for you:

```yaml
configMapGenerator:
- name: settings
  literals: [mode=fast, "greeting=hello world"]
  files: [app.conf, custom-key=other.conf]
  envs: [defaults.env]
```

Sources are read in that order — literals, files, env files — into one map:

- a **literal** splits at the first `=`; one pair of matching quotes around
  the value is removed, and an empty value is allowed;
- a **file** is keyed by its base name, unless written `key=path`; a file that
  is not valid UTF-8 goes under `binaryData`, base64-encoded;
- an **env file** is `KEY=VALUE` lines; blank lines and `#` comments are
  skipped and quotes are kept.

A key may appear only once across all sources. A literal with no `=`, a
missing file, or a file outside the kustomization's directory is an error.

`generatorOptions` sets `labels`, `annotations` and `immutable` for every
generator; a generator's own `options` override them key by key.

A generated ConfigMap is a resource like any other: this kustomization's
namespace, prefix and labels reach it (and its namespace beats the
generator's own `namespace`), and it collides with a handwritten ConfigMap of
the same name. A generator in an inner kustomization is built there, with
that kustomization's transformers.

The real kustomize also appends a content hash to every generated name.
That is stage 11; every tree here sets `disableNameSuffixHash: true`.

## Hints

<details><summary>Nudge</summary>

Generate before the transformers run, and append the result to the resources.
</details>

<details><summary>Approach</summary>

Collect `[key, value]` pairs in source order first, then check for repeats and
decide `data` or `binaryData` with `utf8.ValidString`.
</details>

## Further reading

- [configMapGenerator](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/configmapgenerator/)
- [generatorOptions](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/generatoroptions/)
