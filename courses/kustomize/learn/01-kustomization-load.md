---
title: Read a kustomization
concepts: [kustomization file, strict decoding, YAML streams]
---

## Core concept

A kustomization is a directory with exactly one kustomization file in it.
kustomize looks for three names, in this order:

```
kustomization.yaml
kustomization.yml
Kustomization
```

None of them is an error, and so are two: kustomize will not guess which one
you meant.

The file is read **strictly**. `resourcse:` is not a typo kustomize skips — an
unknown field is an error, and so is a field of the wrong shape, such as
`resources: cm.yaml` where a list belongs. `apiVersion` and `kind` are
optional, but when present they must be `kustomize.config.k8s.io/v1beta1` and
`Kustomization`. An empty file is an error too.

For this stage, `app build DIR` reads the file and prints every file under
`resources`, in the order listed, as one YAML stream: documents separated by
`---` lines. A listed file that does not exist is an error. Every resource
needs a `kind` and a `metadata.name` — those are how kustomize will identify
it from now on — but `apiVersion` may be missing.

## Go APIs

- `sigs.k8s.io/yaml.YAMLToJSON`, then a `json.Decoder` with
  `DisallowUnknownFields`, rejects both unknown fields and wrong types.
  Unmarshalling YAML straight into a struct can quietly coerce a number into
  a string field.
- `os.Stat` tells you which of the three names exist.

## Hints

<details><summary>Nudge</summary>

Write the `Kustomization` struct with only the fields this stage needs; strict
decoding then refuses the rest for free, and each later stage adds its own.
</details>

<details><summary>Approach</summary>

Split each resource file on lines that are exactly `---`, decode each
document into `map[string]any`, check `kind` and `metadata.name`, and marshal
each back out.
</details>

## Further reading

- [The kustomization file](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/)
- [resources](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/resource/)
