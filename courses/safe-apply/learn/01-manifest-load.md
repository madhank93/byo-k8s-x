---
title: Read the manifests
concepts: [manifests, multi-document yaml, unstructured]
---

## Core concept

Everything this tool does starts from a file, and a manifest file is a
*stream* of documents, not one. `---` separates them; a document can be empty,
a comment, YAML, or a JSON object, and real repositories contain all four.

Your program must not know any kinds in advance. It will diff and apply
Deployments, ConfigMaps and custom resources installed yesterday, so each
document becomes an **unstructured** object — a `map[string]any` with helpers
for the three fields every object has: `apiVersion`, `kind`, and
`metadata.name`. A document missing any of those cannot be addressed on the
server, and saying so now is better than failing halfway through an apply.

For this stage, add a command:

```
app objects -f FILE
```

It prints one line per object, in file order:

```
ConfigMap/first
Secret/second
```

Empty and comment-only documents are skipped. A document that is not valid
YAML or JSON, or that has no `apiVersion`, `kind` or `metadata.name`, makes
the command exit non-zero.

## Go APIs

- `yaml.NewYAMLOrJSONDecoder` from `k8s.io/apimachinery/pkg/util/yaml`
  splits the stream on `---` and decodes each document; it returns `io.EOF`
  when the stream ends.
- `unstructured.Unstructured{Object: m}` and its `GetKind`, `GetName`,
  `GetAPIVersion`.

## Hints

<details><summary>Nudge</summary>

Decode into a `map[string]any` and look at what an empty document gives you
before deciding how to skip it.
</details>

## Further reading

- [YAML: multiple documents in one stream](https://yaml.org/spec/1.2.2/#91-documents)
- [Managing resources: organizing resource configurations](https://kubernetes.io/docs/concepts/cluster-administration/manage-deployment/#organizing-resource-configurations)
