---
title: Build your own kustomize — primer
concepts: [kustomization, resources, transformers, generators, patches, overlays]
---

## The mental model

kustomize is a YAML-to-YAML function. It reads a **kustomization** — a
directory holding a `kustomization.yaml` and the manifests it lists — and
prints the resources it produces as one YAML stream. No templates and no
variables: every input is valid Kubernetes YAML, and every change is made by a
named, declarative step.

```
  kustomization.yaml ──► resources ──► generators ──► components
                                                         │
          output ◄── hash names ◄── fix references ◄── transformers
                                                    (patches, namespace,
                                                     prefix, labels, images,
                                                     replicas, replacements)
```

A **resource** is a manifest file or another kustomization directory. A
**generator** makes new objects — ConfigMaps and Secrets from literals and
files. A **transformer** changes what is there: puts it in a namespace,
prefixes its name, labels it, patches it. Because a kustomization directory
can itself be a resource, everything composes: a **base** describes an
application once, and each **overlay** builds on it for one environment.

The hard part is not any one step but keeping the result consistent. Rename a
ConfigMap and every Deployment that mounts it must follow; label a Deployment
and its selector and pod template must agree; patch a resource by the name it
had two layers down. Most of this course is about that bookkeeping.

## The command line

The program has one command:

```
app build DIR
```

It prints the build of `DIR` to stdout, or exits 1 with a reason on stderr,
as `kubectl kustomize DIR` does.

## How the stages are graded

Nothing here needs a cluster. Each stage writes kustomization trees to a
temporary directory and builds each one twice: with your program and with
the kustomize built into the pinned `kubectl` (`kubectl kustomize`). Where
kustomize refuses a tree, your program must exit 1 with a reason and print
nothing. Otherwise the two outputs are compared as data, document by
document and in order — until the last stage, which compares them byte for
byte. Nothing inspects your source.

## Further reading

- [kustomize: the kustomization file](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/)
- [kustomize: glossary](https://kubectl.docs.kubernetes.io/references/kustomize/glossary/)
- [Declarative management with kustomize](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/)
