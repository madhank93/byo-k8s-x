---
title: Resolve any spelling
concepts: [restmapper, discovery, gvk vs gvr]
---

## Core concept

`po`, `pod`, `pods`, `Pod`, `pods.v1.` — five spellings of one thing, and the
API accepts exactly one of them in a URL. Turning what a person typed into a
**GroupVersionResource** is the RESTMapper's job, and it needs the server to do
it: shortnames are declared by whoever defined the resource, so a hardcoded
table covers the core types and breaks on the first CRD.

The mapping runs in both directions and you will need both. A manifest carries
a **GVK** (`apiVersion` + `kind`) and has to become a GVR before it can be
addressed; a command line carries a resource-or-kind string and has to become
the same thing.

The trap in this stage is that the plain discovery mapper does *not* know
shortnames. `pods` resolves and `po` does not, which is a confusing way to
fail. Shortname expansion is a separate client-side layer — the shortcut
expander — wrapped around the mapper.

## Go APIs

- `restmapper.GetAPIGroupResources(dc)` then
  `restmapper.NewDiscoveryRESTMapper(groups)`.
- `restmapper.NewShortcutExpander(mapper, dc, func(string) {})` — the last
  argument is a deprecation-warning sink.
- `schema.ParseResourceArg("pods.v1.")` for fully-qualified forms, and
  `mapper.ResourceFor(gr.WithVersion(""))` for the bare ones.
- `mapper.RESTMapping(gvk.GroupKind(), gvk.Version)` for the GVK direction.

## Hints

<details><summary>Nudge</summary>

If `pods` works and `po` does not, the mapper is right and something is missing
*around* it.
</details>

<details><summary>Approach</summary>

Build the mapper once in a helper both `get` and the later write commands can
call. Lowercase the input before parsing so `Pod` behaves; `ResourceFor` with
an empty version asks for the preferred one.
</details>

<details><summary>Implementation</summary>

```go
return restmapper.NewShortcutExpander(
    restmapper.NewDiscoveryRESTMapper(groups), dc, func(string) {}), nil
```

Then:

```go
fullySpecified, gr := schema.ParseResourceArg(strings.ToLower(resource))
if fullySpecified != nil {
    if m, err := mapper.ResourceFor(*fullySpecified); err == nil { return m, nil }
}
m, err := mapper.ResourceFor(gr.WithVersion(""))
```
</details>

## Further reading

- [restmapper](https://pkg.go.dev/k8s.io/client-go/restmapper)
- [meta.RESTMapper](https://pkg.go.dev/k8s.io/apimachinery/pkg/api/meta#RESTMapper)
