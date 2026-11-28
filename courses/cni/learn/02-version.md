---
title: Negotiate the spec version
concepts: [cniVersion, VERSION command, supportedVersions]
---

## Core concept

Every config and every result carries a `cniVersion`, and the shape of a
result depends on it. A runtime asks first what a plugin speaks:

```
CNI_COMMAND=VERSION  →  {"cniVersion":"1.0.0","supportedVersions":["1.0.0","1.1.0"]}
```

VERSION needs nothing but the command; the runtime may pass a config on
stdin but no container.

Then, on every other command, the plugin checks the config's `cniVersion`
against that list **before doing anything**. A version it cannot speak is
error **1**, "incompatible CNI version". Refusing late — after creating an
interface — leaves something for nobody to clean up.

This stage: answer VERSION, and refuse configs in a version you do not
support. This course speaks `1.0.0`; the old `0.x` result format is not
needed.

## Go APIs

- `slices.Contains(supported, conf.CNIVersion)`.

## Hints

<details><summary>Nudge</summary>

Handle VERSION before the checks that need a container ID.
</details>

## Further reading

- [CNI spec: VERSION](https://www.cni.dev/docs/spec/#version)
- [libcni version negotiation](https://github.com/containernetworking/cni/tree/main/pkg/version)
