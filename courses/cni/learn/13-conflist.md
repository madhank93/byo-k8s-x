---
title: Run a plugin chain
concepts: [network configuration list, chaining, prevResult, result cache]
---

## Core concept

Real networks are rarely one plugin. A **conflist** names several, run in
order:

```json
{"cniVersion": "1.0.0", "name": "chain",
 "plugins": [{"type": "bridge", …}, {"type": "portmap", …}]}
```

The runtime (libcni, inside containerd) runs them:

- **ADD** in order. Each plugin's config is its own entry plus the list's
  `name` and `cniVersion`, and from the second on, `prevResult`: the result
  the plugin before it printed. The last result is the list's result.
- **CHECK** in order, **DEL in reverse**, each with the **cached** result of
  the ADD as `prevResult`. The runtime keeps that cache on disk, keyed by
  network, container and interface, because DEL can come long after ADD.
- Each plugin is found by `type` on `CNI_PATH`, and runs with the same
  `CNI_*` environment.
- The first failure stops the list, and its error is the list's error.

This stage your program plays the runtime:

```
app conflist --cache DIR ADD|CHECK|DEL FILE
```

## Go APIs

- `exec.Command(bin)` with `Env` and `Stdin`; `cmd.Output()`.
- `filepath.SplitList(os.Getenv("CNI_PATH"))`.

## Hints

<details><summary>Nudge</summary>

A plugin's error is on its stdout and in its exit code; pass both on.
</details>

## Further reading

- [CNI spec: network configuration lists](https://www.cni.dev/docs/spec/#network-configuration-lists)
- [libcni](https://github.com/containernetworking/cni/tree/main/libcni)
