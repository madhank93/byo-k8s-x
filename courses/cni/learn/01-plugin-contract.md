---
title: Speak the plugin protocol
concepts: [CNI_COMMAND, CNI environment, CNI error, error codes]
---

## Core concept

A plugin is a program the runtime runs once per operation. Everything it
needs arrives in two places:

- **The environment**: `CNI_COMMAND` (`ADD`, `DEL`, `CHECK`, `VERSION`),
  `CNI_CONTAINERID`, `CNI_NETNS`, `CNI_IFNAME`, `CNI_ARGS`, `CNI_PATH`.
- **stdin**: the network configuration, one JSON object with at least
  `cniVersion`, `name` and `type`.

A plugin that fails does not just exit non-zero: it prints an error object
on **stdout**, which the runtime surfaces in the pod's events:

```json
{"cniVersion": "1.0.0", "code": 4, "msg": "CNI_IFNAME is not set"}
```

The spec reserves codes below 100. The ones this stage needs:

| code | meaning |
|---|---|
| 4 | invalid necessary environment variables |
| 6 | failed to decode content (stdin is not JSON) |
| 7 | invalid network config |

ADD needs a container ID, a netns and an interface name; a missing one is
error 4. An unknown command is also 4.

This stage: validate the environment and the config, and fail the CNI way.

## Go APIs

- `os.Getenv`, `io.ReadAll(os.Stdin)`, `json.Unmarshal`.
- `json.NewEncoder(os.Stdout).Encode(errObject)` then `os.Exit(1)`.

## Hints

<details><summary>Nudge</summary>

Have every failure path return one error type carrying a code, and print it
in exactly one place, in `main`.
</details>

<details><summary>Approach</summary>

Check `CNI_COMMAND` first, then the variables that command needs, then read
and decode stdin. Anything unexpected becomes a code from 100 up.
</details>

## Further reading

- [CNI spec: parameters](https://www.cni.dev/docs/spec/#parameters)
- [CNI spec: error codes](https://www.cni.dev/docs/spec/#error)
