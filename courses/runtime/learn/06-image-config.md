---
title: Read what the image runs
concepts: [image config, Entrypoint, Cmd, Env, WorkingDir]
---

## Core concept

The manifest's `config` blob is JSON describing the image, and its `config`
field says what a container from it runs:

```json
{"config": {
  "Entrypoint": ["/bin/probe"],
  "Cmd": ["from-cmd"],
  "Env": ["PATH=/bin", "GREETING=hello"],
  "WorkingDir": "/work"
}}
```

The rules for turning that into a process are Docker's, and Kubernetes keeps
them (a pod's `command` replaces Entrypoint, `args` replaces Cmd):

- argv is `Entrypoint` followed by `Cmd`.
- Arguments given at run time replace `Cmd` and keep `Entrypoint`. With no
  Entrypoint, they are the whole argv.
- No `WorkingDir` means `/`.
- No `PATH` in `Env` means the default
  `/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`, appended.

This stage: `config [--root DIR] REF [ARG…]` prints

```
args ["/bin/probe","x"]   argv as a JSON array
cwd /work
env PATH=/bin             one line per variable, in order
```

## Go APIs

- `encoding/json` into a struct with `Entrypoint`, `Cmd`, `Env []string` and
  `WorkingDir string` under `config`.
- `fs.Args()[1:]` after the reference are the run-time arguments.
- `slices.Clone` before appending to a slice you did not make.

## Hints

<details><summary>Nudge</summary>

`append(entrypoint, cmd...)` can write into the backing array of the
Entrypoint slice; clone it first, or you will be chasing a bug later when the
same config builds two processes.
</details>

## Further reading

- [OCI image config](https://github.com/opencontainers/image-spec/blob/main/config.md)
- [Dockerfile: how CMD and ENTRYPOINT interact](https://docs.docker.com/reference/dockerfile/#understand-how-cmd-and-entrypoint-interact)
- [Define a command and arguments for a container](https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/)
