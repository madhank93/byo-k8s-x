---
title: Choose a namespace
concepts: [namespaces, precedence, cli parsing]
---

## What this stage teaches

Namespace resolution has three steps and the middle one is the trap:
`--namespace`, then whatever the **current context** carries, then `"default"`.
A tool that reads the flag and defaults straight to `"default"` works for its
author, whose context sets no namespace, and acts in the wrong place for the
colleague whose context does.

`clientcmd` already implements that precedence — push the flag in as an
override and ask the config object for the namespace rather than computing it.

There is a second lesson hiding here: Go's `flag` package stops parsing at the
first non-flag argument, so `get pods -n web` leaves `-n` unparsed and your
program acts in the wrong namespace with no error at all. kubectl accepts flags
before, between and after its positional arguments. Getting that right needs a
small loop: parse a run of flags, take one positional, repeat.

## Go you'll reach for

- `overrides.Context.Namespace = flagValue` — only when the flag was set;
  an empty override must not beat the context.
- `cc.Namespace()` — applies the precedence and reports `"default"` when
  nothing else set one.
- `fs.Parse` / `fs.Args()` in a loop, for interspersed flags.

## Hints

<details><summary>Nudge</summary>

Two flag names, one variable: `flag.StringVar` can bind `-n` to the same
target as `--namespace`.
</details>

<details><summary>Approach</summary>

Set `overrides.Context.Namespace` only when the flag is non-empty, then call
`cc.Namespace()` and use what it returns. Do not reach into the raw kubeconfig
yourself.

For the parsing: loop while `fs.NArg() > 0` — append `fs.Arg(0)` to your
positionals, then re-parse `fs.Args()[1:]`.
</details>

## Going deeper

- [Namespaces](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/)
