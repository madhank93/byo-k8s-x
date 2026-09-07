---
title: Getting started
description: Install byok8s, bring up a kind cluster, and verify your first stage.
---

## Prerequisites

byok8s uses [mise](https://mise.jdx.dev/) to pin the toolchain — Go, kind and
kubectl — so none of them need to be installed globally. You also need a
container runtime for kind to build the cluster on (Docker or Podman).

```sh
brew install mise        # macOS; see the mise docs for other platforms
```

## Set up

```sh
git clone https://github.com/madhank93/byo-k8s-x
cd byo-k8s-x
mise install              # provisions Go, kind and kubectl
mise run build            # builds bin/byok8s and bin/tester onto PATH
byok8s up                 # create the kind cluster the course runs against
byok8s doctor             # check everything is ready
```

`mise` puts `bin/` on your `PATH` inside the repo, so `byok8s` is on the path
once `mise run build` has run. Everything below also works as
`go run ./cmd/byok8s …` if you would rather not build.

## The loop

```sh
byok8s list               # the stages, and where you are
byok8s learn              # the course primer — read this before stage 1
byok8s learn 1            # the concept note for stage 1
byok8s learn 1 -hints     # and the hint ladder, only when you ask for it
byok8s run 1              # verify stage 1
```

Your program is `courses/kubectl/app/main.go`, and it grows for the whole
course — a stage adds one visible thing rather than starting over. `byok8s run
N` verifies stages 1..N, so passing stage 12 means stages 1 through 12 all
still work.

Stuck on a stage and want to move on?

```sh
byok8s reset --to 12      # replace your program with the verified stage 12 reference
```

## How verification works

Stages are graded against a real cluster, not a fake client. `byok8s up`
creates a [kind](https://kind.sigs.k8s.io/) cluster named `byok8s`; each stage
runs in its own namespace so one stage's objects can't affect another's.

The harness builds your program, invokes it with the arguments the stage
specifies, and judges the exit code and stdout. Nothing reads your source, so
there is no single expected implementation — only expected behaviour.

When you're done:

```sh
byok8s down               # delete the cluster
```

## The teaching layer

A stage tells you whether your program behaves; it never tells you why. That
lives in `courses/kubectl/learn/`: a primer to read before stage 1, and one
note per stage covering the core concept, the Go APIs it needs, a hint ladder
scaled to the stage's difficulty, and further reading.

Hints stay behind `-hints`, because a hint you didn't ask for is a spoiler.

The same notes are published here: read the [primer](/learn/kubectl/), then
open any row in the [Catalog](/catalog/) for that stage's note. Each stage
lists the concepts it teaches, and the catalog search matches them.

## Reference solutions

Every stage ships a verified snapshot under
`courses/kubectl/reference/stages/NN-slug/main.go`. Each one passed stages
1..N cumulatively against a live cluster in CI before it was committed, and CI
re-verifies all 30 on every push.

Browse them — with a per-stage diff showing only what that stage added — in the
[Catalog](/catalog/).

## Other commands

```sh
mise run test             # unit tests for the harness itself
mise run lint             # vet and formatting
mise run sweep            # fail if a stage snapshot repeats the one before it
mise run gen              # regenerate the website's catalog and generated pages
```

Next: read the [primer](/learn/kubectl/), then browse the
[Catalog](/catalog/).
