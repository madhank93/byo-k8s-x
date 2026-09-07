# byo-k8s-x

Build your own Kubernetes tooling, one stage at a time, against a real cluster.

Each course is a single program you grow. A stage adds one visible thing, and
running a stage re-verifies every stage before it, so the tool you finish with
is one you wrote every line of.

**Build your own kubectl** is the first course: find the cluster, choose a
context, ask the server what it is, list and print objects, resolve any
resource through discovery and the RESTMapper, select, watch, apply with
server-side apply, describe, and finally stream logs, exec and port-forward.

**Build your own controller** is the second: install a CustomResourceDefinition,
watch it through an informer, turn events into keys on a workqueue, and grow a
reconcile loop that creates and owns a Deployment and a Service, repairs them
when they drift, reports status and conditions, cleans up behind a finalizer,
elects a leader and shuts down cleanly.

The rest of the ladder — webhook, scheduler, apiserver, kubelet, CNI,
kube-proxy and more — is written down in [CURRICULUM.md](CURRICULUM.md), with a
stage list for each.

## Getting started

```sh
mise install          # go, kind, kubectl
byok8s up             # create the local kind cluster
byok8s doctor         # check everything is ready
byok8s courses        # every course, shipped and planned
byok8s list           # the stages of the course you are on
byok8s learn          # the course primer — the background, before stage 1
byok8s learn 1        # the concept note for stage 1 (-hints for the ladder)
byok8s run 1          # verify stage 1
```

Commands work on one course at a time. `kubectl` is the default; put
`--course <slug>` before the command to pick another, or set `BYOK8S_COURSE`:

```sh
byok8s --course controller list
byok8s --course controller learn 7 -hints
byok8s --course controller run 7
```

Your program lives in `courses/<course>/app/main.go` and grows for the whole
course. Stuck on a stage and want to move on? `byok8s reset --to N` replaces it
with the verified reference for stage N.

## How verification works

A real kind cluster, not a fake client — so `create deployment` really does
produce pods, and `logs` really does stream from a container. Each stage runs
in its own namespace, and the verdict is your program's exit code and stdout.
Nothing inspects your source: any correct implementation passes.

## The teaching layer

A stage says whether your program behaves; it never says why. That lives in
`courses/kubectl/learn/` — a primer to read before stage 1, and one note per
stage covering the core concept, the Go APIs it needs, a hint ladder
scaled to the difficulty, and links to the primary sources. Hints stay behind
`-hints`, because a hint you did not ask for is a spoiler.

```sh
byok8s learn            # the primer
byok8s learn 22         # what server-side apply actually is
byok8s learn 22 -hints  # and how to get there
```

The controller course is the same shape, and its stages are graded differently:
the program does not exit, so a stage starts it, changes the cluster underneath
it, waits for it to converge, and stops it.

## The website

Everything the CLI shows is also published at
**[byok8s.madhan.app](https://byok8s.madhan.app)** — the primer, a concept
index, and a catalog of every stage carrying its note and the diff its
reference solution adds. Nothing there is written twice: `web/gen` derives it
all from `courses.yml`, each `course.yml`, the notes and the snapshots, so a
new stage appears on the site the moment it lands.

```sh
mise run gen              # regenerate the catalog data and generated pages
mise run site             # and serve it locally
```

## Repo layout

```
courses.yml               every course, shipped and planned
CURRICULUM.md             the whole ladder, with a stage list per course
courses/<course>/
  course.yml              the stage list
  app/                    your program
  reference/work/         the authoring copy (solutions are written here first)
  reference/stages/NN-*/  a verified snapshot per stage
  learn/index.md          the course primer
  learn/NN-*.md           one concept note per stage
internal/course           courses.yml and course.yml
internal/cluster          kind lifecycle
internal/learn            loads the primer and the stage notes
internal/kube             namespace isolation and cluster guards
internal/runner           builds, invokes and supervises your program
internal/stages           the shape a course's assertions share
internal/stages/<course>  the assertions, one function per stage
hack/vsnap.sh             verify <course> 1..N, snapshot only on a full pass
hack/sweep.sh             fail if a stage's snapshot repeats the one before it
web/                      the published site (Astro + Starlight)
web/gen                   renders the site's catalog and pages from the repo
```

## Adding a course

A course lands only with all five pieces: a `course.yml` stage list, assertions
in `internal/stages/<slug>`, a verified reference snapshot per stage, a note per
stage, and a primer. Two lines connect it to the harness — an entry in
`courses.yml` with `status: shipped`, and a line in `cmd/tester`'s course
table. Everything else (the CLI, the site, CI) picks it up from there.
