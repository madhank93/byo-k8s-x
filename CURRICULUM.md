# Curriculum

Every course here is one Go program you grow across its stages, verified
against a real cluster. The ladder runs from the client you already use down
to the control plane that answers it.

`[x]` = shipped. Stage counts for unshipped courses are the authoring target,
not a promise to the digit.

A course lands only when it has all five pieces: a `course.yml` stage list,
stage assertions in `internal/stages/<slug>`, a verified reference snapshot per
stage, one concept note per stage, and a primer. Anything less is a draft.

## The order

```
kubectl → controller → webhook → scheduler → apiserver
                                     ↓
                      runtime → kubelet → cni → kube-proxy
```

Ecosystem courses (helm, kustomize, csi, hpa, kubeadm) need nothing past
`controller` and can be taken at any point after it.

---

## Tier 0 — the client

Where every Kubernetes engineer already lives. Nothing here needs more than a
kubeconfig and one node.

### [x] `kubectl` — Build your own kubectl · 30 stages

Shipped. kubeconfig and contexts, discovery, the RESTMapper, typed and dynamic
clients, printing, selectors, watch, delete, apply with server-side apply,
patch, scale, describe, logs, exec, port-forward.

**Verified by** running your binary as a subprocess against a seeded namespace
and asserting on its exit code and stdout.

---

## Tier 1 — control loops

The step from *asking* the cluster to *changing* it. All three run as a host
process against the same kind cluster; only the scheduler needs more than one
node.

### [x] `controller` — Build your own controller · 28 stages

Shipped. The reconcile loop, from a bare watch to a leader-elected operator
with its own CRD: a `Website` custom resource becomes a Deployment and a
Service, owned, repaired, reported on and cleaned up after.

| # | stage | adds |
|---|---|---|
| 1 | `install-crd` | the CustomResourceDefinition, installed by the program and waited on |
| 2 | `crd-schema` | typed fields, required and defaulted, plus printer columns |
| 3 | `list-then-watch` | resume a watch from the list's resourceVersion; miss nothing, repeat nothing |
| 4 | `shared-informer` | the factory, handlers, and the moment the cache is trustworthy |
| 5 | `lister` | read the cache, never the API server |
| 6 | `workqueue` | keys, not objects; a burst collapses into one pass |
| 7 | `reconcile` | level-triggered: the event is a hint, the cluster is the truth |
| 8 | `create-child` | the Deployment a Website means |
| 9 | `owner-references` | deleting the parent collects the child |
| 10 | `adopt-existing` | a matching child already there is claimed, not duplicated |
| 11 | `repair-drift` | someone scales it by hand; the next pass puts it back |
| 12 | `update-spec` | editing the Website rolls the change out |
| 13 | `second-child` | the Service, selecting exactly the pods the Deployment makes |
| 14 | `requeue-on-error` | a spec the cluster refuses is retried with backoff, not dropped |
| 15 | `requeue-after` | a timer catches the drift no event would report |
| 16 | `paused-annotation` | an operator can take one object out of your hands |
| 17 | `status-subresource` | `.status` without touching spec; `observedGeneration` |
| 18 | `conditions` | a `Ready` condition with a reason, and a transition time that means something |
| 19 | `finalizer` | hold a deleted object open, clean up, then let go |
| 20 | `conflict-retry` | a 409 is normal: re-read and re-apply |
| 21 | `server-side-apply` | one call for create, adopt and repair, with a field manager |
| 22 | `secondary-watch` | child events mapped back to the owner |
| 23 | `foreign-children` | never touch an object another controller owns |
| 24 | `events` | transitions recorded where `kubectl describe` shows them |
| 25 | `metrics` | passes, failures and queue depth over HTTP |
| 26 | `health-probes` | `/healthz` unconditional, `/readyz` behind the cache sync |
| 27 | `leader-election` | a Lease; two instances, one worker |
| 28 | `graceful-shutdown` | SIGTERM, drain, release the lease, exit 0 |

**Verified by** starting your binary, mutating the cluster, and waiting for it
to converge — then stopping it and checking it let go cleanly.

### `webhook` — Build your own admission webhook · 20 stages

Serving TLS, `AdmissionReview` in and out, mutation by JSON patch, and the
operational traps that take clusters down.

`serve-tls` · `admission-review` · `register-validating` · `deny` ·
`uid-echo` · `rules-scope` · `namespace-selector` · `object-selector` ·
`mutate-patch` · `defaulting` · `sidecar-inject` · `dry-run` ·
`failure-policy` · `timeout` · `reinvocation` · `cert-rotation` ·
`audit-annotations` · `match-conditions` · `validating-admission-policy` ·
`self-exclusion`

**Verified by** registering your webhook into the cluster and applying objects
with `kubectl` — the verdict is whether the API server accepted or rejected
them, and what came back changed.

### `scheduler` — Build your own scheduler · 24 stages

Filter, score, bind. Then the parts that make it a real scheduler: assumed
pods, spreading, priority and preemption.

`watch-unscheduled` · `bind` · `node-list` · `fit-resources` · `fit-ports` ·
`node-selector` · `node-affinity` · `taints` · `unschedulable` · `events` ·
`informer-cache` · `assumed-pods` · `score-least-allocated` ·
`score-balanced` · `score-image-locality` · `spread-by-owner` ·
`topology-spread` · `pod-affinity` · `volume-binding` · `priority` ·
`preemption` · `framework-plugins` · `multi-profile` ·
`percentage-of-nodes`

**Verified by** pods carrying `spec.schedulerName: byok8s` — the default
scheduler ignores them, so your program is the only thing that can place them,
and the assertion is which node they land on.

**Needs** a multi-node kind cluster (`internal/cluster` takes a topology from
`course.yml`).

---

## Tier 2 — the API plane

### `apiserver` — Build your own kube-apiserver · 30 stages

The capstone of the control plane. Everything the other courses talk to, built
until the real `kubectl` can drive it.

`serve` · `discovery-root` · `resource-create` · `resource-get` · `list` ·
`update` · `delete` · `etcd` · `resourceversion` · `namespaces` ·
`field-selector` · `label-selector` · `pagination` · `watch` ·
`watch-from-rv` · `bookmarks` · `patch-merge` · `patch-json` ·
`patch-strategic` · `apply-ssa` · `apply-conflict` · `subresource-status` ·
`subresource-scale` · `openapi` · `authn-token` · `authn-cert` ·
`authz-rbac` · `admission-chain` · `audit-log` · `kubectl-drives-it`

**Verified by** HTTP assertions early, and by shelling out to the real
`kubectl` late — `kubectl get`, `apply`, `edit`, `scale` and `--watch` against
your server, with no kind cluster involved at all.

---

## Tier 3 — the node plane

Where Kubernetes stops being an API and starts being Linux. Each of these
needs namespaces, cgroups and mounts, so the binary is cross-compiled for
Linux, copied into a kind node container and run there; assertions use
`crictl`, `nsenter` and the node's own filesystem. One harness change
(`runner.NodeExec`) unlocks all four.

### `runtime` — Build your own container runtime · 26 stages

An image on a registry, turned into a running process, then wrapped in the CRI
so a real kubelet can drive it.

`registry-auth` · `manifest-fetch` · `layer-pull` · `unpack` ·
`overlay-rootfs` · `image-config` · `spawn` · `mount-ns` · `pivot-root` ·
`pid-ns` · `uts-ipc-ns` · `net-ns` · `cgroup-limit` · `cgroup-stats` ·
`user-ns` · `capabilities` · `seccomp` · `oci-bundle` · `cri-identity` ·
`cri-image` · `cri-sandbox` · `cri-container` · `cri-status` · `cri-logs` ·
`cri-exec` · `kubelet-drives-it`

### `kubelet` — Build your own kubelet · 28 stages

`node-register` · `node-status` · `node-lease` · `static-pod` ·
`watch-assigned` · `cri-connect` · `sandbox` · `image-pull` ·
`container-run` · `pod-status` · `restart-backoff` · `init-containers` ·
`env-and-downward-api` · `configmap-volume` · `secret-volume` · `emptydir` ·
`hostpath` · `projected-token` · `liveness-probe` · `readiness-probe` ·
`startup-probe` · `graceful-delete` · `lifecycle-hooks` · `eviction` ·
`garbage-collection` · `logs-api` · `exec-api` · `cni-invoke`

### `cni` — Build your own CNI plugin · 20 stages

`plugin-contract` · `version` · `netns` · `veth` · `ipam-static` ·
`ipam-persist` · `routes` · `bridge` · `addressing` · `sysctls` · `del` ·
`check` · `conflist` · `portmap` · `bandwidth` · `hostport-cleanup` ·
`node-routes` · `vxlan` · `netpol-lite` · `kubelet-drives-it`

### `kube-proxy` — Build your own kube-proxy · 20 stages

`watch-services` · `watch-endpointslices` · `nft-tables` · `clusterip-dnat` ·
`probability` · `session-affinity` · `masquerade` · `nodeport` ·
`external-traffic-policy` · `headless` · `named-ports` · `udp` ·
`endpoint-removal` · `terminating-endpoints` · `topology-hints` ·
`loadbalancer` · `incremental-sync` · `resync` · `metrics` ·
`second-dataplane`

---

## Tier 4 — the ecosystem

Host processes again. Independent of tiers 2 and 3 — take them whenever.

### `helm` — Build your own Helm · 22 stages

`chart-load` · `values-merge` · `template-engine` · `builtin-objects` ·
`template-functions` · `named-templates` · `subcharts` · `conditions` ·
`values-schema` · `manifest-split` · `install` · `release-storage` ·
`list-status` · `history` · `upgrade-3way-merge` · `rollback` · `hooks` ·
`hook-weights` · `test` · `uninstall` · `dependencies` · `template-parity`

### `kustomize` — Build your own kustomize · 18 stages

`kustomization-load` · `resources` · `namespace` · `name-prefix` ·
`common-labels` · `common-annotations` · `images` · `replicas` ·
`configmap-generator` · `secret-generator` · `name-hash` ·
`name-references` · `strategic-merge-patch` · `json6902-patch` ·
`patch-targets` · `overlays` · `components` · `replacements`

### `csi` — Build your own CSI driver · 20 stages

`identity` · `registration` · `controller-capabilities` · `create-volume` ·
`delete-volume` · `topology` · `node-stage` · `node-publish` ·
`node-unpublish` · `filesystem` · `expansion` · `controller-publish` ·
`node-info` · `ephemeral-inline` · `snapshot-create` · `snapshot-restore` ·
`clone` · `capacity` · `idempotency` · `sidecars-drive-it`

### `hpa` — Build your own metrics server and HPA · 18 stages

`summary-api` · `cadvisor-parse` · `pod-aggregation` · `apiservice` ·
`pod-metrics` · `node-metrics` · `kubectl-top` · `hpa-watch` ·
`scaling-formula` · `tolerance` · `readiness-window` · `missing-metrics` ·
`scale-subresource` · `stabilization` · `behavior-policies` ·
`custom-metrics` · `external-metrics` · `multi-metric`

### `kubeadm` — Build your own cluster bootstrap · 22 stages

`ca` · `serving-certs` · `client-certs` · `sa-keys` · `kubeconfigs` ·
`etcd-static-pod` · `apiserver-static-pod` · `controller-manager` ·
`scheduler` · `kubelet-config` · `bootstrap-token` · `csr-approval` ·
`cluster-info` · `kube-proxy-addon` · `dns-addon` · `cni-install` ·
`node-join` · `control-plane-join` · `upgrade` · `cert-renewal` · `reset` ·
`smoke-test`

---

## Authoring a stage

The loop, and the two traps in it:

```sh
# 1. write the stage's assertions in internal/stages/<course>/stages.go
# 2. grow reference/work/main.go until they pass
BYOK8S_COURSE=<course> BYOK8S_SUBMISSION_DIR=$tmp \
  BYOK8S_TEST_CASES_JSON='[{"slug":"<stage>","title":"x"}]' go run ./cmd/tester
# 3. snapshot it, which re-verifies 1..N first
./hack/vsnap.sh <course> <N>
```

**Snapshot before you move on.** `vsnap.sh` copies whatever is in
`reference/work/` at the end of the run, so editing it for stage N+1 before
snapshotting stage N stores the wrong program under stage N.

**A cumulative run is slow, and gets slower.** Stage N re-verifies 1..N, so
late stages of a long course take minutes. Grade a single stage with
`cmd/tester` directly while authoring, and keep `vsnap.sh` for the snapshot.

**An assertion that writes to a custom resource must retry on conflict.** Once
the learner's controller writes status, every read-modify-write from the
harness races it.

## What each tier needs from the harness

| Need | Wanted by | State |
|---|---|---|
| Course-aware tester dispatch, `--course` | every course past `kubectl` | built alongside `controller` |
| A long-running learner process (start / assert / SIGTERM) | `controller`, `webhook`, `scheduler`, `apiserver` | built alongside `controller` |
| Convergence fixtures (`WaitFor`, CRD helpers) | `controller` onward | built alongside `controller` |
| Multi-node kind, topology from `course.yml` | `scheduler`, `cni`, `kube-proxy` | not built |
| No-cluster courses (nothing to `kind up`) | `apiserver`, `kustomize` | not built |
| `runner.NodeExec` — cross-compile, copy into a kind node, run there | all of tier 3, `csi` | not built |
| A non-Go course | none yet | `course.yml` carries `language`/`build`; `internal/runner` still assumes Go |
