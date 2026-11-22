// Package kustomize holds the assertions for the "Build your own kustomize"
// course — one function per stage, registered by slug.
//
// Every stage writes kustomization trees to a temporary directory and runs
// `build DIR` on both the learner's program and the kustomize built into the
// pinned kubectl. Until the last stage the two outputs are compared as data,
// document by document and in order; the last stage compares them byte for
// byte. No cluster is involved.
package kustomize

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"sigs.k8s.io/yaml"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// Stage is one gradable step, in the shape cmd/tester dispatches on.
type Stage = stages.Stage

var registry = map[string]Stage{}

func register(s Stage) { registry[s.Slug] = s }

// Lookup returns the stage with this slug.
func Lookup(slug string) (Stage, bool) {
	s, ok := registry[slug]
	return s, ok
}

func init() {
	register(Stage{Slug: "kustomization-load", Run: stageKustomizationLoad})
	register(Stage{Slug: "resources", Run: stageResources})
	register(Stage{Slug: "namespace", Run: stageNamespace})
	register(Stage{Slug: "name-prefix", Run: stageNamePrefix})
	register(Stage{Slug: "common-labels", Run: stageCommonLabels})
	register(Stage{Slug: "common-annotations", Run: stageCommonAnnotations})
	register(Stage{Slug: "images", Run: stageImages})
	register(Stage{Slug: "replicas", Run: stageReplicas})
	register(Stage{Slug: "configmap-generator", Run: stageConfigMapGenerator})
	register(Stage{Slug: "secret-generator", Run: stageSecretGenerator})
	register(Stage{Slug: "name-hash", Run: stageNameHash})
}

// tree is a kustomization directory as slash paths to contents.
type tree map[string]string

// check is one directory to build, and what it teaches when it fails.
type check struct {
	why  string
	root string // the directory to build, relative to the tree
	t    tree
}

// stageKustomizationLoad — the kustomization file is found under any of its
// three names, read strictly, and the files it lists are printed back as one
// YAML stream; every way the file can be wrong is refused.
func stageKustomizationLoad(ctx context.Context, _ *kube.Env, bin string) error {
	cm := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\ndata:\n  mode: fast\n  retries: \"3\"\n"
	svc := "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\nspec:\n  ports:\n  - port: 80\n    targetPort: 8080\n"
	return runChecks(ctx, bin, []check{
		{"kustomization.yaml lists two files, printed in the order listed", ".", tree{
			"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n- cm.yaml\n- svc.yaml\n",
			"cm.yaml":            cm, "svc.yaml": svc,
		}},
		{"kustomization.yml is the second name kustomize looks for, and apiVersion and kind are optional", ".", tree{
			"kustomization.yml": "resources:\n- svc.yaml\n", "svc.yaml": svc,
		}},
		{"Kustomization, capitalised and with no extension, is the third", ".", tree{
			"Kustomization": "resources: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"a resource may leave out apiVersion; kind and metadata.name are what kustomize insists on", ".", tree{
			"kustomization.yaml": "resources: [bare.yaml]\n", "bare.yaml": "kind: ConfigMap\nmetadata:\n  name: bare\n",
		}},
		{"a resource file can live in a subdirectory", ".", tree{
			"kustomization.yaml": "resources: [manifests/cm.yaml]\n", "manifests/cm.yaml": cm,
		}},
		{"there is no kustomization file at all", ".", tree{"cm.yaml": cm}},
		{"kustomization.yaml and kustomization.yml both exist, and kustomize will not guess", ".", tree{
			"kustomization.yaml": "resources: [cm.yaml]\n", "kustomization.yml": "resources: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"the kustomization file is empty", ".", tree{"kustomization.yaml": ""}},
		{"`resourcse` is not a field kustomize knows, and an unknown field is an error, not a typo to skip", ".", tree{
			"kustomization.yaml": "resourcse: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"resources is a list, not a string", ".", tree{
			"kustomization.yaml": "resources: cm.yaml\n", "cm.yaml": cm,
		}},
		{"kind must be Kustomization", ".", tree{
			"kustomization.yaml": "kind: Kustomisation\nresources: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"a Kustomization's apiVersion must be kustomize.config.k8s.io/v1beta1", ".", tree{
			"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v2\nkind: Kustomization\nresources: [cm.yaml]\n", "cm.yaml": cm,
		}},
		{"a listed file that does not exist", ".", tree{
			"kustomization.yaml": "resources: [cm.yaml, missing.yaml]\n", "cm.yaml": cm,
		}},
		{"a resource with no kind", ".", tree{
			"kustomization.yaml": "resources: [r.yaml]\n", "r.yaml": "apiVersion: v1\nmetadata:\n  name: x\n",
		}},
		{"a resource with no metadata.name", ".", tree{
			"kustomization.yaml": "resources: [r.yaml]\n", "r.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  labels:\n    a: b\n",
		}},
	})
}

// mixed is one file holding a little of everything, in no particular order,
// for the stages that judge kustomize's output order.
const mixed = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: nginx:1.27
---
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  selector:
    app: web
  ports:
  - port: 80
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: guard
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: b-settings
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: a-settings
  namespace: zeta
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: a-settings
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: a-settings
  namespace: alpha
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: deny
spec:
  podSelector: {}
---
apiVersion: v1
kind: Pod
metadata:
  name: solo
spec:
  containers:
  - name: c
    image: busybox
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: agent
---
apiVersion: v1
kind: Namespace
metadata:
  name: tools
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: reader
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: runner
`

// stageResources — resources come from multi-document files, from List
// objects, and from other kustomization directories built recursively; the
// whole result is ordered the way kustomize orders it, and two resources with
// the same identity, a cycle, or a file outside the root are refused.
func stageResources(ctx context.Context, _ *kube.Env, bin string) error {
	cm := func(name string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\n"
	}
	return runChecks(ctx, bin, []check{
		{"one file holding many documents comes out in kustomize's order: Namespace, then the kinds kustomize ranks first, then everything else by group, version and kind, webhooks last, and by name then namespace within a kind", ".", tree{
			"kustomization.yaml": "resources: [all.yaml]\n", "all.yaml": mixed,
		}},
		{"empty documents and comment-only documents are not resources", ".", tree{
			"kustomization.yaml": "resources: [a.yaml]\n",
			"a.yaml":             "---\n" + cm("one") + "---\n# only a comment\n---\n\n---\n" + cm("two"),
		}},
		{"a List is unwrapped into its items, and so is any kind ending in List", ".", tree{
			"kustomization.yaml": "resources: [list.yaml, cmlist.yaml]\n",
			"list.yaml":          "apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: Service\n  metadata:\n    name: s1\n- apiVersion: v1\n  kind: ConfigMap\n  metadata:\n    name: c1\n",
			"cmlist.yaml":        "apiVersion: v1\nkind: ConfigMapList\nitems:\n- apiVersion: v1\n  kind: ConfigMap\n  metadata:\n    name: c2\n",
		}},
		{"a directory in resources is another kustomization, built on its own and added in", "app", tree{
			"app/kustomization.yaml":        "resources:\n- ../base\n- extra.yaml\n",
			"app/extra.yaml":                cm("extra"),
			"base/kustomization.yaml":       "resources:\n- deploy.yaml\n- nested\n",
			"base/deploy.yaml":              "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: api\n",
			"base/nested/kustomization.yml": "resources: [cm.yaml]\n",
			"base/nested/cm.yaml":           cm("nested"),
		}},
		{"the same name in two namespaces is two resources", ".", tree{
			"kustomization.yaml": "resources: [a.yaml, b.yaml]\n",
			"a.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: same\n  namespace: a\n",
			"b.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: same\n  namespace: b\n",
		}},
		{"two resources with the same kind, name and namespace are one identity twice, which kustomize refuses", ".", tree{
			"kustomization.yaml": "resources: [a.yaml, b.yaml]\n", "a.yaml": cm("same"), "b.yaml": cm("same"),
		}},
		{"a duplicate arriving from a sub-kustomization is refused the same way", "app", tree{
			"app/kustomization.yaml": "resources: [../base, cm.yaml]\n", "app/cm.yaml": cm("shared"),
			"base/kustomization.yaml": "resources: [cm.yaml]\n", "base/cm.yaml": cm("shared"),
		}},
		{"a directory with no kustomization file cannot be a resource", ".", tree{
			"kustomization.yaml": "resources: [plain]\n", "plain/cm.yaml": cm("x"),
		}},
		{"a and b include each other, which is a cycle", "a", tree{
			"a/kustomization.yaml": "resources: [../b]\n", "b/kustomization.yaml": "resources: [../a]\n",
		}},
		{"a file outside the kustomization's own directory is refused: only directories may reach outside", "app", tree{
			"app/kustomization.yaml": "resources: [../shared/cm.yaml]\n", "shared/cm.yaml": cm("shared"),
		}},
		{"a file that is not YAML at all", ".", tree{
			"kustomization.yaml": "resources: [bad.yaml]\n", "bad.yaml": "kind: ConfigMap\nmetadata: {name: [\n",
		}},
	})
}

// clusterScoped is one of each kind of object that has no namespace, plus a
// look-alike from another API group that does.
const clusterScoped = `apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: reader
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: reader
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: reader
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: disk
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: fast
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: urgent
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: guard
webhooks:
- name: guard.example.com
  clientConfig:
    service:
      name: guard
      namespace: guard-system
---
apiVersion: example.com/v1
kind: ClusterRole
metadata:
  name: not-rbac
`

// stageNamespace — namespace moves every namespaced resource into one
// namespace, overwriting what was there, leaves cluster-scoped ones alone,
// renames a Namespace object to match, and refuses the collisions that makes.
func stageNamespace(ctx context.Context, _ *kube.Env, bin string) error {
	return runChecks(ctx, bin, []check{
		{"every namespaced resource lands in prod, the one that named its own namespace too", ".", tree{
			"kustomization.yaml": "namespace: prod\nresources: [all.yaml]\n",
			"all.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: settings
  namespace: somewhere-else
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
`,
		}},
		{"cluster-scoped kinds have no namespace to set, and a kind is judged with its API group: example.com's ClusterRole is namespaced", ".", tree{
			"kustomization.yaml": "namespace: prod\nresources: [cluster.yaml]\n", "cluster.yaml": clusterScoped,
		}},
		{"a Namespace object in the resources is renamed to the namespace everything else now lives in", ".", tree{
			"kustomization.yaml": "namespace: prod\nresources: [ns.yaml, cm.yaml]\n",
			"ns.yaml":            "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: dev\n  labels:\n    team: a\n",
			"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  namespace: dev\n",
		}},
		{"a binding's subject named default gets the namespace, whatever its kind; other subjects are left alone", ".", tree{
			"kustomization.yaml": "namespace: prod\nresources: [rb.yaml]\n",
			"rb.yaml": `apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: rb
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: r
subjects:
- kind: ServiceAccount
  name: default
  namespace: dev
- kind: ServiceAccount
  name: default
- kind: User
  name: default
- kind: ServiceAccount
  name: builder
  namespace: ci
- kind: Group
  name: admins
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: crb
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: r
subjects:
- kind: ServiceAccount
  name: default
  namespace: kube-system
- kind: ServiceAccount
  name: builder
  namespace: ci
`,
		}},
		{"each kustomization applies its own namespace to what it builds, so the outer one has the last word", "app", tree{
			"app/kustomization.yaml":  "namespace: prod\nresources: [../base, extra.yaml]\n",
			"app/extra.yaml":          "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: extra\n",
			"base/kustomization.yaml": "namespace: dev\nresources: [../lib, cm.yaml]\n",
			"base/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: base\n",
			"lib/kustomization.yaml":  "resources: [cm.yaml]\n",
			"lib/cm.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: lib\n  namespace: lib\n",
		}},
		{"an inner namespace survives an outer kustomization that sets none", "app", tree{
			"app/kustomization.yaml":  "resources: [../base]\n",
			"base/kustomization.yaml": "namespace: dev\nresources: [cm.yaml]\n",
			"base/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: base\n",
		}},
		{"two ConfigMaps called settings, in two namespaces, become one identity twice once both are in prod", ".", tree{
			"kustomization.yaml": "namespace: prod\nresources: [a.yaml, b.yaml]\n",
			"a.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  namespace: a\n",
			"b.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  namespace: b\n",
		}},
		{"two Namespace objects would both be renamed prod", ".", tree{
			"kustomization.yaml": "namespace: prod\nresources: [ns.yaml]\n",
			"ns.yaml":            "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: one\n---\napiVersion: v1\nkind: Namespace\nmetadata:\n  name: two\n",
		}},
	})
}

// stageNamePrefix — namePrefix and nameSuffix wrap every name except the
// three kinds whose names mean something to the API server, and each level of
// kustomization wraps the names the level below produced.
func stageNamePrefix(ctx context.Context, _ *kube.Env, bin string) error {
	unprefixed := `apiVersion: v1
kind: Namespace
metadata:
  name: tools
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
---
apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata:
  name: v1beta1.metrics.example.com
`
	return runChecks(ctx, bin, []check{
		{"namePrefix goes in front of every name, cluster-scoped ones included", ".", tree{
			"kustomization.yaml": "namePrefix: prod-\nresources: [all.yaml, cluster.yaml]\n", "all.yaml": mixed, "cluster.yaml": clusterScoped,
		}},
		{"nameSuffix goes on the end", ".", tree{
			"kustomization.yaml": "nameSuffix: -v2\nresources: [all.yaml]\n", "all.yaml": mixed,
		}},
		{"a Namespace, a CustomResourceDefinition and an APIService keep their names: the server derives them from what they define", ".", tree{
			"kustomization.yaml": "namePrefix: prod-\nnameSuffix: -v2\nresources: [fixed.yaml]\n", "fixed.yaml": unprefixed,
		}},
		{"the outer kustomization's prefix goes outside the inner one's, and its suffix after", "app", tree{
			"app/kustomization.yaml":  "namePrefix: outer-\nnameSuffix: -outer\nresources: [../base, own.yaml]\n",
			"app/own.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: own\n",
			"base/kustomization.yaml": "namePrefix: inner-\nnameSuffix: -inner\nresources: [cm.yaml]\n",
			"base/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n",
		}},
		{"namespace and namePrefix together: the Namespace object takes the namespace's name, unprefixed", ".", tree{
			"kustomization.yaml": "namespace: prod\nnamePrefix: app-\nresources: [r.yaml]\n",
			"r.yaml":             "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: dev\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n",
		}},
	})
}

// workloads is every kind with a pod template or a selector, each with the
// optional fields commonLabels reaches into only when they are already there.
const workloads = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  labels:
    tier: front
spec:
  template:
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchLabels:
                tier: front
            topologyKey: kubernetes.io/hostname
          - labelSelector:
              matchExpressions: []
            topologyKey: zone
      topologySpreadConstraints:
      - labelSelector:
          matchLabels:
            tier: front
        maxSkew: 1
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: db
spec:
  volumeClaimTemplates:
  - metadata:
      name: data
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: agent
spec:
  template:
    spec:
      topologySpreadConstraints:
      - labelSelector:
          matchLabels:
            x: "y"
---
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: rs
---
apiVersion: v1
kind: ReplicationController
metadata:
  name: rc
---
apiVersion: batch/v1
kind: Job
metadata:
  name: once
spec:
  selector:
    matchLabels:
      job: once
---
apiVersion: batch/v1
kind: Job
metadata:
  name: twice
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly
spec:
  schedule: "0 0 * * *"
---
apiVersion: v1
kind: Service
metadata:
  name: web
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: web
spec:
  minAvailable: 1
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: db
spec:
  selector:
    matchLabels:
      app: db
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow
spec:
  podSelector: {}
  ingress:
  - from:
    - podSelector:
        matchLabels:
          role: client
    - namespaceSelector: {}
    - podSelector:
        matchLabels:
          role: admin
---
apiVersion: v1
kind: Pod
metadata:
  name: solo
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
spec:
  selector:
    matchLabels:
      a: b
  template:
    metadata: {}
`

// stageCommonLabels — commonLabels lands on every resource and on the
// selectors and pod templates that must agree with it; labels does the same
// with each part switched on separately.
func stageCommonLabels(ctx context.Context, _ *kube.Env, bin string) error {
	return runChecks(ctx, bin, []check{
		{"commonLabels goes on every object's metadata, and into each selector and template kustomize knows of — creating the ones a workload must have, extending the optional ones only where they exist, and never touching a kind it does not know", ".", tree{
			"kustomization.yaml": "commonLabels:\n  app: shop\n  tier: back\nresources: [workloads.yaml]\n", "workloads.yaml": workloads,
		}},
		{"labels with neither switch is metadata only", ".", tree{
			"kustomization.yaml": "labels:\n- pairs:\n    owner: team-a\nresources: [workloads.yaml]\n", "workloads.yaml": workloads,
		}},
		{"includeTemplates adds the pod templates (and the claim templates), but no selector", ".", tree{
			"kustomization.yaml": "labels:\n- pairs:\n    version: v2\n  includeTemplates: true\nresources: [workloads.yaml]\n", "workloads.yaml": workloads,
		}},
		{"includeSelectors is commonLabels, wherever it reaches", ".", tree{
			"kustomization.yaml": "labels:\n- pairs:\n    app: shop\n  includeSelectors: true\n- pairs:\n    owner: team-a\nresources: [workloads.yaml]\n", "workloads.yaml": workloads,
		}},
		{"labels and commonLabels together, the outer kustomization's label overriding the inner one's value", "app", tree{
			"app/kustomization.yaml":  "commonLabels:\n  env: prod\nlabels:\n- pairs:\n    team: core\nresources: [../base]\n",
			"base/kustomization.yaml": "commonLabels:\n  env: dev\n  app: web\nresources: [svc.yaml]\n",
			"base/svc.yaml":           "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\nspec:\n  selector:\n    app: old\n",
		}},
		{"a label value is a string: kustomize refuses a number", ".", tree{
			"kustomization.yaml": "commonLabels:\n  replicas: 3\nresources: [svc.yaml]\n",
			"svc.yaml":           "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n",
		}},
	})
}

// stageCommonAnnotations — commonAnnotations lands on every object and on
// every pod template, never on a selector, and overrides what was there.
func stageCommonAnnotations(ctx context.Context, _ *kube.Env, bin string) error {
	return runChecks(ctx, bin, []check{
		{"commonAnnotations goes on every object's metadata and into every pod template kustomize knows of, creating them", ".", tree{
			"kustomization.yaml": "commonAnnotations:\n  owner: team-a\n  docs: https://example.com/runbook\nresources: [workloads.yaml]\n", "workloads.yaml": workloads,
		}},
		{"an annotation already there keeps its neighbours and loses its value; the outer kustomization has the last word", "app", tree{
			"app/kustomization.yaml":  "commonAnnotations:\n  stage: prod\nresources: [../base]\n",
			"base/kustomization.yaml": "commonAnnotations:\n  stage: dev\n  build: \"42\"\ncommonLabels:\n  app: web\nresources: [d.yaml]\n",
			"base/d.yaml":             "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  annotations:\n    stage: none\n    keep: me\nspec:\n  template:\n    metadata:\n      annotations:\n        keep: pod\n",
		}},
		{"an annotation value is a string: kustomize refuses a boolean", ".", tree{
			"kustomization.yaml": "commonAnnotations:\n  enabled: true\nresources: [svc.yaml]\n",
			"svc.yaml":           "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n",
		}},
	})
}

// stageImages — images rewrites the name, tag and digest of every container
// and init container whose image name matches exactly, wherever in the object
// the containers are, entry by entry in the order they are listed.
func stageImages(ctx context.Context, _ *kube.Env, bin string) error {
	pods := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  template:
    spec:
      initContainers:
      - name: migrate
        image: localhost:5000/tools/migrate:1.0
      - name: seed
        image: localhost:5000/tools/migrate
      containers:
      - name: web
        image: nginx
      - name: pinned
        image: nginx:1.25@sha256:0000000000000000000000000000000000000000000000000000000000000001
      - name: cache
        image: redis:7.2@sha256:0000000000000000000000000000000000000000000000000000000000000002
      - name: shell
        image: busybox:1.36
      - name: lookalike
        image: nginx-exporter:0.11
      - name: namespaced
        image: library/nginx:1.25
      - name: registry
        image: docker.io/nginx:1.25
      - name: noimage
      ephemeralContainers:
      - name: debug
        image: busybox:1.36
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: backup
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: busybox
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
spec:
  image: nginx
  runner:
    containers:
    - name: inner
      image: redis
`
	return runChecks(ctx, bin, []check{
		{"newTag replaces the tag and drops a digest; newName keeps both; digest replaces the tag; a name only matches the whole name, registry and path included; ephemeral containers and other image fields are left alone; containers are found wherever they are", ".", tree{
			"kustomization.yaml": `images:
- name: nginx
  newTag: "1.27"
- name: redis
  newName: valkey/valkey
- name: busybox
  digest: sha256:00000000000000000000000000000000000000000000000000000000000000bb
- name: localhost:5000/tools/migrate
  newName: ghcr.io/acme/migrate
  newTag: "2.0"
resources: [pods.yaml]
`, "pods.yaml": pods,
		}},
		{"newTag with digest keeps both, and entries apply in order, so a later entry sees what an earlier one renamed", ".", tree{
			"kustomization.yaml": `images:
- name: nginx
  newName: mirror/nginx
- name: mirror/nginx
  newTag: "1.27"
  digest: sha256:00000000000000000000000000000000000000000000000000000000000000aa
- name: busybox
  newTag: "1.37"
- name: busybox
  newName: alpine
resources: [pods.yaml]
`, "pods.yaml": pods,
		}},
		{"an outer kustomization's images apply after the inner one's", "app", tree{
			"app/kustomization.yaml":  "images:\n- name: nginx\n  newTag: \"1.27\"\nresources: [../base]\n",
			"base/kustomization.yaml": "images:\n- name: nginx\n  newTag: \"1.26\"\n- name: redis\n  newTag: \"7.4\"\nresources: [pods.yaml]\n",
			"base/pods.yaml":          pods,
		}},
		{"newTag is a string; 1.10 unquoted is a number, and kustomize refuses it rather than print 1.1", ".", tree{
			"kustomization.yaml": "images:\n- name: nginx\n  newTag: 1.10\nresources: [pods.yaml]\n", "pods.yaml": pods,
		}},
	})
}

// stageReplicas — replicas sets spec.replicas on the scalable kinds with a
// matching name, found by the name they have now or the one they started
// with, and refuses a name nothing scalable answers to.
func stageReplicas(ctx context.Context, _ *kube.Env, bin string) error {
	scalable := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 1
---
apiVersion: v1
kind: Service
metadata:
  name: web
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: db
---
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: rs
---
apiVersion: v1
kind: ReplicationController
metadata:
  name: rc
---
apiVersion: example.com/v1
kind: Deployment
metadata:
  name: web
---
apiVersion: batch/v1
kind: Job
metadata:
  name: once
`
	return runChecks(ctx, bin, []check{
		{"every scalable kind with the name gets the count, whatever its API group, and an existing count is overwritten; the Service called web is not scalable", ".", tree{
			"kustomization.yaml": "replicas:\n- name: web\n  count: 3\n- name: db\n  count: 0\n- name: rs\n  count: 2\n- name: rc\n  count: 4\nresources: [r.yaml]\n", "r.yaml": scalable,
		}},
		{"a prefixed resource answers to its new name and to the one it started with", ".", tree{
			"kustomization.yaml": "namePrefix: shop-\nreplicas:\n- name: web\n  count: 3\n- name: shop-db\n  count: 5\nresources: [r.yaml]\n", "r.yaml": scalable,
		}},
		{"the outer kustomization finds the inner one's Deployment by its original name, and its count wins", "app", tree{
			"app/kustomization.yaml":  "replicas:\n- name: web\n  count: 10\nresources: [../base]\n",
			"base/kustomization.yaml": "namePrefix: base-\nreplicas:\n- name: web\n  count: 2\nresources: [r.yaml]\n",
			"base/r.yaml":             scalable,
		}},
		{"a name no scalable resource has is an error, not a no-op: the Job called once cannot be scaled", ".", tree{
			"kustomization.yaml": "replicas:\n- name: once\n  count: 3\nresources: [r.yaml]\n", "r.yaml": scalable,
		}},
		{"count is a number", ".", tree{
			"kustomization.yaml": "replicas:\n- name: web\n  count: \"3\"\nresources: [r.yaml]\n", "r.yaml": scalable,
		}},
	})
}

// sources are the files a generator reads, a binary one among them.
var sources = tree{
	"app.conf":         "port = 8080\nworkers = 4\n",
	"conf/logging.ini": "[root]\nlevel = info\n",
	"logo.png":         "\x89PNG\r\n\x1a\n\x00\x00\xff\xfe",
	"app.env":          "# settings: MODE=slow is the default\nMODE=fast\n\nURL=https://example.com/?a=b\nQUOTED=\"kept\"\nEMPTY=\n",
}

// with returns a copy of t with more files.
func (t tree) with(more tree) tree {
	out := tree{}
	for k, v := range t {
		out[k] = v
	}
	for k, v := range more {
		out[k] = v
	}
	return out
}

// stageConfigMapGenerator — configMapGenerator makes a ConfigMap from
// literals, files and env files, with generatorOptions and per-generator
// options, and the result is a resource like any other. Every generator here
// turns the name hash off; the hash is stage 11.
func stageConfigMapGenerator(ctx context.Context, _ *kube.Env, bin string) error {
	return runChecks(ctx, bin, []check{
		{"literals split at the first =, lose one pair of surrounding quotes, and may be empty; files are keyed by base name unless a key= is given; a file that is not UTF-8 goes in binaryData; an env file is KEY=VALUE lines, comments and blank lines skipped, quotes kept", ".", sources.with(tree{
			"kustomization.yaml": `generatorOptions:
  disableNameSuffixHash: true
configMapGenerator:
- name: literals
  literals:
  - mode=fast
  - query=a=b
  - greeting="hello there"
  - single='quoted'
  - empty=
- name: files
  files:
  - app.conf
  - conf/logging.ini
  - settings.conf=app.conf
  - logo.png
- name: env
  envs:
  - app.env
- name: everything
  literals:
  - extra=yes
  files:
  - app.conf
  envs:
  - app.env
- name: nothing
`,
		})},
		{"generatorOptions labels and annotations go on every generated object, a generator's own options override them key by key, and immutable sets immutable", ".", tree{
			"kustomization.yaml": `generatorOptions:
  disableNameSuffixHash: true
  labels:
    generated: "true"
    owner: platform
  annotations:
    note: generated
configMapGenerator:
- name: plain
  literals: [a=1]
- name: owned
  literals: [a=1]
  options:
    labels:
      owner: team-a
    annotations:
      extra: "yes"
    immutable: true
`,
		}},
		{"a generated ConfigMap is a resource like the rest: namespace, namePrefix and commonLabels reach it, and kustomization's namespace beats the generator's own", ".", tree{
			"kustomization.yaml": `namespace: prod
namePrefix: shop-
commonLabels:
  app: shop
configMapGenerator:
- name: settings
  namespace: elsewhere
  literals: [a=1]
  options:
    disableNameSuffixHash: true
- name: other
  literals: [b=2]
  options:
    disableNameSuffixHash: true
resources: [cm.yaml]
`,
			"cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: handwritten\ndata:\n  c: \"3\"\n",
		}},
		{"a generator in an inner kustomization is built there, with that kustomization's transformers", "app", tree{
			"app/kustomization.yaml":  "namePrefix: outer-\nresources: [../base]\n",
			"base/kustomization.yaml": "namePrefix: inner-\ngeneratorOptions:\n  disableNameSuffixHash: true\nconfigMapGenerator:\n- name: settings\n  literals: [a=1]\n",
		}},
		{"a key may appear once: here a literal and a file both say app.conf", ".", sources.with(tree{
			"kustomization.yaml": "generatorOptions:\n  disableNameSuffixHash: true\nconfigMapGenerator:\n- name: twice\n  literals: [app.conf=inline]\n  files: [app.conf]\n",
		})},
		{"a literal with no = has no value", ".", tree{
			"kustomization.yaml": "generatorOptions:\n  disableNameSuffixHash: true\nconfigMapGenerator:\n- name: bad\n  literals: [novalue]\n",
		}},
		{"a file that is not there", ".", tree{
			"kustomization.yaml": "generatorOptions:\n  disableNameSuffixHash: true\nconfigMapGenerator:\n- name: bad\n  files: [missing.conf]\n",
		}},
		{"a file outside the kustomization's directory is refused, as resources refuse one", "app", tree{
			"app/kustomization.yaml": "generatorOptions:\n  disableNameSuffixHash: true\nconfigMapGenerator:\n- name: bad\n  files: [../shared.conf]\n",
			"shared.conf":            "x\n",
		}},
		{"a generated ConfigMap collides with a handwritten one of the same name", ".", tree{
			"kustomization.yaml": "generatorOptions:\n  disableNameSuffixHash: true\nconfigMapGenerator:\n- name: settings\n  literals: [a=1]\nresources: [cm.yaml]\n",
			"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n",
		}},
	})
}

// stageSecretGenerator — secretGenerator reads the same sources into a
// Secret, every value base64-encoded under data, with a type.
func stageSecretGenerator(ctx context.Context, _ *kube.Env, bin string) error {
	return runChecks(ctx, bin, []check{
		{"every value is base64 under data, binary ones too, and type defaults to Opaque", ".", sources.with(tree{
			"kustomization.yaml": `generatorOptions:
  disableNameSuffixHash: true
secretGenerator:
- name: credentials
  literals:
  - username=admin
  - password="p@ss=word"
  files:
  - app.conf
  - logo.png
  envs:
  - app.env
- name: tls
  type: kubernetes.io/tls
  files:
  - tls.crt=app.conf
  - tls.key=conf/logging.ini
- name: nothing
`,
		})},
		{"options, namespace and prefixes reach a Secret as they reach a ConfigMap, and a ConfigMap and a Secret may share a name", ".", tree{
			"kustomization.yaml": `namespace: prod
namePrefix: shop-
commonAnnotations:
  team: a
generatorOptions:
  disableNameSuffixHash: true
  labels:
    generated: "true"
configMapGenerator:
- name: settings
  literals: [mode=fast]
secretGenerator:
- name: settings
  literals: [token=abc]
  options:
    immutable: true
    labels:
      secret: "yes"
`,
		}},
		{"a Secret refuses a repeated key too", ".", tree{
			"kustomization.yaml": "generatorOptions:\n  disableNameSuffixHash: true\nsecretGenerator:\n- name: twice\n  literals: [a=1, a=2]\n",
		}},
		{"type belongs to secretGenerator only; configMapGenerator has no such field", ".", tree{
			"kustomization.yaml": "generatorOptions:\n  disableNameSuffixHash: true\nconfigMapGenerator:\n- name: settings\n  type: Opaque\n  literals: [a=1]\n",
		}},
	})
}

// stageNameHash — a generated object's name ends in a hash of its content,
// so changing the data changes the name; the hash covers kind, data and type
// but not the name, and is added last, once every kustomization has had its
// say.
func stageNameHash(ctx context.Context, _ *kube.Env, bin string) error {
	return runChecks(ctx, bin, []check{
		{"each generated ConfigMap and Secret gets a ten-character suffix computed from its kind, data and type — not its name, so two empty ConfigMaps share a suffix", ".", sources.with(tree{
			"kustomization.yaml": `configMapGenerator:
- name: literals
  literals: [mode=fast, retries=3]
- name: files
  files: [app.conf, logo.png]
- name: empty
- name: also-empty
- name: frozen
  literals: [mode=fast]
  options:
    immutable: true
secretGenerator:
- name: credentials
  literals: [password=s3cr3t]
- name: tls
  type: kubernetes.io/tls
  files: [tls.crt=app.conf, tls.key=conf/logging.ini]
- name: empty
`,
		})},
		{"labels and annotations are not content: these two hash as plain generators do", ".", tree{
			"kustomization.yaml": `commonLabels:
  app: shop
commonAnnotations:
  note: hi
generatorOptions:
  labels:
    generated: "true"
configMapGenerator:
- name: literals
  literals: [mode=fast, retries=3]
- name: unhashed
  literals: [mode=fast]
  options:
    disableNameSuffixHash: true
`,
		}},
		{"the hash goes after the prefix and suffix", ".", tree{
			"kustomization.yaml": "namespace: prod\nnamePrefix: shop-\nnameSuffix: -v1\nconfigMapGenerator:\n- name: settings\n  literals: [mode=fast]\n",
		}},
		{"a generator in an inner kustomization is hashed once, at the very end, so the hash lands after the outer suffix", "app", tree{
			"app/kustomization.yaml":  "namePrefix: outer-\nnameSuffix: -v2\nresources: [../base]\n",
			"base/kustomization.yaml": "namePrefix: inner-\nconfigMapGenerator:\n- name: settings\n  literals: [mode=fast]\nsecretGenerator:\n- name: token\n  literals: [t=abc]\n",
		}},
	})
}

// runChecks builds every check with both programs and compares them as data.
func runChecks(ctx context.Context, bin string, checks []check) error {
	w, err := newWork(ctx, bin)
	if err != nil {
		return err
	}
	defer w.close()
	for i, c := range checks {
		if err := w.same(c, fmt.Sprintf("case%02d", i+1), false); err != nil {
			return err
		}
	}
	return nil
}

// work is one stage's scratch space.
type work struct {
	ctx context.Context
	bin string
	dir string
}

func newWork(ctx context.Context, bin string) (*work, error) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		return nil, fmt.Errorf("this course is graded against `kubectl kustomize`, and there is no kubectl on PATH: install one (mise install provisions the version the course is graded with)")
	}
	dir, err := os.MkdirTemp("", "byok8s-kustomize-")
	if err != nil {
		return nil, err
	}
	w := &work{ctx: ctx, bin: bin, dir: dir}
	res, err := runner.Invoke(ctx, "kubectl", stages.Timeout, "version", "--client", "-o", "json")
	if err != nil {
		w.close()
		return nil, err
	}
	var v struct {
		Kustomize string `json:"kustomizeVersion"`
	}
	if json.Unmarshal([]byte(res.Stdout), &v) != nil || !strings.HasPrefix(v.Kustomize, "v5.") {
		w.close()
		return nil, fmt.Errorf("this course is graded against the kustomize v5 built into kubectl; `kubectl version --client` reports kustomize %q", v.Kustomize)
	}
	return w, nil
}

func (w *work) close() { os.RemoveAll(w.dir) }

// write lays a tree out under name and returns its directory. A failed write
// is a harness fault, so it panics rather than threading an error through.
func (w *work) write(name string, t tree) string {
	root := filepath.Join(w.dir, name)
	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}
	for p, body := range t {
		path := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			panic(err)
		}
	}
	return root
}

// same builds one check with kubectl kustomize and with the program. Where
// kustomize refuses, the program must exit 1 with a reason on stderr and
// nothing on stdout; otherwise it must print the same documents in the same
// order — as data, or byte for byte when exact is set.
func (w *work) same(c check, name string, exact bool) error {
	dir := filepath.Join(w.write(name, c.t), filepath.FromSlash(c.root))
	want, err := runner.Invoke(w.ctx, "kubectl", stages.Timeout, "kustomize", dir)
	if err != nil {
		return err
	}
	got, err := runner.Invoke(w.ctx, w.bin, stages.Timeout, "build", dir)
	if err != nil {
		return err
	}
	shown := describe(c)
	if want.ExitCode != 0 {
		if got.ExitCode != 1 || strings.TrimSpace(got.Stderr) == "" || got.Stdout != "" {
			return fmt.Errorf("%s\nkustomize refuses this (%s), so the program must exit 1 with a reason on stderr and print nothing; it exited %d\nstdout:\n%s\nstderr:\n%s",
				shown, firstLine(want.Stderr, w.dir), got.ExitCode, tail(got.Stdout), tail(got.Stderr))
		}
		return nil
	}
	if got.ExitCode != 0 {
		return fmt.Errorf("%s\nkustomize builds this, but the program exited %d:\n%s", shown, got.ExitCode, tail(got.Stderr))
	}
	if exact {
		if got.Stdout != want.Stdout {
			return fmt.Errorf("%s\nexpected exactly (between the lines):\n-----\n%s-----\ngot:\n-----\n%s-----", shown, want.Stdout, got.Stdout)
		}
		return nil
	}
	return sameDocs(shown, got.Stdout, want.Stdout)
}

// sameDocs compares two YAML streams document by document.
func sameDocs(shown, got, want string) error {
	g, err := docs(got)
	if err != nil {
		return fmt.Errorf("%s\nthe output is not a YAML stream: %v\n%s", shown, err, tail(got))
	}
	e, err := docs(want)
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(e) || i < len(g); i++ {
		switch {
		case i >= len(g):
			return fmt.Errorf("%s\nexpected %d documents, got %d; the first missing one is:\n%s\nexpected the whole stream:\n%s", shown, len(e), len(g), indent(show(e[i])), indent(want))
		case i >= len(e):
			return fmt.Errorf("%s\nexpected %d documents, got %d; the first extra one is:\n%s", shown, len(e), len(g), indent(show(g[i])))
		case !reflect.DeepEqual(g[i], e[i]):
			return fmt.Errorf("%s\ndocument %d differs; expected:\n%s\ngot:\n%s\nexpected the whole stream:\n%s", shown, i+1, indent(show(e[i])), indent(show(g[i])), indent(want))
		}
	}
	return nil
}

// docs splits a stream on `---` lines and parses each non-empty document.
func docs(s string) ([]any, error) {
	var out []any
	for _, d := range strings.Split("\n"+s, "\n---\n") {
		var v any
		if err := yaml.Unmarshal([]byte(d), &v); err != nil {
			return nil, err
		}
		if v != nil {
			out = append(out, v)
		}
	}
	return out, nil
}

func show(v any) string {
	b, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// describe is a check as the learner needs to see it: the reason, then every
// file of the tree.
func describe(c check) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s — building %s of:\n", c.why, c.root)
	for _, p := range sortedKeys(c.t) {
		fmt.Fprintf(&b, "  ┌ %s\n", p)
		body := c.t[p]
		if !utf8.ValidString(body) {
			body = fmt.Sprintf("(%d bytes of binary)", len(body))
		}
		for _, l := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
			fmt.Fprintf(&b, "  │ %s\n", l)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func sortedKeys(t tree) []string {
	var ks []string
	for k := range t {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// firstLine is kustomize's error, its first line only, with the scratch
// directory hidden.
func firstLine(s, dir string) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, dir, "DIR"))
	for _, l := range strings.Split(s, "\n") {
		if !strings.HasPrefix(l, "# Warning") {
			return l
		}
	}
	return s
}

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n  ")
}

// tail trims captured output to something readable in a failure message.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "  (nothing)"
	}
	l := strings.Split(s, "\n")
	if len(l) > 40 {
		l = append([]string{"…"}, l[len(l)-40:]...)
	}
	return "  " + strings.Join(l, "\n  ")
}
