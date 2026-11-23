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
	register(Stage{Slug: "name-references", Run: stageNameReferences})
	register(Stage{Slug: "strategic-merge-patch", Run: stageStrategicMergePatch})
	register(Stage{Slug: "json6902-patch", Run: stageJSON6902Patch})
	register(Stage{Slug: "patch-targets", Run: stagePatchTargets})
	register(Stage{Slug: "overlays", Run: stageOverlays})
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

// consumers refer to config, credentials, a claim and a service account from
// every place a pod spec can, in three kinds of workload.
const consumers = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  template:
    spec:
      serviceAccountName: runner
      imagePullSecrets:
      - name: registry
      initContainers:
      - name: init
        image: busybox
        envFrom:
        - configMapRef:
            name: settings
      containers:
      - name: web
        image: nginx
        env:
        - name: MODE
          valueFrom:
            configMapKeyRef:
              name: settings
              key: mode
        - name: PASSWORD
          valueFrom:
            secretKeyRef:
              name: credentials
              key: password
        - name: EXTERNAL
          valueFrom:
            configMapKeyRef:
              name: not-ours
              key: x
        envFrom:
        - secretRef:
            name: credentials
        - configMapRef:
            name: settings
      volumes:
      - name: config
        configMap:
          name: settings
      - name: secret
        secret:
          secretName: credentials
      - name: data
        persistentVolumeClaim:
          claimName: data
      - name: both
        projected:
          sources:
          - configMap:
              name: settings
          - secret:
              name: credentials
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly
spec:
  schedule: "0 0 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: runner
          containers:
          - name: job
            image: busybox
            envFrom:
            - configMapRef:
                name: settings
---
apiVersion: v1
kind: Pod
metadata:
  name: solo
spec:
  containers:
  - name: c
    image: busybox
    envFrom:
    - secretRef:
        name: credentials
  volumes:
  - name: config
    configMap:
      name: settings
`

// providers are what consumers and the routing objects refer to.
const providers = `apiVersion: v1
kind: ServiceAccount
metadata:
  name: runner
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
---
apiVersion: v1
kind: Service
metadata:
  name: web
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: reader
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: reader
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: read-ns
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: reader
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: read-all
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: reader
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: web
spec:
  defaultBackend:
    service:
      name: web
      port:
        number: 80
  tls:
  - secretName: tls
    hosts: [shop.example.com]
  rules:
  - host: shop.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: web
            port:
              number: 80
      - path: /api
        pathType: Prefix
        backend:
          service:
            name: api
            port:
              number: 80
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: db
spec:
  serviceName: web
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: web
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web
  minReplicas: 1
  maxReplicas: 3
`

// stageNameReferences — when a name changes, every field kustomize knows to
// refer to that kind of object changes with it: generated hashes, prefixes
// and suffixes, through any number of kustomizations, but only within one
// namespace, and never a name that is not one of the build's own.
func stageNameReferences(ctx context.Context, _ *kube.Env, bin string) error {
	generators := `configMapGenerator:
- name: settings
  literals: [mode=fast]
secretGenerator:
- name: credentials
  literals: [password=s3cr3t]
- name: registry
  type: kubernetes.io/dockerconfigjson
  literals: ['.dockerconfigjson={}']
- name: tls
  type: kubernetes.io/tls
  literals: [tls.crt=c, tls.key=k]
`
	return runChecks(ctx, bin, []check{
		{"a generated ConfigMap or Secret is referred to by its hashed name from every volume, env, envFrom, projection and image pull secret of a pod spec, in any workload; not-ours is nobody's here and stays as it is", ".", tree{
			"kustomization.yaml": generators + "resources: [consumers.yaml]\n", "consumers.yaml": consumers,
		}},
		{"a prefix renames the Service an Ingress and a StatefulSet route to, the ServiceAccount a pod runs as, the claim it mounts, the Role or ClusterRole a binding grants — told apart by roleRef.kind — and the Deployment an autoscaler scales", ".", tree{
			"kustomization.yaml": "namePrefix: shop-\nnameSuffix: -v1\n" + generators + "resources: [consumers.yaml, providers.yaml]\n",
			"consumers.yaml":     consumers, "providers.yaml": providers,
		}},
		{"names change at every level and references follow all the way: the overlay's own Pod refers to the base's ConfigMap by its original name", "app", tree{
			"app/kustomization.yaml":  "namePrefix: prod-\nresources: [../base, pod.yaml]\n",
			"app/pod.yaml":            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: debug\nspec:\n  containers:\n  - name: c\n    image: busybox\n    envFrom:\n    - configMapRef:\n        name: settings\n",
			"base/kustomization.yaml": "namePrefix: base-\n" + generators + "resources: [consumers.yaml, providers.yaml]\n",
			"base/consumers.yaml":     consumers, "base/providers.yaml": providers,
		}},
		{"a reference only follows a rename in its own namespace: the Pod in b keeps settings, because the ConfigMap that was settings lives in a", "app", tree{
			"app/kustomization.yaml": "resources: [../a, ../b]\n",
			"a/kustomization.yaml":   "namespace: a\nnamePrefix: a-\nconfigMapGenerator:\n- name: settings\n  literals: [x=1]\n",
			"b/kustomization.yaml":   "namespace: b\nresources: [pod.yaml]\n",
			"b/pod.yaml":             "apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nspec:\n  containers:\n  - name: c\n    image: busybox\n    envFrom:\n    - configMapRef:\n        name: settings\n",
		}},
	})
}

// app is a Deployment, a Service and a custom resource with lists of every
// sort a strategic merge patch treats differently.
const app = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  labels:
    app: web
    tier: front
spec:
  replicas: 1
  template:
    spec:
      tolerations:
      - key: spot
        operator: Exists
      containers:
      - name: web
        image: nginx
        args: [--port, "80"]
        ports:
        - containerPort: 80
          name: http
        env:
        - name: A
          value: "1"
        - name: B
          value: "2"
        volumeMounts:
        - mountPath: /data
          name: data
      - name: sidecar
        image: envoy
      volumes:
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  ports:
  - port: 80
    targetPort: 8080
  - port: 81
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
spec:
  containers:
  - name: one
    size: 1
  - name: two
    size: 2
`

// stageStrategicMergePatch — a patch shaped like the resource it changes:
// maps merge, null deletes, lists the schema keys merge item by item with the
// patch's items first, other lists are replaced, and $patch directives delete
// or replace. Patches find their target by apiVersion, kind, name and
// namespace, and run before this kustomization's namespace and prefix.
func stageStrategicMergePatch(ctx context.Context, _ *kube.Env, bin string) error {
	merge := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  labels:
    tier: null
    team: shop
spec:
  template:
    spec:
      tolerations:
      - key: gpu
        operator: Exists
      containers:
      - name: init
        image: busybox
      - name: web
        args: [--port, "443"]
        ports:
        - containerPort: 443
        env:
        - name: B
          value: "22"
        - name: C
          value: "3"
        volumeMounts:
        - mountPath: /cache
          name: cache
      - name: sidecar
        $patch: delete
---
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  type: NodePort
  ports:
  - port: 443
    name: https
  - port: 80
    targetPort: 9090
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
spec:
  containers:
  - name: two
    size: 22
`
	settings := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\ndata:\n  mode: slow\n  extra: \"yes\"\n"
	return runChecks(ctx, bin, []check{
		{"one patch file holds three patches: labels merge and tier is deleted; containers, env, ports and volume mounts merge by their keys with the patch's items first and the rest after; sidecar is deleted; tolerations and args are replaced whole; a Service's ports merge by port; a custom resource's list is replaced even when it is called containers, since kustomize knows no schema for it", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- path: merge.yaml\n", "app.yaml": app, "merge.yaml": merge,
		}},
		{"an inline patch: $patch: replace swaps a whole map and a whole keyed list, and replicas: null removes the field", ".", tree{
			"kustomization.yaml": `resources: [app.yaml]
patches:
- patch: |-
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: web
      labels:
        $patch: replace
        only: me
    spec:
      replicas: null
      template:
        spec:
          volumes:
          - $patch: replace
          - name: scratch
            emptyDir: {}
`, "app.yaml": app,
		}},
		{"$patch: delete at the top of a patch deletes the whole resource", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- patch: |-\n    apiVersion: v1\n    kind: Service\n    metadata:\n      name: web\n    $patch: delete\n", "app.yaml": app,
		}},
		{"patches run before this kustomization's namespace and prefix, so they name a resource as it arrives; a generated ConfigMap can be patched too, and its hash is of the patched data", ".", tree{
			"kustomization.yaml": "namespace: prod\nnamePrefix: p-\nresources: [app.yaml]\nconfigMapGenerator:\n- name: settings\n  literals: [mode=fast]\npatches:\n- path: settings.yaml\n- patch: |-\n    apiVersion: apps/v1\n    kind: Deployment\n    metadata:\n      name: web\n    spec:\n      replicas: 3\n",
			"app.yaml":           app, "settings.yaml": settings,
		}},
		{"a patch in an overlay can name a base's resource by the name it had before the base prefixed it, or by the name it has now", "overlay", tree{
			"base/kustomization.yaml":    "namePrefix: base-\nresources: [app.yaml]\n",
			"base/app.yaml":              app,
			"overlay/kustomization.yaml": "resources: [../base]\npatches:\n- patch: |-\n    apiVersion: apps/v1\n    kind: Deployment\n    metadata:\n      name: web\n    spec:\n      replicas: 4\n- patch: |-\n    apiVersion: v1\n    kind: Service\n    metadata:\n      name: base-web\n    spec:\n      type: LoadBalancer\n",
		}},
		{"a patch's namespace is part of what it matches: two ConfigMaps called settings, and the patch is for the one in b", ".", tree{
			"kustomization.yaml": "resources: [a.yaml, b.yaml]\npatches:\n- patch: |-\n    apiVersion: v1\n    kind: ConfigMap\n    metadata:\n      name: settings\n      namespace: b\n    data:\n      mode: slow\n",
			"a.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  namespace: a\ndata:\n  mode: fast\n",
			"b.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  namespace: b\ndata:\n  mode: fast\n",
		}},
		{"patchesStrategicMerge, the older field, takes file names and inline patches alike, and runs before patches", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatchesStrategicMerge:\n- svc.yaml\n- |-\n  apiVersion: apps/v1\n  kind: Deployment\n  metadata:\n    name: web\n  spec:\n    replicas: 5\npatches:\n- patch: |-\n    apiVersion: apps/v1\n    kind: Deployment\n    metadata:\n      name: web\n    spec:\n      replicas: 6\n",
			"app.yaml":           app, "svc.yaml": "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\nspec:\n  type: NodePort\n",
		}},
		{"no resource is a Deployment called api", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- patch: |-\n    apiVersion: apps/v1\n    kind: Deployment\n    metadata:\n      name: api\n    spec:\n      replicas: 3\n", "app.yaml": app,
		}},
		{"apiVersion is part of the match too: there is no apps/v1beta1 Deployment here", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- patch: |-\n    apiVersion: apps/v1beta1\n    kind: Deployment\n    metadata:\n      name: web\n    spec:\n      replicas: 3\n", "app.yaml": app,
		}},
		{"a patch without a namespace matches only a resource without one", ".", tree{
			"kustomization.yaml": "resources: [b.yaml]\npatches:\n- patch: |-\n    apiVersion: v1\n    kind: ConfigMap\n    metadata:\n      name: settings\n    data:\n      mode: slow\n",
			"b.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  namespace: b\n",
		}},
		{"a patch that names its target nowhere: no kind", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- patch: |-\n    metadata:\n      name: web\n    spec:\n      replicas: 3\n", "app.yaml": app,
		}},
		{"a patch entry with both a path and an inline patch", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- path: settings.yaml\n  patch: |-\n    kind: ConfigMap\n", "app.yaml": app, "settings.yaml": settings,
		}},
		{"a patch entry with neither", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- {}\n", "app.yaml": app,
		}},
	})
}

// stageJSON6902Patch — a JSON patch is a list of RFC 6902 operations on paths
// into the resource, applied to the resources its target names; a step that
// cannot be taken is an error. patchesJson6902, the older field, runs later,
// after labels and annotations.
func stageJSON6902Patch(ctx context.Context, _ *kube.Env, bin string) error {
	ops := `- op: add
  path: /spec/template/spec/containers/0/env/-
  value:
    name: Z
    value: "26"
- op: add
  path: /spec/template/spec/containers/0/args/0
  value: --verbose
- op: replace
  path: /spec/replicas
  value: 3
- op: remove
  path: /metadata/labels/tier
- op: copy
  from: /spec/template/spec/containers/0/image
  path: /spec/template/spec/containers/1/image
- op: move
  from: /spec/template/spec/tolerations
  path: /spec/template/spec/preemptedTolerations
- op: add
  path: /metadata/annotations
  value:
    example.com/owner: shop
- op: add
  path: /metadata/annotations/example.com~1path
  value: a/b
- op: replace
  path: /spec/template/spec/containers/0/env/1
  value:
    name: B
    value: "two"
- op: remove
  path: /spec/template/spec/containers/0/ports/0
- op: copy
  from: /metadata/labels
  path: /spec/template/metadata
- op: add
  path: /spec/template/metadata/copied
  value: "only here"
- op: test
  path: /spec/replicas
  value: 3
`
	patch := func(target, body string) string {
		return "resources: [app.yaml]\npatches:\n- target:\n    " + strings.ReplaceAll(target, "\n", "\n    ") + "\n  patch: |-\n    " + strings.ReplaceAll(strings.TrimRight(body, "\n"), "\n", "\n    ") + "\n"
	}
	return runChecks(ctx, bin, []check{
		{"add (into a map, at a list index and at the end with -), remove, replace, copy, move and test, in order; ~1 in a path is a slash, and a copy is a copy, not the same map twice", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- path: ops.yaml\n  target:\n    kind: Deployment\n    name: web\n", "app.yaml": app, "ops.yaml": ops,
		}},
		{"the same operations written as JSON, and a target by group, version and kind", ".", tree{
			"kustomization.yaml": patch("group: \"\"\nversion: v1\nkind: Service", `[{"op": "add", "path": "/spec/ports/1/name", "value": "metrics"}, {"op": "replace", "path": "/spec/ports/0/port", "value": 8080}]`), "app.yaml": app,
		}},
		{"a target that names nothing in the build is not an error; the patch just applies nowhere", ".", tree{
			"kustomization.yaml": patch("kind: Deployment\nname: api", `[{"op": "remove", "path": "/spec"}]`), "app.yaml": app,
		}},
		{"patches run before the prefix, so the target is the name as written; patchesJson6902 runs after the prefix and labels, and finds the resource by its first name or its current one", ".", tree{
			"kustomization.yaml": "namePrefix: p-\nlabels:\n- pairs:\n    team: shop\nresources: [app.yaml]\npatches:\n- target:\n    kind: Service\n    name: web\n  patch: '[{\"op\": \"add\", \"path\": \"/spec/type\", \"value\": \"NodePort\"}]'\npatchesJson6902:\n- target:\n    group: apps\n    version: v1\n    kind: Deployment\n    name: web\n  path: labels.yaml\n- target:\n    version: v1\n    kind: Service\n    name: p-web\n  patch: '[{\"op\": \"add\", \"path\": \"/metadata/labels/seen\", \"value\": \"yes\"}]'\n",
			"app.yaml":           app, "labels.yaml": "- op: replace\n  path: /metadata/labels/team\n  value: other\n",
		}},
		{"a target's namespace is matched too, and a resource with none is in default", ".", tree{
			"kustomization.yaml": patch("namespace: default", `[{"op": "add", "path": "/metadata/annotations", "value": {"seen": "yes"}}]`) + "- target:\n    namespace: b\n  patch: '[{\"op\": \"add\", \"path\": \"/data\", \"value\": {\"in\": \"b\"}}]'\n",
			"app.yaml":           "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: plain\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: in-b\n  namespace: b\n",
		}},
		{"remove a path that is not there", ".", tree{
			"kustomization.yaml": patch("kind: Deployment", "- op: remove\n  path: /spec/paused\n"), "app.yaml": app,
		}},
		{"replace a key that is not there, which kustomize lets through as an add", ".", tree{
			"kustomization.yaml": patch("kind: Deployment", "- op: replace\n  path: /spec/paused\n  value: true\n"), "app.yaml": app,
		}},
		{"replace a list item that is not there, which it does not", ".", tree{
			"kustomization.yaml": patch("kind: Deployment", "- op: replace\n  path: /spec/template/spec/containers/7\n  value: {name: x}\n"), "app.yaml": app,
		}},
		{"add past the end of a list", ".", tree{
			"kustomization.yaml": patch("kind: Deployment", "- op: add\n  path: /spec/template/spec/containers/5\n  value: {name: x}\n"), "app.yaml": app,
		}},
		{"add under a parent that does not exist", ".", tree{
			"kustomization.yaml": patch("kind: Deployment", "- op: add\n  path: /spec/strategy/type\n  value: Recreate\n"), "app.yaml": app,
		}},
		{"a test that fails stops the build", ".", tree{
			"kustomization.yaml": patch("kind: Deployment", "- op: test\n  path: /spec/replicas\n  value: 9\n"), "app.yaml": app,
		}},
		{"a JSON patch has no name of its own to match, so it needs a target", ".", tree{
			"kustomization.yaml": "resources: [app.yaml]\npatches:\n- patch: '[{\"op\": \"remove\", \"path\": \"/spec\"}]'\n", "app.yaml": app,
		}},
	})
}

// fleet is several resources sharing names, kinds and labels in different
// combinations, for telling target selectors apart.
const fleet = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  labels:
    tier: front
  annotations:
    team: shop
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-canary
  labels:
    tier: front
    canary: "true"
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: db
  labels:
    tier: back
  annotations:
    team: data
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: web
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: jobs
  namespace: batch
---
apiVersion: example.com/v1beta1
kind: Deployment
metadata:
  name: web
`

// stagePatchTargets — a target is a set of filters: anchored regular
// expressions on group, version, kind, name and namespace, and label and
// annotation selectors. One patch, strategic or JSON, applies to every
// resource the target selects.
func stagePatchTargets(ctx context.Context, _ *kube.Env, bin string) error {
	mark := func(target, key string) string {
		return "- target:\n    " + strings.ReplaceAll(target, "\n", "\n    ") + "\n  patch: '[{\"op\": \"add\", \"path\": \"/metadata/annotations/" + key + "\", \"value\": \"hit\"}]'\n"
	}
	return runChecks(ctx, bin, []check{
		{"each filter is a regular expression matched against the whole field: web is not web-canary, Deploy is not Deployment, but web.* and Deployment|StatefulSet are what they look like; version v1 matches apps/v1 and v1 alike", ".", tree{
			"kustomization.yaml": "resources: [fleet.yaml]\npatches:\n" +
				mark("name: web", "exact") + mark("name: web.*\ngroup: apps", "prefix") + mark("kind: Deploy", "partial") +
				mark("kind: Deployment|StatefulSet", "either") + mark("version: v1", "v1") + mark("group: example.com", "group") +
				mark("namespace: bat.*", "ns"),
			"fleet.yaml": fleet,
		}},
		{"labelSelector and annotationSelector take Kubernetes selector syntax: =, ==, !=, in, notin, a bare key and !key, joined by commas", ".", tree{
			"kustomization.yaml": "resources: [fleet.yaml]\npatches:\n" +
				mark("labelSelector: tier=front", "eq") + mark("labelSelector: tier==front,canary!=true", "ne") +
				mark("labelSelector: tier in (front, back)", "in") + mark("labelSelector: tier notin (front)", "notin") +
				mark("labelSelector: canary", "has") + mark("labelSelector: '!tier'", "hasnot") +
				mark("annotationSelector: team=data", "ann") + mark("kind: Deployment\nlabelSelector: tier", "both"),
			"fleet.yaml": fleet,
		}},
		{"a strategic merge patch with a target applies to every resource it selects; the patch's own kind and name are ignored, and each resource keeps its own", ".", tree{
			"kustomization.yaml": "resources: [fleet.yaml]\npatches:\n- target:\n    group: apps\n    labelSelector: tier\n  patch: |-\n    kind: Anything\n    metadata:\n      name: anything\n      labels:\n        patched: \"yes\"\n    spec:\n      replicas: 2\n",
			"fleet.yaml":         fleet,
		}},
		{"$patch: delete with a target deletes every resource selected", ".", tree{
			"kustomization.yaml": "resources: [fleet.yaml]\npatches:\n- target:\n    name: web\n  patch: |-\n    kind: Gone\n    metadata:\n      name: gone\n    $patch: delete\n",
			"fleet.yaml":         fleet,
		}},
		{"a target can name a base's resource by its name before the base's prefix", "overlay", tree{
			"base/kustomization.yaml":    "namePrefix: base-\nresources: [fleet.yaml]\n",
			"base/fleet.yaml":            fleet,
			"overlay/kustomization.yaml": "resources: [../base]\npatches:\n" + mark("name: db", "first") + mark("name: base-db", "current"),
		}},
		{"one patches entry with a target may hold one strategic merge patch, not two", ".", tree{
			"kustomization.yaml": "resources: [fleet.yaml]\npatches:\n- target:\n    kind: StatefulSet\n  patch: |-\n    kind: A\n    metadata:\n      name: a\n    ---\n    kind: B\n    metadata:\n      name: b\n",
			"fleet.yaml":         fleet,
		}},
		{"a label selector that does not parse", ".", tree{
			"kustomization.yaml": "resources: [fleet.yaml]\npatches:\n" + mark("labelSelector: tier in front", "bad"),
			"fleet.yaml":         fleet,
		}},
	})
}

// shopBase is a small application as a base: a Deployment wired to a
// generated ConfigMap and Secret, and the Service in front of it.
var shopBase = tree{
	"base/kustomization.yaml": `labels:
- pairs:
    app: shop
  includeSelectors: true
resources: [deployment.yaml, service.yaml]
configMapGenerator:
- name: settings
  literals: [mode=fast, level=1]
  options:
    labels:
      from: base
secretGenerator:
- name: credentials
  literals: [user=shop]
`,
	"base/deployment.yaml": `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: web
        image: shop/web:1.0
        envFrom:
        - configMapRef:
            name: settings
        - secretRef:
            name: credentials
`,
	"base/service.yaml": `apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  ports:
  - port: 80
`,
}

// stageOverlays — an overlay is a kustomization whose resources include a
// base. Everything the overlay does applies on top of the base's finished
// output, and a generator can merge into or replace what the base generated.
func stageOverlays(ctx context.Context, _ *kube.Env, bin string) error {
	prod := `namespace: prod
namePrefix: prod-
labels:
- pairs:
    env: prod
resources: [../base]
replicas:
- name: web
  count: 3
images:
- name: shop/web
  newTag: "2.0"
patches:
- patch: |-
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: web
    spec:
      template:
        spec:
          containers:
          - name: web
            resources:
              limits:
                memory: 512Mi
configMapGenerator:
- name: settings
  behavior: merge
  literals: [level=3, region=eu]
  options:
    labels:
      from: prod
    annotations:
      tuned: "true"
`
	return runChecks(ctx, bin, []check{
		{"prod overlays the base: its namespace, prefix, labels, replica count, image tag and patch all land on the base's resources; its ConfigMap generator merges into the base's settings, and the Deployment follows the merged ConfigMap's new hash", "prod", shopBase.with(tree{
			"prod/kustomization.yaml": prod,
		})},
		{"behavior: replace swaps the generated data whole but keeps the base's labels; a Secret generator merges, and its type is the overlay's", "dev", shopBase.with(tree{
			"dev/kustomization.yaml": "resources: [../base]\nconfigMapGenerator:\n- name: settings\n  behavior: replace\n  literals: [debug=true]\nsecretGenerator:\n- name: credentials\n  behavior: merge\n  type: kubernetes.io/basic-auth\n  literals: [password=dev]\n",
		})},
		{"bases, the older field, lists bases just as resources does", "dev", shopBase.with(tree{
			"dev/kustomization.yaml": "bases: [../base]\nnameSuffix: -dev\n",
		})},
		{"overlays stack: top builds on mid, which builds on base; prefixes compose, and a generator two levels up still finds settings by the name the base gave it", "top", shopBase.with(tree{
			"mid/kustomization.yaml": "namePrefix: mid-\nresources: [../base]\ncommonAnnotations:\n  layer: mid\n",
			"top/kustomization.yaml": "namePrefix: top-\nresources: [../mid]\nconfigMapGenerator:\n- name: settings\n  behavior: merge\n  literals: [layer=top]\n",
		})},
		{"one kustomization can hold two overlays of the same base, each built separately, so long as they end up with different names or namespaces", "all", shopBase.with(tree{
			"prod/kustomization.yaml": prod,
			"dev/kustomization.yaml":  "namespace: dev\nresources: [../base]\n",
			"all/kustomization.yaml":  "resources: [../prod, ../dev]\n",
		})},
		{"two copies of the base with nothing to tell them apart collide", "all", shopBase.with(tree{
			"a/kustomization.yaml":   "resources: [../base]\n",
			"b/kustomization.yaml":   "resources: [../base]\n",
			"all/kustomization.yaml": "resources: [../a, ../b]\n",
		})},
		{"a generator without behavior creates, and settings already exists in the base — even though mid has since renamed it mid-settings", "dev", shopBase.with(tree{
			"mid/kustomization.yaml": "namePrefix: mid-\nresources: [../base]\n",
			"dev/kustomization.yaml": "resources: [../mid]\nconfigMapGenerator:\n- name: settings\n  literals: [level=2]\n",
		})},
		{"merge needs something to merge into, and the base generated no ConfigMap called flags", "dev", shopBase.with(tree{
			"dev/kustomization.yaml": "resources: [../base]\nconfigMapGenerator:\n- name: flags\n  behavior: merge\n  literals: [level=2]\n",
		})},
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
	// A stage whose every tree kustomize refuses grades nothing but exit codes,
	// which is how a typo in a test's own YAML would pass silently.
	if w.built == 0 {
		panic("kustomize refused every tree of this stage")
	}
	return nil
}

// work is one stage's scratch space.
type work struct {
	ctx   context.Context
	bin   string
	dir   string
	built int // trees kustomize built rather than refused
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
	w.built++
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
