// Package helm holds the assertions for the "Build your own Helm" course —
// one function per stage, registered by slug.
//
// Every stage writes a chart to a temporary directory, runs the learner's
// built binary on it, and judges exit code, stdout and, from install onwards,
// what the cluster holds. Flags always come before positional arguments, as
// both Go's flag package and the real helm accept.
package helm

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/madhank93/byo-k8s-x/internal/cluster"
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
	register(Stage{Slug: "chart-load", Run: stageChartLoad})
	register(Stage{Slug: "values-merge", Run: stageValuesMerge})
	register(Stage{Slug: "template-engine", Run: stageTemplateEngine})
	register(Stage{Slug: "builtin-objects", Run: stageBuiltinObjects})
	register(Stage{Slug: "template-functions", Run: stageTemplateFunctions})
	register(Stage{Slug: "named-templates", Run: stageNamedTemplates})
	register(Stage{Slug: "subcharts", Run: stageSubcharts})
	register(Stage{Slug: "conditions", Run: stageConditions})
	register(Stage{Slug: "values-schema", Run: stageValuesSchema})
	register(Stage{Slug: "manifest-split", Run: stageManifestSplit})
	register(Stage{Slug: "install", Run: stageInstall})
	register(Stage{Slug: "release-storage", Run: stageReleaseStorage})
	register(Stage{Slug: "list-status", Run: stageListStatus})
	register(Stage{Slug: "history", Run: stageHistory})
	register(Stage{Slug: "upgrade-3way-merge", Run: stageUpgrade})
	register(Stage{Slug: "rollback", Run: stageRollback})
	register(Stage{Slug: "hooks", Run: stageHooks})
	register(Stage{Slug: "hook-weights", Run: stageHookWeights})
	register(Stage{Slug: "test", Run: stageTest})
}

const chartYAML = `apiVersion: v2
name: demo
version: 1.2.3
appVersion: "4.5"
description: a chart the grader wrote
`

const configMapTemplate = `apiVersion: v1
kind: ConfigMap
metadata:
  name: demo
`

// stageChartLoad — a chart directory is read, Chart.yaml is validated the way
// Helm validates it, and every file under templates/ is listed, nested ones
// included, while files elsewhere in the chart are not.
func stageChartLoad(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	good := w.chart("demo", map[string]string{
		"Chart.yaml":                chartYAML,
		"values.yaml":               "replicas: 1\n",
		"templates/configmap.yaml":  configMapTemplate,
		"templates/_helpers.tpl":    `{{- define "demo.name" -}}demo{{- end -}}` + "\n",
		"templates/NOTES.txt":       "Installed.\n",
		"templates/sub/deeper.yaml": configMapTemplate,
		"templates/a-b.yaml":        configMapTemplate,
		"templates/a/x.yaml":        configMapTemplate,
		"templates/.hidden.yaml":    configMapTemplate,
		"files/app.conf":            "key = value\n",
		"README.md":                 "# demo\n",
	})
	res, err := w.run("chart", good)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	want := []string{
		"demo 1.2.3",
		"templates/NOTES.txt",
		"templates/_helpers.tpl",
		"templates/a-b.yaml",
		"templates/a/x.yaml",
		"templates/configmap.yaml",
		"templates/sub/deeper.yaml",
	}
	if err := wantLines(res.Stdout, want); err != nil {
		return fmt.Errorf("templates sort by whole path (a-b.yaml before a/x.yaml, since - sorts before /), and Helm never loads templates/.hidden.yaml: %w", err)
	}

	// Helm 2 charts had no apiVersion, and Helm 3 still loads them as v1.
	legacy := w.chart("legacy", map[string]string{
		"Chart.yaml":               "name: legacy\nversion: 0.1.0\n",
		"templates/configmap.yaml": configMapTemplate,
	})
	res, err = w.run("chart", legacy)
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("a Chart.yaml with no apiVersion is a Helm 2 chart, which Helm still loads, but the program exited %d:\n%s",
			res.ExitCode, tail(res.Stderr))
	}
	if err := wantLines(res.Stdout, []string{"legacy 0.1.0", "templates/configmap.yaml"}); err != nil {
		return err
	}

	// Helm's semver parser is lenient: a leading v and a missing patch are fine.
	lenient := w.chart("lenient", map[string]string{
		"Chart.yaml":               "apiVersion: v2\nname: lenient\nversion: v1.2\ntype: application\n",
		"templates/configmap.yaml": configMapTemplate,
	})
	if res, err = w.run("chart", lenient); err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("version v1.2 is a version Helm accepts, and type application is valid, but the program exited %d:\n%s",
			res.ExitCode, tail(res.Stderr))
	}
	if err := wantLines(res.Stdout, []string{"lenient v1.2", "templates/configmap.yaml"}); err != nil {
		return err
	}

	for _, bad := range []struct{ name, chartYAML, why string }{
		{"no-chart-yaml", "", "there is no Chart.yaml"},
		{"no-name", "apiVersion: v2\nversion: 1.0.0\n", "Chart.yaml has no name"},
		{"slashed-name", "apiVersion: v2\nname: a/b\nversion: 1.0.0\n", "the name a/b is a path, not a chart name"},
		{"no-version", "apiVersion: v2\nname: a\n", "Chart.yaml has no version"},
		{"bad-version", "apiVersion: v2\nname: a\nversion: banana\n", `the version "banana" is not semantic versioning`},
		{"bad-type", "apiVersion: v2\nname: a\nversion: 1.0.0\ntype: plugin\n", "type must be application or library"},
		{"bad-yaml", "apiVersion: v2\nname: [a\n", "Chart.yaml is not valid YAML"},
	} {
		files := map[string]string{"templates/configmap.yaml": configMapTemplate}
		if bad.chartYAML != "" {
			files["Chart.yaml"] = bad.chartYAML
		}
		res, err := w.run("chart", w.chart(bad.name, files))
		if err != nil {
			return err
		}
		if err := wantError(res); err != nil {
			return fmt.Errorf("%s: %s, so this is not a chart Helm would load: %w", bad.name, bad.why, err)
		}
	}
	return nil
}

// stageValuesMerge — chart defaults, then -f files in order, then --set in
// order, merged map by map: lists are replaced whole, null deletes a default,
// and --set wins over every file wherever it appears on the command line.
func stageValuesMerge(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	dir := w.chart("demo", map[string]string{
		"Chart.yaml": chartYAML,
		"values.yaml": `replicas: 1
image:
  repository: nginx
  tag: "1.25"
  pullPolicy: IfNotPresent
service:
  type: ClusterIP
  port: 80
labels:
  team: web
ports: [80, 443]
debug: false
`,
		"templates/configmap.yaml": configMapTemplate,
	})
	prod := w.file("prod.yaml", `replicas: 3
image:
  tag: "1.27"
labels: null
ports: [8080]
service:
  port: 8443
`)
	last := w.file("last.yaml", "service:\n  port: 8080\nimage:\n  repository: mirror/nginx\n")

	res, err := w.run("values",
		"--set", "replicas=7",
		"-f", prod, "-f", last,
		"--set", "image.pullPolicy=Always,debug=true",
		"--set", "extra.zip=007",
		"--set", "extra.gone=null",
		"--set", "service.type=null",
		"--set", "count=10",
		dir)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantYAML(res.Stdout, `count: 10
debug: true
extra:
  gone: null
  zip: "007"
image:
  pullPolicy: Always
  repository: mirror/nginx
  tag: "1.27"
ports: [8080]
replicas: 7
service:
  port: 8080
`, `the values, merged in Helm's order:
  values.yaml < -f prod.yaml < -f last.yaml < every --set, in order
where a map merges key by key, a list is replaced whole, null deletes the
default (and stays a null where there is no default, as extra.gone does), "007"
stays a string because of its leading zero, and 10 and true
are a number and a boolean`); err != nil {
		return err
	}

	// Files merge with each other before any default is deleted, so a later
	// file can put back under a key that an earlier one nulled.
	twoPass := w.chart("two-pass", map[string]string{
		"Chart.yaml":               "apiVersion: v2\nname: two-pass\nversion: 0.1.0\n",
		"values.yaml":              "labels:\n  team: web\nport: 80\n",
		"templates/configmap.yaml": configMapTemplate,
	})
	res, err = w.run("values", "-f", w.file("nolabels.yaml", "labels: null\n"), "-f", w.file("xlabel.yaml", "labels:\n  x: \"1\"\n"), twoPass)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantYAML(res.Stdout, "labels: {team: web, x: \"1\"}\nport: 80\n", `defaults labels: {team: web}, then -f nolabels.yaml (labels: null), then
-f xlabel.yaml (labels: {x: "1"}): the user's files merge first, the later map
replacing the null, and only then meet the defaults`); err != nil {
		return err
	}

	bare := w.chart("bare", map[string]string{
		"Chart.yaml":               "apiVersion: v2\nname: bare\nversion: 0.1.0\n",
		"templates/configmap.yaml": configMapTemplate,
	})
	res, err = w.run("values", "--set", "a.b.c=x", bare)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("a chart with no values.yaml has no defaults, which is allowed: %w", err)
	}
	if err := wantYAML(res.Stdout, "a: {b: {c: x}}\n", "a chart with no values.yaml, given --set a.b.c=x"); err != nil {
		return err
	}

	for _, bad := range []struct {
		args []string
		why  string
	}{
		{[]string{"--set", "novalue", dir}, `--set novalue has no "=", so it sets nothing`},
		{[]string{"-f", filepath.Join(w.dir, "missing.yaml"), dir}, "the -f file does not exist"},
		{[]string{"-f", w.file("broken.yaml", "a: [b\n"), dir}, "the -f file is not valid YAML"},
	} {
		res, err := w.run(append([]string{"values"}, bad.args...)...)
		if err != nil {
			return err
		}
		if err := wantError(res); err != nil {
			return fmt.Errorf("%s: %w", bad.why, err)
		}
	}
	return nil
}

// renderChart is a chart whose templates exercise text/template alone: values,
// a condition, a range, a missing value, a nested file, a partial and notes.
func renderChart(w *work) string {
	return w.chart("demo", map[string]string{
		"Chart.yaml":  chartYAML,
		"values.yaml": "greeting: hello\nenabled: false\nitems: [a, b]\n",
		"templates/a-greeting.yaml": `
apiVersion: v1
kind: ConfigMap
metadata:
  name: greeting
data:
  greeting: {{ .Values.greeting }}
  missing: "{{ .Values.nothing }}"

`,
		"templates/b-optional.yaml": `{{- if .Values.enabled }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: optional
{{- end }}
`,
		"templates/c-list.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: items
data:
{{- range $i, $v := .Values.items }}
  item{{ $i }}: {{ $v }}
{{- end }}
`,
		"templates/c/e.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: e
`,
		"templates/sub/d.yaml":      configMapTemplate,
		"templates/_helpers.tpl":    "{{/* a partial: never output, even text outside a define */}}\nnot a manifest\n",
		"templates/NOTES.txt":       "Thanks {{ .Values.greeting }}\n",
		"templates/sub/MYNOTES.txt": "Also notes, by the name's ending: {{ .Values.greeting }}\n",
	})
}

// stageTemplateEngine — every template is rendered with the merged values and
// printed the way helm template prints it: one "# Source:" document per file,
// trimmed, in whole-path order, with partials, notes and empty output left out.
func stageTemplateEngine(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	dir := renderChart(w)

	greeting := func(g string) string {
		return `---
# Source: demo/templates/a-greeting.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: greeting
data:
  greeting: ` + g + `
  missing: ""
`
	}
	const optional = `---
# Source: demo/templates/b-optional.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: optional
`
	const rest = `---
# Source: demo/templates/c-list.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: items
data:
  item0: a
  item1: b
---
# Source: demo/templates/c/e.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: e
---
# Source: demo/templates/sub/d.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: demo
`
	res, err := w.run("template", "demo", dir)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantExact(res.Stdout, greeting("hello")+rest, "the chart's defaults"); err != nil {
		return err
	}

	res, err = w.run("template", "-f", w.file("hi.yaml", "greeting: hi\n"), "--set", "enabled=true", "demo", dir)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantExact(res.Stdout, greeting("hi")+optional+rest, "-f hi.yaml (greeting: hi) --set enabled=true"); err != nil {
		return err
	}

	for _, bad := range []struct{ file, body, why string }{
		{"templates/unclosed.yaml", "{{ .Values.greeting \n", "it does not parse"},
		{"templates/deeper.yaml", "{{ .Values.greeting.deep }}\n", "it reads a field of a string"},
		{"templates/NOTES.txt", "{{ .Values.greeting.deep }}\n", "NOTES.txt renders like any template, and it reads a field of a string"},
	} {
		dir := w.chart("bad-"+filepath.Base(bad.file), map[string]string{
			"Chart.yaml": chartYAML, "values.yaml": "greeting: hello\n", bad.file: bad.body,
		})
		res, err := w.run("template", "demo", dir)
		if err != nil {
			return err
		}
		if res.ExitCode == 0 {
			return fmt.Errorf("%s cannot render because %s, but the program exited 0 with:\n%s", bad.file, bad.why, tail(res.Stdout))
		}
		if !strings.Contains(res.Stderr, bad.file) {
			return fmt.Errorf("%s cannot render because %s; the error should name the file, but stderr was:\n%s", bad.file, bad.why, tail(res.Stderr))
		}
	}
	return nil
}

// stageBuiltinObjects — templates see .Release, .Chart, .Template and .Files
// with the values helm template gives them, .Template differing per file.
func stageBuiltinObjects(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	const body = `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-{{ .Chart.Name }}
  namespace: {{ .Release.Namespace }}
data:
  revision: "{{ .Release.Revision }}"
  isInstall: "{{ .Release.IsInstall }}"
  isUpgrade: "{{ .Release.IsUpgrade }}"
  service: {{ .Release.Service }}
  chart: {{ .Chart.Name }}-{{ .Chart.Version }}
  appVersion: "{{ .Chart.AppVersion }}"
  description: "{{ .Chart.Description }}"
  template: {{ .Template.Name }}
  basePath: {{ .Template.BasePath }}
  conf: "{{ .Files.Get "files/app.conf" }}"
  missing: "{{ .Files.Get "files/nope" }}"
  chartYaml: "{{ .Files.Get "Chart.yaml" }}"
  valuesYaml: "{{ .Files.Get "values.yaml" }}"
  replicas: "{{ .Values.replicas }}"
  home: {{ .Chart.Home }}
  keyword: {{ index .Chart.Keywords 1 }}
  maintainer: {{ (index .Chart.Maintainers 0).Email }}
  schema: "{{ .Files.Get "values.schema.json" }}"
  lock: "{{ .Files.Get "Chart.lock" }}"
  requirements: "{{ .Files.Get "requirements.yaml" }}"
`
	dir := w.chart("demo", map[string]string{
		"Chart.yaml":              chartYAML + "home: https://example.com\nkeywords: [web, demo]\nmaintainers:\n- name: ann\n  email: ann@example.com\n",
		"values.schema.json":      "{}\n",
		"Chart.lock":              "dependencies: []\n",
		"requirements.yaml":       "dependencies: []\n",
		"values.yaml":             "replicas: 2\n",
		"files/app.conf":          "key = value",
		"templates/builtins.yaml": body,
		"templates/sub/where.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: where
data:
  template: {{ .Template.Name }}
`,
	})

	want := func(release, ns, replicas string) string {
		return `---
# Source: demo/templates/builtins.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ` + release + `-demo
  namespace: ` + ns + `
data:
  revision: "1"
  isInstall: "true"
  isUpgrade: "false"
  service: Helm
  chart: demo-1.2.3
  appVersion: "4.5"
  description: "a chart the grader wrote"
  template: demo/templates/builtins.yaml
  basePath: demo/templates
  conf: "key = value"
  missing: ""
  chartYaml: ""
  valuesYaml: ""
  replicas: "` + replicas + `"
  home: https://example.com
  keyword: demo
  maintainer: ann@example.com
  schema: ""
  lock: ""
  requirements: ""
---
# Source: demo/templates/sub/where.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: where
data:
  template: demo/templates/sub/where.yaml
`
	}
	three := w.file("three.yaml", "replicas: 3\n")
	for _, c := range []struct {
		flag, release, ns, replicas string
		files                       []string
	}{
		{"-n", "web", "team-a", "2", nil},
		{"--namespace", "api", "team-b", "3", []string{"-f", three}},
	} {
		args := append(append([]string{"template"}, c.files...), c.flag, c.ns, c.release, dir)
		res, err := w.run(args...)
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return err
		}
		if err := wantExact(res.Stdout, want(c.release, c.ns, c.replicas), fmt.Sprintf("release %s in namespace %s (%s %s)", c.release, c.ns, strings.Join(c.files, " "), c.flag)); err != nil {
			return err
		}
	}
	return nil
}

// stageTemplateFunctions — Helm's function map: sprig's functions without env
// and expandenv, plus toYaml, toJson and required, rendering exactly as Helm's.
func stageTemplateFunctions(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	files := map[string]string{
		"Chart.yaml": chartYAML,
		"values.yaml": `name: Payments-Gateway-Primary-Region-Canary-Release-For-Load-Tester-Pool
port: 8080
labels:
  tier: web
  team: core
resources:
  limits: {cpu: 100m, memory: 64Mi}
  requests: {cpu: 50m}
tags: [beta, alpha]
password: s3cret
image:
  tag: ""
`,
		"templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Values.name | lower | trunc 63 | trimSuffix "-" }}
  labels:
    {{- toYaml .Values.labels | nindent 4 }}
data:
  port: {{ .Values.port | quote }}
  region: {{ .Values.region | default "us-east-1" | quote }}
  emptyTag: {{ .Values.image.tag | default "latest" | quote }}
  shout: {{ upper "quiet" }}
  tags: {{ .Values.tags | sortAlpha | join "," | quote }}
  password: {{ .Values.password | b64enc | quote }}
  json: {{ toJson .Values.labels | squote }}
  sum: {{ add 1 2 | quote }}
  resources: |
{{ toYaml .Values.resources | indent 4 }}
`,
	}
	res, err := w.run("template", "demo", w.chart("demo", files))
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantExact(res.Stdout, `---
# Source: demo/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: payments-gateway-primary-region-canary-release-for-load-tester
  labels:
    team: core
    tier: web
data:
  port: "8080"
  region: "us-east-1"
  emptyTag: "latest"
  shout: QUIET
  tags: "alpha,beta"
  password: "czNjcmV0"
  json: '{"team":"core","tier":"web"}'
  sum: "3"
  resources: |
    limits:
      cpu: 100m
      memory: 64Mi
    requests:
      cpu: 50m
`, "a template using sprig's functions and Helm's toYaml and toJson"); err != nil {
		return err
	}

	for _, req := range []struct{ name, body, msg, why string }{
		{"required-empty", `tag: {{ required "image.tag is required" .Values.image.tag }}`, "image.tag is required", "an empty string"},
		{"required-absent", `x: {{ required "absent is required" .Values.absent }}`, "absent is required", "a value that is not set at all"},
	} {
		files["templates/req.yaml"] = req.body + "\n"
		if res, err = w.run("template", "demo", w.chart(req.name, files)); err != nil {
			return err
		}
		if err := wantError(res); err != nil {
			return fmt.Errorf("required %q was given %s, which must fail the render: %w", req.msg, req.why, err)
		}
		if !strings.Contains(res.Stderr, req.msg) {
			return fmt.Errorf("required failed the render, but the error should carry its message, %q; stderr was:\n%s", req.msg, tail(res.Stderr))
		}
	}
	files["templates/req.yaml"] = `tag: {{ required "image.tag is required" .Values.image.tag }}` + "\n"
	if res, err = w.run("template", "-f", w.file("tag.yaml", "image:\n  tag: \"1.0\"\n"), "demo", w.chart("required", files)); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("with -f tag.yaml (image.tag: \"1.0\"), required has its value: %w", err)
	}
	if !strings.Contains(res.Stdout, "tag: 1.0") {
		return fmt.Errorf("required returns the value it checks, so req.yaml should render \"tag: 1.0\"; got:\n%s", tail(res.Stdout))
	}
	delete(files, "templates/req.yaml")

	for _, fn := range []string{`env "HOME"`, `expandenv "$HOME"`} {
		name := strings.Fields(fn)[0]
		files["templates/env.yaml"] = `home: {{ ` + fn + ` }}` + "\n"
		if res, err = w.run("template", "demo", w.chart(name, files)); err != nil {
			return err
		}
		if err := wantError(res); err != nil {
			return fmt.Errorf("Helm removes %s from sprig's functions, so a chart cannot read the environment of whoever installs it; a template calling it must not render: %w", name, err)
		}
	}
	return nil
}

// stageNamedTemplates — define, template, include and tpl: named templates
// from any file, partial or not, called with any data, piped through
// functions, and template text taken from values that can itself include.
func stageNamedTemplates(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	files := map[string]string{
		"Chart.yaml": chartYAML,
		"values.yaml": `labels:
  tier: web
greeting: "hello from {{ .Release.Name }} in {{ .Template.Name }}"
nameRef: '{{ template "demo.fullname" . }}-ref'
inline: '{{ define "demo.inline" }}IN-{{ .Values.labels.tier }}{{ end }}{{ include "demo.inline" . }}'
blank: "{{ .Values.nothing }}x"
`,
		"templates/_helpers.tpl": `{{/* The release-qualified name, cut to Kubernetes' limit. */}}
{{- define "demo.fullname" -}}
{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "demo.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- range $k, $v := .Values.labels }}
{{ $k }}: {{ $v }}
{{- end }}
{{- end -}}

{{- define "demo.pair" -}}
{{ .k }}={{ .v }}
{{- end -}}
`,
		"templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "demo.fullname" . }}
  labels:
    {{- include "demo.labels" . | nindent 4 }}
data:
  pair: {{ include "demo.pair" (dict "k" "color" "v" "blue") | quote }}
  upper: {{ include "demo.fullname" . | upper }}
  greeting: {{ tpl .Values.greeting . | quote }}
  ref: {{ tpl .Values.nameRef . }}
  local: {{ template "demo.local" . }}
  inline: {{ tpl .Values.inline . }}
  blank: {{ tpl .Values.blank . | len | quote }}
  ctx: {{ tpl "{{ .k }}-{{ .v }}" (dict "k" "a" "v" "b") }}
`,
		"templates/other.yaml": `{{- define "demo.local" -}}
defined-in-other-{{ .Chart.Version }}
{{- end -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "demo.fullname" . }}-other
`,
	}
	res, err := w.run("template", "web", w.chart("demo", files))
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantExact(res.Stdout, `---
# Source: demo/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-demo
  labels:
    app.kubernetes.io/name: demo
    app.kubernetes.io/instance: web
    tier: web
data:
  pair: "color=blue"
  upper: WEB-DEMO
  greeting: "hello from web in demo/templates/cm.yaml"
  ref: web-demo-ref
  local: defined-in-other-1.2.3
  inline: IN-web
  blank: "1"
  ctx: a-b
---
# Source: demo/templates/other.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-demo-other
`, `release web: named templates from _helpers.tpl and other.yaml, called with include, template and tpl;
tpl text may define a template and include it, its missing values print nothing, and its data can be any value`); err != nil {
		return err
	}

	files["templates/missing.yaml"] = `name: {{ include "demo.nope" . }}` + "\n"
	if res, err = w.run("template", "web", w.chart("missing", files)); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("templates/missing.yaml includes \"demo.nope\", which no file defines, so the render must fail: %w", err)
	}
	if !strings.Contains(res.Stderr, "demo.nope") {
		return fmt.Errorf("an include of an undefined template failed, but the error should name it, \"demo.nope\"; stderr was:\n%s", tail(res.Stderr))
	}

	loop := w.chart("loop", map[string]string{
		"Chart.yaml":        chartYAML,
		"templates/_r.tpl":  `{{ define "r" }}{{ include "r" . }}{{ end }}` + "\n",
		"templates/cm.yaml": `x: {{ include "r" . }}` + "\n",
	})
	if res, err = w.run("template", "web", loop); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("template r includes itself forever, which Helm stops after 1000 nested includes: %w", err)
	}
	if s := "rendering template has a nested reference name: r"; !strings.Contains(res.Stderr, s) {
		return fmt.Errorf("an include that recurses forever should fail as Helm's does, with %q; stderr was:\n%s", s, tail(res.Stderr))
	}

	// Helm parses deepest paths first, then reverse lexical, and a later define
	// replaces an earlier one: the shallowest, lexically first file wins.
	dir := w.chart("precedence", map[string]string{
		"Chart.yaml":             chartYAML,
		"templates/_a.tpl":       `{{- define "x" }}from-_a{{ end }}` + "\n",
		"templates/_b.tpl":       `{{- define "x" }}from-_b{{ end }}` + "\n" + `{{- define "y" }}shallow-_b{{ end }}` + "\n",
		"templates/sub/_c.tpl":   `{{- define "x" }}from-sub{{ end }}` + "\n",
		"templates/_a/_deep.tpl": `{{- define "y" }}deep-_a{{ end }}` + "\n",
		"templates/cm.yaml":      "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ template \"x\" }}\ndata:\n  y: {{ include \"y\" . }}\n",
	})
	if res, err = w.run("template", "web", dir); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	return wantExact(res.Stdout, `---
# Source: demo/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: from-_a
data:
  y: shallow-_b
`, `"x" defined in _a.tpl, _b.tpl and sub/_c.tpl, and "y" in _b.tpl and _a/_deep.tpl:
Helm keeps the define from the shallowest path, and at one depth the lexically first`)
}

// shopChart is a parent with two subcharts: db, with its own values, file,
// defines, globals and a subchart of its own, pool, and cache, with no
// values.yaml at all.
func shopChart() map[string]string {
	return map[string]string{
		"Chart.yaml": `apiVersion: v2
name: shop
version: 1.0.0
`,
		"charts/cache/Chart.yaml": `apiVersion: v2
name: cache
version: 0.1.0
`,
		"charts/cache/templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: cache
data:
  values: {{ toJson .Values | squote }}
`,
		"charts/db/Chart.yaml": `apiVersion: v2
name: db
version: 0.3.0
appVersion: "16"
`,
		"charts/db/db.conf": `listen = all
`,
		"charts/db/charts/pool/Chart.yaml":  "apiVersion: v2\nname: pool\nversion: 0.0.1\n",
		"charts/db/charts/pool/values.yaml": "size: 5\n",
		"charts/db/charts/pool/templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: pool
data:
  values: {{ toJson .Values | squote }}
`,
		"charts/db/templates/_helpers.tpl": `{{- define "db.name" -}}db-{{ .Release.Name }}{{- end -}}
{{- define "common.label" -}}from-db{{- end -}}
`,
		"charts/db/templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "db.name" . }}
data:
  chart: {{ .Chart.Name }}-{{ .Chart.Version }}-{{ .Chart.AppVersion }}
  port: {{ .Values.port | quote }}
  name: {{ .Values.name }}
  extra: {{ toJson .Values.extra | squote }}
  global: {{ toJson .Values.global | squote }}
  template: {{ .Template.Name }}
  base: {{ .Template.BasePath }}
  conf: {{ .Files.Get "db.conf" | trim | quote }}
  label: {{ include "common.label" . }}
  release: {{ .Release.Name }}
`,
		"charts/db/values.yaml": `global:
  region: us
  tier: data
name: orders
port: 5432
extra:
  kept: true
`,
		"templates/_helpers.tpl": `{{- define "common.label" -}}from-parent{{- end -}}
`,
		"templates/app.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-{{ .Chart.Name }}
data:
  env: {{ .Values.global.env }}
  dbPort: {{ .Values.db.port | quote }}
  dbName: {{ .Values.db.name | quote }}
  subDefine: {{ include "db.name" . }}
  label: {{ include "common.label" . }}
  parentGlobal: {{ toJson .Values.global | squote }}
  parentDb: {{ toJson .Values.db | squote }}
  parentCache: {{ toJson .Values.cache | squote }}
`,
		"values.yaml": `global:
  env: prod
  region: eu
db:
  port: 6543
  extra:
    added: yes
cache:
  enabled: true
`,
	}
}

// stageSubcharts — charts under charts/ render with the parent: each sees its
// own values laid over its defaults, the parent's globals on top of its own,
// its own .Chart, .Files and .Template, and every define in the one set.
func stageSubcharts(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	files := shopChart()
	dir := w.chart("shop", files)
	res, err := w.run("template", "web", dir)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantExact(res.Stdout, `---
# Source: shop/charts/cache/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cache
data:
  values: '{"enabled":true,"global":{"env":"prod","region":"eu"}}'
---
# Source: shop/charts/db/charts/pool/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pool
data:
  values: '{"global":{"env":"prod","region":"eu","tier":"data"},"size":5}'
---
# Source: shop/charts/db/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: db-web
data:
  chart: db-0.3.0-16
  port: "6543"
  name: orders
  extra: '{"added":true,"kept":true}'
  global: '{"env":"prod","region":"eu","tier":"data"}'
  template: shop/charts/db/templates/cm.yaml
  base: shop/charts/db/templates
  conf: "listen = all"
  label: from-parent
  release: web
---
# Source: shop/templates/app.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-shop
data:
  env: prod
  dbPort: "6543"
  dbName: "orders"
  subDefine: db-web
  label: from-parent
  parentGlobal: '{"env":"prod","region":"eu"}'
  parentDb: '{"extra":{"added":true,"kept":true},"global":{"env":"prod","region":"eu","tier":"data"},"name":"orders","pool":{"global":{"env":"prod","region":"eu","tier":"data"},"size":5},"port":6543}'
  parentCache: '{"enabled":true,"global":{"env":"prod","region":"eu"}}'
`, `release web of shop, whose charts/ holds db and cache, and db's holds pool:
shop's values under db: override db's defaults, shop's global wins over db's
and reaches pool through db, and the merged db values, pool's included, are
what shop's own templates see under .Values.db`); err != nil {
		return err
	}

	res, err = w.run("template", "--set", "db.port=1", "--set", "global.env=dev", "web", dir)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	for _, line := range []string{
		`  port: "1"`,
		`  global: '{"env":"dev","region":"eu","tier":"data"}'`,
		`  values: '{"enabled":true,"global":{"env":"dev","region":"eu"}}'`,
	} {
		if !strings.Contains(res.Stdout, line+"\n") {
			return fmt.Errorf("with --set db.port=1 --set global.env=dev, the subcharts should render %q; got:\n%s", strings.TrimSpace(line), tail(res.Stdout))
		}
	}

	// A null bound for a subchart deletes the subchart's own default too,
	// however deep, not only the parent's.
	if res, err = w.run("template", "--set", "db.port=null", "--set", "db.pool.size=null", "web", dir); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if line := `  parentDb: '{"extra":{"added":true,"kept":true},"global":{"env":"prod","region":"eu","tier":"data"},"name":"orders","pool":{"global":{"env":"prod","region":"eu","tier":"data"}}}'`; !strings.Contains(res.Stdout, line+"\n") {
		return fmt.Errorf("--set db.port=null deletes db's own port: 5432 as well as shop's db.port: 6543, and db.pool.size=null deletes pool's size: 5, so shop's .Values.db should be\n%s\ngot:\n%s", line, tail(res.Stdout))
	}

	// Helm skips charts/ entries starting with . or _, keeps a .prov file as
	// one of the parent's files, gives every subchart a global map, and renders
	// only a library chart's defines.
	extras := w.chart("extras", map[string]string{
		"Chart.yaml":                 "apiVersion: v2\nname: extras\nversion: 1.0.0\n",
		"templates/cm.yaml":          "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: extras\ndata:\n  lib: {{ include \"l.x\" . }}\n  prov: {{ .Files.Get \"charts/s-0.1.0.prov\" | trim | quote }}\n",
		"charts/.DS_Store":           "junk\n",
		"charts/_skip/README":        "junk\n",
		"charts/s-0.1.0.prov":        "signed\n",
		"charts/s/Chart.yaml":        "apiVersion: v2\nname: s\nversion: 0.1.0\n",
		"charts/s/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: s\ndata:\n  values: {{ toJson .Values | squote }}\n",
		"charts/l/Chart.yaml":        "apiVersion: v2\nname: l\nversion: 0.1.0\ntype: library\n",
		"charts/l/templates/_x.tpl":  "{{- define \"l.x\" }}from-lib{{ end }}\n",
		"charts/l/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: lib-rendered\n",
	})
	if res, err = w.run("template", "web", extras); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("charts/.DS_Store and charts/_skip are skipped and charts/s-0.1.0.prov is a file, so extras loads: %w", err)
	}
	if err := wantExact(res.Stdout, `---
# Source: extras/charts/s/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: s
data:
  values: '{"global":{}}'
---
# Source: extras/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: extras
data:
  lib: from-lib
  prov: "signed"
`, `extras: s gets an empty global though extras has none, l is a library chart
whose define extras includes and whose cm.yaml never renders, and the .prov
file is one of extras' .Files`); err != nil {
		return err
	}
	toplib := w.chart("toplib", map[string]string{
		"Chart.yaml":       "apiVersion: v2\nname: toplib\nversion: 1.0.0\ntype: library\n",
		"templates/_x.tpl": "{{ define \"q\" }}{{ end }}\n",
	})
	if res, err = w.run("template", "web", toplib); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("toplib is a library chart, which Helm will not render on its own: %w", err)
	}
	if !strings.Contains(res.Stderr, "library charts are not installable") {
		return fmt.Errorf("a library chart should be refused as Helm refuses it, \"library charts are not installable\"; stderr was:\n%s", tail(res.Stderr))
	}

	for name, file := range map[string]string{"stray-file": "charts/notes.txt", "no-chart-yaml": "charts/empty/x.yaml"} {
		bad := shopChart()
		bad[file] = "x: 1\n"
		res, err := w.run("template", "web", w.chart(name, bad))
		if err != nil {
			return err
		}
		if err := wantError(res); err != nil {
			return fmt.Errorf("%s is not a chart, and Helm loads everything in charts/ as one: %w", file, err)
		}
	}
	return nil
}

// stageConditions — Chart.yaml's dependencies switch subcharts off: the first
// condition path holding a boolean decides, then tags, where any true tag
// enables and a false one disables; a disabled subchart renders nothing and
// its defaults never reach the parent's values.
func stageConditions(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	files := shopChart()
	files["Chart.yaml"] = `apiVersion: v2
name: shop
version: 1.0.0
dependencies:
  - name: db
    version: 0.3.0
    condition: db.enabled
  - name: cache
    version: 0.1.0
    condition: cache.enabled,global.cache
    tags: [frontend]
  - name: queue
    version: 0.1.0
    condition: queue.enabled
    tags: [backend]
  - name: metrics
    version: 0.1.0
    tags: [backend, ops]
  - name: edge
    version: 0.1.0
    condition: nope.enabled, edge.enabled
`
	files["values.yaml"] = `global:
  env: prod
db:
  port: 6543
cache:
  enabled: "yes"
edge:
  enabled: false
tags:
  backend: false
`
	files["charts/db/Chart.yaml"] += `dependencies:
  - name: pool
    version: 0.0.1
    condition: pool.enabled
    tags: [pooling]
`
	files["charts/db/values.yaml"] += "tags:\n  pooling: false\n"
	files["templates/app.yaml"] = `apiVersion: v1
kind: ConfigMap
metadata:
  name: app
data:
  db: {{ toJson .Values.db | squote }}
`
	for _, sub := range []string{"queue", "metrics", "edge", "free"} {
		files["charts/"+sub+"/Chart.yaml"] = "apiVersion: v2\nname: " + sub + "\nversion: 0.1.0\n"
		files["charts/"+sub+"/templates/cm.yaml"] = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + sub + "\n"
	}
	files["charts/queue/values.yaml"] = "enabled: true\n"
	dir := w.chart("shop", files)

	src := func(subs ...string) []string {
		var out []string
		for _, s := range subs {
			out = append(out, "# Source: shop/charts/"+s+"/templates/cm.yaml")
		}
		return append(out, "# Source: shop/templates/app.yaml")
	}
	for _, c := range []struct {
		sets []string
		want []string
		why  string
	}{
		{nil, src("cache", "db", "edge", "free", "queue"),
			`cache.enabled is "yes", not a boolean, so it is skipped and cache stays on; queue's own default queue.enabled: true beats tags.backend: false; metrics has only tags, and backend is false; edge's condition is trimmed as a whole, so its second path is " edge.enabled", which is not set; free is in charts/ but not in dependencies; db's own default tags.pooling: false switches pool off`},
		{[]string{"db.enabled=false"}, src("cache", "edge", "free", "queue"), "db.enabled is false, and pool goes with it"},
		{[]string{"tags.ops=true"}, src("cache", "db", "edge", "free", "metrics", "queue"), "metrics is tagged backend and ops, and one true tag is enough"},
		{[]string{"tags.backend=true", "queue.enabled=false"}, src("cache", "db", "edge", "free", "metrics"), "queue.enabled: false is a condition, and a condition beats tags"},
		{[]string{"global.cache=false"}, src("db", "edge", "free", "queue"), "cache's condition is cache.enabled,global.cache: the first is not a boolean, the second is false"},
		{[]string{"cache.enabled=false", "tags.frontend=true"}, src("db", "edge", "free", "queue"), "cache.enabled: false decides before the frontend tag is looked at"},
		{[]string{"db.pool.enabled=true"}, src("cache", "db/charts/pool", "db", "edge", "free", "queue"), "pool's condition, pool.enabled, is read under db's values, db.pool.enabled"},
		{[]string{"tags.pooling=true"}, src("cache", "db/charts/pool", "db", "edge", "free", "queue"), "the top chart's tags.pooling: true wins over db's own default tags.pooling: false"},
	} {
		args := []string{"template"}
		for _, s := range c.sets {
			args = append(args, "--set", s)
		}
		res, err := w.run(append(args, "web", dir)...)
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return err
		}
		var got []string
		for _, l := range strings.Split(res.Stdout, "\n") {
			if strings.HasPrefix(l, "# Source: ") {
				got = append(got, l)
			}
		}
		if strings.Join(got, "\n") != strings.Join(c.want, "\n") {
			what := "the chart's values"
			if c.sets != nil {
				what = "--set " + strings.Join(c.sets, " --set ")
			}
			return fmt.Errorf("with %s: %s; expected these documents:\n  %s\ngot:\n  %s",
				what, c.why, strings.Join(c.want, "\n  "), strings.Join(got, "\n  "))
		}
		if len(c.sets) == 1 && c.sets[0] == "db.enabled=false" && !strings.Contains(res.Stdout, `  db: '{"enabled":false,"port":6543}'`+"\n") {
			return fmt.Errorf("with db switched off, shop's .Values.db is only what shop's values say, since db's defaults and globals are never merged in; expected\n  db: '{\"enabled\":false,\"port\":6543}'\ngot:\n%s", tail(res.Stdout))
		}
	}

	files["Chart.yaml"] += "  - name: ghost\n    version: 1.0.0\n    condition: ghost.enabled\n"
	files["values.yaml"] += "ghost:\n  enabled: false\n"
	res, err := w.run("template", "web", w.chart("ghost", files))
	if err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("Chart.yaml lists ghost, which charts/ does not hold, and Helm refuses that even with ghost switched off: %w", err)
	}
	if s := "found in Chart.yaml, but missing in charts/ directory: ghost"; !strings.Contains(res.Stderr, s) {
		return fmt.Errorf("the error should say, as Helm's does, %q; stderr was:\n%s", s, tail(res.Stderr))
	}
	return nil
}

// stageValuesSchema — merged values are checked against values.schema.json
// before anything renders, each subchart's slice against its own schema and
// a disabled subchart's not at all; a failure names the chart and the path.
func stageValuesSchema(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	files := shopChart()
	files["Chart.yaml"] = `apiVersion: v2
name: shop
version: 1.0.0
dependencies:
  - name: db
    version: 0.3.0
    condition: db.enabled
`
	files["values.yaml"] = `global:
  env: prod
  region: eu
db:
  port: 6543
  extra:
    added: yes
cache:
  enabled: true
replicas: 2
image:
  tag: "1.0"
  pullPolicy: IfNotPresent
`
	files["values.schema.json"] = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["replicas"],
  "properties": {
    "replicas": {"type": "integer", "minimum": 1},
    "image": {
      "type": "object",
      "properties": {"tag": {"type": "string"}, "pullPolicy": {"enum": ["Always", "IfNotPresent"]}}
    }
  }
}
`
	files["charts/db/values.schema.json"] = `{"type": "object", "properties": {"port": {"type": "integer", "maximum": 65535}}}
`
	files["charts/db/charts/pool/values.schema.json"] = `{"type": "object", "properties": {"size": {"type": "integer", "maximum": 10}}}
`
	files["templates/app.yaml"] = `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}
data:
  replicas: {{ .Values.replicas | quote }}
`
	dir := w.chart("shop", files)

	for _, sets := range [][]string{nil, {"db.enabled=false", "db.port=70000"}} {
		args := []string{"template"}
		for _, s := range sets {
			args = append(args, "--set", s)
		}
		res, err := w.run(append(args, "web", dir)...)
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			if sets != nil {
				return fmt.Errorf("db is switched off, so its schema does not apply, and db.port=70000 is no error: %w", err)
			}
			return fmt.Errorf("the chart's own values fit its schemas: %w", err)
		}
	}

	float := w.file("float.yaml", "replicas: 2.5\n")
	for _, c := range []struct {
		args []string
		want []string
		why  string
	}{
		{[]string{"--set", "replicas=0"}, []string{"shop", "/replicas"}, "shop's schema says replicas has a minimum of 1"},
		{[]string{"--set", "replicas=two"}, []string{"shop", "/replicas"}, "replicas must be an integer, not a string"},
		{[]string{"-f", float}, []string{"shop", "/replicas"}, "2.5 is a number but not an integer"},
		{[]string{"--set", "replicas=null"}, []string{"shop", "replicas"}, "the null deletes replicas, which the schema requires"},
		{[]string{"--set", "image.pullPolicy=Never"}, []string{"shop", "/image/pullPolicy"}, "pullPolicy must be Always or IfNotPresent"},
		{[]string{"--set", "db.port=70000"}, []string{"db", "/port"}, "db's own schema caps port at 65535"},
		{[]string{"--set", "db.pool.size=50"}, []string{"pool", "/size"}, "pool, db's subchart, caps size at 10 in its own schema"},
		{[]string{"--set", "image.tag=5", "--set", "replicas=0"}, []string{"/replicas", "/image/tag"}, "both values are wrong, and every failure is reported, not only the first"},
	} {
		res, err := w.run(append(append([]string{"template"}, c.args...), "web", dir)...)
		if err != nil {
			return err
		}
		what := strings.Join(c.args, " ")
		if err := wantError(res); err != nil {
			return fmt.Errorf("%s: %s: %w", what, c.why, err)
		}
		if strings.Contains(res.Stdout, "# Source:") {
			return fmt.Errorf("%s: %s, so nothing should render, but stdout was:\n%s", what, c.why, tail(res.Stdout))
		}
		for _, s := range c.want {
			if !strings.Contains(res.Stderr, s) {
				return fmt.Errorf("%s: %s; the error should name %q, but stderr was:\n%s", what, c.why, s, tail(res.Stderr))
			}
		}
	}
	return nil
}

// stageManifestSplit — rendered files split into documents, each under its
// file's Source header, ordered as Helm installs: by Helm's kind table, then
// unknown kinds alphabetically, and otherwise in path and document order.
// --show-only picks templates; a document that is not YAML fails the render.
func stageManifestSplit(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()

	files := map[string]string{
		"Chart.yaml": "apiVersion: v2\nname: web\nversion: 1.0.0\n",
		"templates/all.yaml": `# a leading comment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
---
apiVersion: v1
kind: Service
metadata:
  name: web
---

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-b
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
--- # trailing comment
apiVersion: v1
kind: Namespace
metadata:
  name: shop
`,
		"templates/a-cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: web-a
---
apiVersion: example.com/v1
kind: Gadget
metadata:
  name: g
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sa
`,
		"templates/z-crd.yaml": `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: r
---
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w0
`,
	}
	dir := w.chart("web", files)
	res, err := w.run("template", "r", dir)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantExact(res.Stdout, `---
# Source: web/templates/all.yaml
# trailing comment
apiVersion: v1
kind: Namespace
metadata:
  name: shop
---
# Source: web/templates/a-cm.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sa
---
# Source: web/templates/a-cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-a
---
# Source: web/templates/all.yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-b
---
# Source: web/templates/z-crd.yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
---
# Source: web/templates/z-crd.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: r
---
# Source: web/templates/all.yaml
apiVersion: v1
kind: Service
metadata:
  name: web
---
# Source: web/templates/all.yaml
# a leading comment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
---
# Source: web/templates/a-cm.yaml
apiVersion: example.com/v1
kind: Gadget
metadata:
  name: g
---
# Source: web/templates/all.yaml
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w
---
# Source: web/templates/z-crd.yaml
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w0
`, `three files of several documents each, split on Helm's separator (which keeps
"--- # trailing comment" as a comment line, and leaves the "---" of an empty
document at the start of the next), sorted by kind`); err != nil {
		return err
	}

	res, err = w.run("template", "-s", "templates/a-cm.yaml", "--show-only", "templates/z-crd.yaml", "r", dir)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantExact(res.Stdout, `---
# Source: web/templates/a-cm.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sa
---
# Source: web/templates/a-cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-a
---
# Source: web/templates/a-cm.yaml
apiVersion: example.com/v1
kind: Gadget
metadata:
  name: g
---
# Source: web/templates/z-crd.yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
---
# Source: web/templates/z-crd.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: r
---
# Source: web/templates/z-crd.yaml
apiVersion: example.com/v1
kind: Widget
metadata:
  name: w0
`, "-s templates/a-cm.yaml --show-only templates/z-crd.yaml: each pattern's documents in turn, in kind order"); err != nil {
		return err
	}

	res, err = w.run("template", "-s", "templates/nope.yaml", "r", dir)
	if err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("--show-only templates/nope.yaml names no template in the chart: %w", err)
	}
	if !strings.Contains(res.Stderr, "templates/nope.yaml") {
		return fmt.Errorf("--show-only templates/nope.yaml failed, but the error should name it; stderr was:\n%s", tail(res.Stderr))
	}

	files["templates/broken.yaml"] = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: [x\n"
	if res, err = w.run("template", "r", w.chart("broken", files)); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("templates/broken.yaml renders to YAML that does not parse, which Helm refuses before anything is printed: %w", err)
	}
	if !strings.Contains(res.Stderr, "web/templates/broken.yaml") {
		return fmt.Errorf("the YAML parse error should name the file, web/templates/broken.yaml; stderr was:\n%s", tail(res.Stderr))
	}
	if strings.Contains(res.Stdout, "# Source:") {
		return fmt.Errorf("a file that does not parse stops the whole render, but stdout was:\n%s", tail(res.Stdout))
	}

	// Twenty documents of two kinds: more than sort.Slice sorts by insertion,
	// so only a stable sort keeps each kind in file order.
	var many []string
	for i := 0; i < 20; i++ {
		kind := "ConfigMap"
		if i%2 == 1 {
			kind = "Secret"
		}
		many = append(many, fmt.Sprintf("apiVersion: v1\nkind: %s\nmetadata:\n  name: d%02d", kind, i))
	}
	manyDir := w.chart("many", map[string]string{
		"Chart.yaml":                "apiVersion: v2\nname: many\nversion: 1.0.0\n",
		"templates/many.yaml":       strings.Join(many, "\n---\n") + "\n",
		"templates/svc.yaml":        "apiVersion: v1\nkind: Service\nmetadata:\n  name: svc\n",
		"templates/sub/pod.yaml":    "apiVersion: v1\nkind: Pod\nmetadata:\n  name: pod\n",
		"charts/k/Chart.yaml":       "apiVersion: v2\nname: k\nversion: 0.1.0\n",
		"charts/k/templates/x.yaml": "apiVersion: rbac.authorization.k8s.io/v1\nkind: Role\nmetadata:\n  name: role\n",
	})
	odd, even := "d01 d03 d05 d07 d09 d11 d13 d15 d17 d19", "d00 d02 d04 d06 d08 d10 d12 d14 d16 d18"
	for _, c := range []struct {
		show []string
		want string
		why  string
	}{
		{nil, odd + " " + even + " role svc pod", "twenty Secrets and ConfigMaps from one file keep their file order within each kind"},
		{[]string{"-s", "templates/*.yaml"}, odd + " " + even + " svc", "the glob templates/*.yaml matches many.yaml and svc.yaml, not sub/pod.yaml or the subchart's"},
		{[]string{"-s", "charts/k/templates/x.yaml"}, "role", "a subchart's template is named from the top chart, charts/k/templates/x.yaml"},
	} {
		if res, err = w.run(append(append([]string{"template"}, c.show...), "r", manyDir)...); err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return err
		}
		var names []string
		for _, l := range strings.Split(res.Stdout, "\n") {
			if n, ok := strings.CutPrefix(l, "  name: "); ok {
				names = append(names, n)
			}
		}
		if got := strings.Join(names, " "); got != c.want {
			return fmt.Errorf("template %s r many: %s; expected documents named, in order:\n  %s\ngot:\n  %s", strings.Join(c.show, " "), c.why, c.want, got)
		}
	}

	// Every kind in Helm's table, written in reverse, plus two it does not
	// know: the known ones come back in table order, the unknown after them.
	kinds := []string{"PriorityClass", "Namespace", "NetworkPolicy", "ResourceQuota", "LimitRange", "PodSecurityPolicy", "PodDisruptionBudget", "ServiceAccount", "Secret", "SecretList", "ConfigMap", "StorageClass", "PersistentVolume", "PersistentVolumeClaim", "CustomResourceDefinition", "ClusterRole", "ClusterRoleList", "ClusterRoleBinding", "ClusterRoleBindingList", "Role", "RoleList", "RoleBinding", "RoleBindingList", "Service", "DaemonSet", "Pod", "ReplicationController", "ReplicaSet", "Deployment", "HorizontalPodAutoscaler", "StatefulSet", "Job", "CronJob", "IngressClass", "Ingress", "APIService"}
	var docs []string
	for i := len(kinds) - 1; i >= 0; i-- {
		docs = append(docs, "kind: "+kinds[i]+"\nmetadata:\n  name: x")
	}
	docs = append(docs, "kind: Zeta\nmetadata:\n  name: x", "kind: Alpha\nmetadata:\n  name: x")
	ordered := w.chart("kinds", map[string]string{
		"Chart.yaml":         "apiVersion: v2\nname: kinds\nversion: 1.0.0\n",
		"templates/all.yaml": strings.Join(docs, "\n---\n") + "\n",
	})
	if res, err = w.run("template", "r", ordered); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	var got []string
	for _, l := range strings.Split(res.Stdout, "\n") {
		if k, ok := strings.CutPrefix(l, "kind: "); ok {
			got = append(got, k)
		}
	}
	want := append(append([]string{}, kinds...), "Alpha", "Zeta")
	if strings.Join(got, " ") != strings.Join(want, " ") {
		return fmt.Errorf("documents of every kind Helm orders, given in reverse, plus the unknown kinds Zeta and Alpha:\nexpected kinds in this order:\n  %s\ngot:\n  %s", strings.Join(want, " "), strings.Join(got, " "))
	}
	return nil
}

// webChart is the chart the release stages install: two ConfigMaps and a
// Service from the top chart, notes that read values, and a subchart whose
// notes Helm leaves out.
func webChart(msg string) map[string]string {
	return map[string]string{
		"Chart.yaml":  "apiVersion: v2\nname: web\nversion: 1.0.0\nappVersion: \"2.1\"\n",
		"values.yaml": "msg: " + msg + "\nport: 8080\n",
		"templates/config.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-config
data:
  msg: {{ .Values.msg }}
  namespace: {{ .Release.Namespace }}
`,
		"templates/svc.yaml": `apiVersion: v1
kind: Service
metadata:
  name: {{ .Release.Name }}-svc
spec:
  selector:
    app: {{ .Release.Name }}
  ports:
  - name: http
    port: 80
    targetPort: {{ .Values.port }}
`,
		"templates/NOTES.txt":             "Installed {{ .Release.Name }} into {{ .Release.Namespace }}.\nSay {{ .Values.msg }}.\n",
		"charts/side/Chart.yaml":          "apiVersion: v2\nname: side\nversion: 0.1.0\n",
		"charts/side/templates/NOTES.txt": "SUBCHART NOTES\n",
		"charts/side/templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-side
`,
	}
}

// stageInstall — install creates every rendered object in the release's
// namespace, the kubeconfig's by default, marked with Helm's ownership label
// and annotations, prints Helm's summary, and refuses before creating
// anything when an object it would create exists without the release's marks.
func stageInstall(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	ns := env.Namespace

	res, err := w.run("install", "--set", "msg=hi", "first", w.chart("web", webChart("hello")))
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if err := wantSummary(res.Stdout, `NAME: first
LAST DEPLOYED: *
NAMESPACE: `+ns+`
STATUS: deployed
REVISION: 1
NOTES:
Installed first into `+ns+`.
Say hi.
`, "install first, with no -n: the kubeconfig's namespace, and only the top chart's NOTES.txt"); err != nil {
		return err
	}
	cm, err := env.ConfigMap(ctx, "first-config")
	if err != nil {
		return fmt.Errorf("install first should have created ConfigMap first-config in %s, the kubeconfig's namespace: %w", ns, err)
	}
	if cm.Data["msg"] != "hi" || cm.Data["namespace"] != ns {
		return fmt.Errorf("ConfigMap first-config should hold msg=hi (from --set) and namespace=%s, got %v", ns, cm.Data)
	}
	svc, err := env.Client.CoreV1().Services(ns).Get(ctx, "first-svc", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("install first should have created Service first-svc: %w", err)
	}
	side, err := env.ConfigMap(ctx, "first-side")
	if err != nil {
		return fmt.Errorf("install first should have created the subchart's ConfigMap first-side: %w", err)
	}
	for _, o := range []metav1.Object{cm, svc, side} {
		if err := wantMarks(o, "first", ns); err != nil {
			return err
		}
	}

	res, err = w.run("install", "Bad_Name", w.chart("web", webChart("hello")))
	if err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("Bad_Name is not a valid release name (lowercase DNS names only): %w", err)
	}

	// A Secret installs before a ConfigMap, so a program that creates as it
	// checks would leave grab-token behind.
	if err := seedConfigMap(ctx, env, "taken", nil, nil); err != nil {
		return err
	}
	grab := w.chart("grab", map[string]string{
		"Chart.yaml":           "apiVersion: v2\nname: grab\nversion: 1.0.0\n",
		"templates/token.yaml": "apiVersion: v1\nkind: Secret\nmetadata:\n  name: grab-token\nstringData:\n  k: v\n",
		"templates/taken.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: taken\ndata:\n  k: chart\n",
	})
	if res, err = w.run("install", "grab", grab); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("ConfigMap taken already exists and no release owns it, so Helm refuses the install: %w", err)
	}
	for _, s := range []string{`ConfigMap "taken" in namespace "` + ns + `" exists and cannot be imported into the current release`, `missing key "app.kubernetes.io/managed-by": must be set to "Helm"`} {
		if !strings.Contains(res.Stderr, s) {
			return fmt.Errorf("the refusal should say, as Helm does:\n  %s\nstderr was:\n%s", s, tail(res.Stderr))
		}
	}
	if _, err := env.Client.CoreV1().Secrets(ns).Get(ctx, "grab-token", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return fmt.Errorf("the refused install created Secret grab-token: every object is checked before any is created, so a refusal leaves nothing behind")
	}
	if cm, err := env.ConfigMap(ctx, "taken"); err != nil || cm.Data["k"] != "" || len(cm.Labels) != 0 {
		return fmt.Errorf("the refused install should leave ConfigMap taken as it was (no data, no labels), got %v", cm)
	}

	if err := seedConfigMap(ctx, env, "theirs",
		map[string]string{"app.kubernetes.io/managed-by": "Helm"},
		map[string]string{"meta.helm.sh/release-name": "other", "meta.helm.sh/release-namespace": ns}); err != nil {
		return err
	}
	mine := w.chart("mine", map[string]string{
		"Chart.yaml":           "apiVersion: v2\nname: mine\nversion: 1.0.0\n",
		"templates/cm.yaml":    "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: theirs\n",
		"templates/other.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: mine-other\n",
	})
	if res, err = w.run("install", "--namespace", ns, "mine", mine); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("ConfigMap theirs belongs to release other, so installing release mine over it is refused: %w", err)
	}
	if s := `key "meta.helm.sh/release-name" must equal "mine": current value is "other"`; !strings.Contains(res.Stderr, s) {
		return fmt.Errorf("the refusal should say, as Helm does:\n  %s\nstderr was:\n%s", s, tail(res.Stderr))
	}
	if _, err := env.ConfigMap(ctx, "mine-other"); !apierrors.IsNotFound(err) {
		return fmt.Errorf("the refused install of mine created ConfigMap mine-other")
	}
	return nil
}

// wantMarks checks the label and annotations Helm puts on every object it
// creates, which are how a later install knows who owns what.
func wantMarks(o metav1.Object, release, ns string) error {
	if got := o.GetLabels()["app.kubernetes.io/managed-by"]; got != "Helm" {
		return fmt.Errorf("%s should be labelled app.kubernetes.io/managed-by=Helm, labels are %v", o.GetName(), o.GetLabels())
	}
	a := o.GetAnnotations()
	if a["meta.helm.sh/release-name"] != release || a["meta.helm.sh/release-namespace"] != ns {
		return fmt.Errorf("%s should be annotated meta.helm.sh/release-name=%s and meta.helm.sh/release-namespace=%s, annotations are %v", o.GetName(), release, ns, a)
	}
	return nil
}

func seedConfigMap(ctx context.Context, env *kube.Env, name string, labels, annotations map[string]string) error {
	_, err := env.Client.CoreV1().ConfigMaps(env.Namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: annotations},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("seed ConfigMap %s: %w", name, err)
	}
	return nil
}

// wantSummary compares a release summary, as install and status print it,
// line by line. A want line "LAST DEPLOYED: *" accepts any time in Helm's
// format, and TEST SUITE is not judged.
func wantSummary(got, want, what string) error {
	var norm []string
	for _, l := range strings.Split(strings.TrimRight(got, "\n"), "\n") {
		if strings.HasPrefix(l, "TEST SUITE:") {
			continue
		}
		if v, ok := strings.CutPrefix(l, "LAST DEPLOYED: "); ok {
			if _, err := time.Parse(time.ANSIC, v); err == nil {
				l = "LAST DEPLOYED: *"
			}
		}
		norm = append(norm, l)
	}
	if strings.Join(norm, "\n")+"\n" != want {
		return fmt.Errorf("for %s, expected (LAST DEPLOYED as Go's time.ANSIC, TEST SUITE not judged):\n-----\n%s-----\ngot:\n-----\n%s\n-----", what, want, strings.TrimRight(got, "\n"))
	}
	return nil
}

// work is one stage's scratch space: a directory for charts and values files,
// and the environment that points the program at the stage's namespace.
type work struct {
	ctx context.Context
	env *kube.Env
	bin string
	dir string
	kc  []string
}

func newWork(ctx context.Context, env *kube.Env, bin string) (*work, error) {
	dir, err := os.MkdirTemp("", "byok8s-helm-")
	if err != nil {
		return nil, err
	}
	kc, err := env.KubeconfigScoped(dir)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	return &work{ctx: ctx, env: env, bin: bin, dir: dir, kc: []string{"KUBECONFIG=" + kc}}, nil
}

func (w *work) close() { os.RemoveAll(w.dir) }

// file writes one file under the scratch directory and returns its path. A
// failed write is a harness fault, so it panics rather than threading an error
// through every stage.
func (w *work) file(name, body string) string {
	path := filepath.Join(w.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		panic(err)
	}
	return path
}

// chart writes a chart directory from slash paths to contents and returns it.
func (w *work) chart(name string, files map[string]string) string {
	for p, body := range files {
		w.file(filepath.Join("charts-src", name, filepath.FromSlash(p)), body)
	}
	dir := filepath.Join(w.dir, "charts-src", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	return dir
}

func (w *work) run(args ...string) (*runner.Result, error) {
	return runner.InvokeEnv(w.ctx, w.bin, w.kc, stages.Timeout, args...)
}

func wantExit(res *runner.Result, code int) error {
	if res.ExitCode != code {
		return fmt.Errorf("expected exit %d, got %d\nstdout:\n%s\nstderr:\n%s", code, res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return nil
}

// wantError is a refusal the way Helm refuses: exit 1 with a reason on stderr,
// which a crash (exit 2, a stack trace) is not.
func wantError(res *runner.Result) error {
	if res.ExitCode != 1 || strings.TrimSpace(res.Stderr) == "" {
		return fmt.Errorf("expected exit 1 with an error message on stderr, got exit %d\nstdout:\n%s\nstderr:\n%s", res.ExitCode, tail(res.Stdout), tail(res.Stderr))
	}
	return nil
}

func wantLines(stdout string, want []string) error {
	if got := lines(stdout); strings.Join(got, "\n") != strings.Join(want, "\n") {
		return fmt.Errorf("expected exactly these lines, in this order:\n  %s\ngot:\n%s",
			strings.Join(want, "\n  "), tail(stdout))
	}
	return nil
}

// wantExact compares output byte for byte, showing both in full on a mismatch
// because the difference is often whitespace.
func wantExact(got, want, what string) error {
	if got != want {
		return fmt.Errorf("for %s, expected exactly (between the lines):\n-----\n%s-----\ngot:\n-----\n%s-----", what, want, got)
	}
	return nil
}

// wantYAML compares output with the expected document as data, so key order,
// indentation and quoting style are the program's own business.
func wantYAML(got, want, what string) error {
	var g, e any
	if err := yaml.Unmarshal([]byte(got), &g); err != nil {
		return fmt.Errorf("expected %s as YAML, but the output does not parse: %v\n%s", what, err, tail(got))
	}
	if err := yaml.Unmarshal([]byte(want), &e); err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(g, e) {
		return fmt.Errorf("expected %s:\n%s\ngot:\n%s", what, indent(want), tail(got))
	}
	return nil
}

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n  ")
}

// lines is stdout split into trimmed, non-empty lines.
func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// tail trims captured output to something readable in a failure message.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "  (nothing)"
	}
	l := strings.Split(s, "\n")
	if len(l) > 20 {
		l = append([]string{"…"}, l[len(l)-20:]...)
	}
	return "  " + strings.Join(l, "\n  ")
}

// stageReleaseStorage — install records revision 1 as a Secret in Helm's own
// format, which the real helm reads back; a name with any revision is refused,
// a refused install records nothing, and a failed create is recorded as failed.
func stageReleaseStorage(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	ns := env.Namespace

	files := webChart("hello")
	files["README.md"] = "read me\n"
	web := w.chart("web", files)
	vals := w.file("vals.yaml", "port: 9090\nextra:\n  list: [1, 2]\n")
	start := time.Now().Add(-time.Second)
	res, err := w.run("install", "-f", vals, "--set", "msg=hi", "first", web)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	end := time.Now().Add(time.Second)
	summary := `NAME: first
LAST DEPLOYED: *
NAMESPACE: ` + ns + `
STATUS: deployed
REVISION: 1
NOTES:
Installed first into ` + ns + `.
Say hi.
`
	if err := wantSummary(res.Stdout, summary, "install first"); err != nil {
		return err
	}

	rec, err := wantRecord(ctx, env, "first", 1, "deployed", start, end)
	if err != nil {
		return err
	}
	tmpl, err := w.run("template", "-n", ns, "-f", vals, "--set", "msg=hi", "first", web)
	if err != nil {
		return err
	}
	if err := wantExit(tmpl, 0); err != nil {
		return fmt.Errorf("template, run with install's arguments, to compare with the recorded manifest: %w", err)
	}
	var templates []any
	for _, n := range []string{"templates/NOTES.txt", "templates/config.yaml", "templates/svc.yaml"} {
		templates = append(templates, map[string]any{"name": n, "data": base64.StdEncoding.EncodeToString([]byte(files[n]))})
	}
	for _, c := range []struct {
		path string
		got  any
		want any
	}{
		{"name", rec["name"], "first"},
		{"namespace", rec["namespace"], ns},
		{"version", rec["version"], 1.0},
		{"info.status", dig(rec, "info", "status"), "deployed"},
		{"info.description", dig(rec, "info", "description"), "Install complete"},
		{"info.notes (rendered, not trimmed)", dig(rec, "info", "notes"), "Installed first into " + ns + ".\nSay hi.\n"},
		{"chart.metadata", dig(rec, "chart", "metadata"), map[string]any{"name": "web", "version": "1.0.0", "appVersion": "2.1", "apiVersion": "v2"}},
		{"chart.templates (the top chart's only, data in base64), sorted by name", sortedByName(dig(rec, "chart", "templates")), templates},
		{"chart.values (the chart's defaults)", dig(rec, "chart", "values"), map[string]any{"msg": "hello", "port": 8080.0}},
		{"chart.files (every other file of the top chart)", sortedByName(dig(rec, "chart", "files")), []any{map[string]any{"name": "README.md", "data": base64.StdEncoding.EncodeToString([]byte("read me\n"))}}},
		{"config (the user's values: -f, then --set)", rec["config"], map[string]any{"msg": "hi", "port": 9090.0, "extra": map[string]any{"list": []any{1.0, 2.0}}}},
		{"manifest (what template prints)", rec["manifest"], tmpl.Stdout},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			return fmt.Errorf("the release record's %s should be\n  %s\ngot\n  %s", c.path, jsonText(c.want), jsonText(c.got))
		}
	}
	first, _ := dig(rec, "info", "first_deployed").(string)
	last, _ := dig(rec, "info", "last_deployed").(string)
	if t, err := time.Parse(time.RFC3339Nano, last); err != nil || first != last || t.Before(start) || t.After(end) {
		return fmt.Errorf("info.first_deployed and info.last_deployed should both be the install's time, as Go's time.Time marshals to JSON (RFC 3339), got %q and %q", first, last)
	}

	// The real helm reads what the program wrote.
	res, err = w.helm("status", "first")
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("helm status first, run by the real helm against your record: %w", err)
	}
	if err := wantSummary(res.Stdout, summary, "helm status first, run by the real helm against your record"); err != nil {
		return err
	}
	res, err = w.helm("get", "values", "first", "-a", "-o", "json")
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("helm get values first -a, run by the real helm against your record: %w", err)
	}
	if err := wantYAML(res.Stdout, `{msg: hi, port: 9090, extra: {list: [1, 2]}}`, "helm get values first --all, the record's config over its chart's values"); err != nil {
		return err
	}
	res, err = w.helm("list", "-o", "json")
	if err != nil {
		return err
	}
	var listed []map[string]any
	if err := json.Unmarshal([]byte(res.Stdout), &listed); err != nil || len(listed) != 1 {
		return fmt.Errorf("helm list, run by the real helm, should find exactly release first, got:\n%s\n%s", tail(res.Stdout), tail(res.Stderr))
	}
	delete(listed[0], "updated")
	if want := map[string]any{"name": "first", "namespace": ns, "revision": "1", "status": "deployed", "chart": "web-1.0.0", "app_version": "2.1"}; !reflect.DeepEqual(listed[0], want) {
		return fmt.Errorf("helm list, run by the real helm against your record, should show\n  %s\ngot\n  %s", jsonText(want), jsonText(listed[0]))
	}

	// A name with any revision is taken, and nothing is touched.
	if res, err = w.run("install", "--set", "msg=again", "first", web); err != nil {
		return err
	}
	if err := wantRefusal(res, "cannot re-use a name that is still in use", "installing first a second time"); err != nil {
		return err
	}
	if cm, err := env.ConfigMap(ctx, "first-config"); err != nil || cm.Data["msg"] != "hi" {
		return fmt.Errorf("the refused second install of first should leave ConfigMap first-config with msg=hi, got %v (%v)", cm, err)
	}
	if err := wantRevisions(ctx, env, "first", 1); err != nil {
		return err
	}

	// The same holds for a release the real helm installed.
	if res, err = w.helm("install", "--set", "msg=h", "helmed", web); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("the grader's own helm install helmed: %w", err)
	}
	if res, err = w.run("install", "helmed", web); err != nil {
		return err
	}
	if err := wantRefusal(res, "cannot re-use a name that is still in use", "installing helmed, which the real helm installed"); err != nil {
		return err
	}

	// An install refused by the ownership check never got as far as a record.
	if err := seedConfigMap(ctx, env, "grab-taken", nil, nil); err != nil {
		return err
	}
	grab := w.chart("grab", map[string]string{
		"Chart.yaml":           "apiVersion: v2\nname: grab\nversion: 1.0.0\n",
		"templates/taken.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: grab-taken\n",
	})
	if res, err = w.run("install", "grab", grab); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("ConfigMap grab-taken exists and no release owns it: %w", err)
	}
	if err := wantRevisions(ctx, env, "grab", 0); err != nil {
		return fmt.Errorf("%w: a refusal by the ownership check comes before the record is written", err)
	}

	// A create the API server rejects leaves a failed revision.
	broken := w.chart("broken", map[string]string{
		"Chart.yaml":         "apiVersion: v2\nname: broken\nversion: 0.2.0\n",
		"templates/a.yaml":   "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: broken-ok\n",
		"templates/bad.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: broken-bad\ndata:\n  \"no spaces\": x\n",
	})
	start = time.Now().Add(-time.Second)
	if res, err = w.run("install", "broken", broken); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("ConfigMap broken-bad has a data key the API server rejects (\"no spaces\"), so install broken fails: %w", err)
	}
	if !strings.Contains(res.Stderr, "INSTALLATION FAILED") {
		return fmt.Errorf("a failed install says so, as Helm does (Error: INSTALLATION FAILED: ...), stderr was:\n%s", tail(res.Stderr))
	}
	rec, err = wantRecord(ctx, env, "broken", 1, "failed", start, time.Now().Add(time.Second))
	if err != nil {
		return fmt.Errorf("%w (the create of broken-bad was rejected, so revision 1 ends failed)", err)
	}
	if d, _ := dig(rec, "info", "description").(string); !strings.HasPrefix(d, `Release "broken" failed: `) || !strings.Contains(d, "is invalid") {
		return fmt.Errorf(`a failed revision's info.description is Helm's: Release "broken" failed: <the error from the API server>, got %q`, d)
	}
	if res, err = w.helm("status", "broken"); err != nil {
		return err
	}
	if !strings.Contains(res.Stdout, "STATUS: failed\n") {
		return fmt.Errorf("helm status broken, run by the real helm against your record, should say STATUS: failed, got:\n%s\n%s", tail(res.Stdout), tail(res.Stderr))
	}
	if res, err = w.run("install", "broken", broken); err != nil {
		return err
	}
	return wantRefusal(res, "cannot re-use a name that is still in use", "installing broken again: a failed revision still holds the name")
}

// wantRecord finds a release's revision Secret by Helm's labels, checks the
// labels and type, and returns the decoded record.
func wantRecord(ctx context.Context, env *kube.Env, name string, version int, status string, start, end time.Time) (map[string]any, error) {
	secrets, err := env.Client.CoreV1().Secrets(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "owner=helm,name=" + name + ",version=" + strconv.Itoa(version)})
	if err != nil {
		return nil, err
	}
	secretName := fmt.Sprintf("sh.helm.release.v1.%s.v%d", name, version)
	if len(secrets.Items) != 1 || secrets.Items[0].Name != secretName {
		var got []string
		for _, s := range secrets.Items {
			got = append(got, s.Name)
		}
		return nil, fmt.Errorf("expected one Secret labelled owner=helm,name=%s,version=%d, named %s, found %v", name, version, secretName, got)
	}
	s := secrets.Items[0]
	if s.Type != "helm.sh/release.v1" {
		return nil, fmt.Errorf("Secret %s should have type helm.sh/release.v1, got %q", s.Name, s.Type)
	}
	mod, err := strconv.ParseInt(s.Labels["modifiedAt"], 10, 64)
	if err != nil || mod < start.Unix() || mod > end.Unix() {
		return nil, fmt.Errorf("Secret %s's label modifiedAt should be the Unix time, in seconds, it was last written, got %q", s.Name, s.Labels["modifiedAt"])
	}
	want := map[string]string{"name": name, "owner": "helm", "status": status, "version": strconv.Itoa(version), "modifiedAt": s.Labels["modifiedAt"]}
	if !reflect.DeepEqual(s.Labels, want) {
		return nil, fmt.Errorf("Secret %s should be labelled exactly %v, got %v", s.Name, want, s.Labels)
	}
	gz, err := base64.StdEncoding.DecodeString(string(s.Data["release"]))
	if err != nil {
		return nil, fmt.Errorf("Secret %s's data.release should be base64 text (of gzipped JSON): %v", s.Name, err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, fmt.Errorf("Secret %s's data.release, base64-decoded, should be gzip: %v", s.Name, err)
	}
	var rec map[string]any
	if err := json.NewDecoder(zr).Decode(&rec); err != nil {
		return nil, fmt.Errorf("Secret %s's data.release, decoded and gunzipped, should be the release as JSON: %v", s.Name, err)
	}
	return rec, nil
}

// wantRevisions checks how many revision Secrets a release has.
func wantRevisions(ctx context.Context, env *kube.Env, name string, n int) error {
	secrets, err := env.Client.CoreV1().Secrets(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "owner=helm,name=" + name})
	if err != nil {
		return err
	}
	if len(secrets.Items) != n {
		var got []string
		for _, s := range secrets.Items {
			got = append(got, s.Name)
		}
		return fmt.Errorf("release %s should have %d revision Secret(s), found %v", name, n, got)
	}
	return nil
}

// wantRefusal is a refusal carrying Helm's own words.
func wantRefusal(res *runner.Result, text, what string) error {
	if err := wantError(res); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if !strings.Contains(res.Stderr, text) {
		return fmt.Errorf("%s should be refused as Helm refuses it:\n  %s\nstderr was:\n%s", what, text, tail(res.Stderr))
	}
	return nil
}

// dig walks nested JSON objects, returning nil where a key is missing.
func dig(v any, keys ...string) any {
	for _, k := range keys {
		m, _ := v.(map[string]any)
		v = m[k]
	}
	return v
}

// sortedByName orders a JSON list of {name, data} entries by name, since the
// order a program walks a chart in is its own business.
func sortedByName(v any) any {
	l, ok := v.([]any)
	if !ok {
		return v
	}
	l = append([]any(nil), l...)
	sort.SliceStable(l, func(i, j int) bool {
		a, _ := dig(l[i], "name").(string)
		b, _ := dig(l[j], "name").(string)
		return a < b
	})
	return l
}

func jsonText(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// needHelm checks for the real helm, which the release stages grade against:
// what the program records must be readable by it, and what it prints must
// match it.
func needHelm(w *work) error {
	if _, err := exec.LookPath("helm"); err != nil {
		return fmt.Errorf("this stage compares your program with the real helm, and there is no helm on PATH: install one (mise install provisions the version the course is graded with)")
	}
	res, err := w.helm("version", "--short")
	if err != nil {
		return err
	}
	if !strings.HasPrefix(res.Stdout, "v3.") {
		return fmt.Errorf("this stage is graded against helm 3; helm version --short says %q", strings.TrimSpace(res.Stdout))
	}
	return nil
}

// helm runs the real helm against the stage's cluster and namespace. The
// context is named outright so that no HELM_KUBECONTEXT in the environment
// can point it at another cluster.
func (w *work) helm(args ...string) (*runner.Result, error) {
	env := append([]string{"HELM_NAMESPACE=" + w.env.Namespace}, w.kc...)
	return runner.InvokeEnv(w.ctx, "helm", env, stages.Timeout, append([]string{"--kube-context", cluster.Context}, args...)...)
}

// stageListStatus — list and status print exactly what the real helm prints
// for the same releases, some installed by the program and some by helm, in
// two namespaces and in every state the filters tell apart.
func stageListStatus(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	ns := env.Namespace
	ns2, err := extraNamespace(ctx, env, "other")
	if err != nil {
		return err
	}
	defer env.Client.CoreV1().Namespaces().Delete(context.WithoutCancel(ctx), ns2, metav1.DeleteOptions{})

	web := w.chart("web", webChart("hello"))
	broken := w.chart("broken", map[string]string{
		"Chart.yaml":         "apiVersion: v2\nname: broken\nversion: 0.2.0\n",
		"templates/bad.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: broken-bad\ndata:\n  \"no spaces\": x\n",
	})
	// "web" and "web-a" sort one way by name and the other by Secret name.
	for _, step := range []struct {
		helm bool
		args []string
		exit int
	}{
		{false, []string{"install", "--set", "msg=a", "web-a", web}, 0},
		{false, []string{"install", "web", web}, 0},
		{false, []string{"install", "broken", broken}, 1},
		{false, []string{"install", "-n", ns2, "web-c", web}, 0},
		{true, []string{"install", "helmed", web}, 0},
		{true, []string{"upgrade", "--set", "msg=two", "helmed", web}, 0},
		{true, []string{"install", "dropped", web}, 0},
		{true, []string{"uninstall", "--keep-history", "dropped"}, 0},
		{true, []string{"install", "-n", ns2, "elsewhere", web}, 0},
	} {
		run, who := w.run, "app"
		if step.helm {
			run, who = w.helm, "the grader's helm"
		}
		res, err := run(step.args...)
		if err != nil {
			return err
		}
		if err := wantExit(res, step.exit); err != nil {
			return fmt.Errorf("%s %s: %w", who, strings.Join(step.args, " "), err)
		}
	}

	for _, args := range [][]string{
		{"list"},
		{"list", "-a"},
		{"list", "--all"},
		{"list", "-q"},
		{"list", "--short", "-a"},
		{"list", "-n", ns2},
		{"list", "--namespace", ns2, "-q"},
		{"status", "web-a"},
		{"status", "broken"},
		{"status", "helmed"},
		{"status", "dropped"},
		{"status", "-n", ns2, "elsewhere"},
	} {
		if err := sameAsHelm(w, args, nil); err != nil {
			return err
		}
	}
	// Every namespace holds other releases too, some coming and going, so
	// only the rows from this stage's namespaces are compared.
	ours := func(row string) bool {
		f := strings.Split(row, "\t")
		return len(f) > 1 && (strings.TrimSpace(f[1]) == ns || strings.TrimSpace(f[1]) == ns2 || strings.TrimSpace(f[0]) == "NAME")
	}
	for _, args := range [][]string{{"list", "-A"}, {"list", "--all-namespaces", "-a"}} {
		if err := sameAsHelm(w, args, ours); err != nil {
			return err
		}
	}
	names := map[string]bool{"web-a": true, "web": true, "broken": true, "helmed": true, "dropped": true, "web-c": true, "elsewhere": true}
	if err := sameAsHelm(w, []string{"list", "-A", "-q"}, func(l string) bool { return names[l] }); err != nil {
		return err
	}

	res, err := w.run("status", "nope")
	if err != nil {
		return err
	}
	return wantRefusal(res, "release: not found", "status nope, a release that does not exist")
}

// sameAsHelm runs the same command through the program and the real helm and
// wants the same exit code and stdout. With keep, only the lines it keeps are
// compared, each with the padding around its cells trimmed, since the widths
// of a table depend on rows that were dropped.
func sameAsHelm(w *work, args []string, keep func(string) bool) error {
	want, err := w.helm(args...)
	if err != nil {
		return err
	}
	got, err := w.run(args...)
	if err != nil {
		return err
	}
	if err := wantExit(got, want.ExitCode); err != nil {
		return fmt.Errorf("%s, which exits %d for the real helm: %w", strings.Join(args, " "), want.ExitCode, err)
	}
	what := strings.Join(args, " ") + ", as the real helm prints it for the same releases"
	if keep == nil {
		return wantExact(got.Stdout, want.Stdout, what)
	}
	filter := func(s string) string {
		var out []string
		for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
			if keep(l) {
				cells := strings.Split(l, "\t")
				for i := range cells {
					cells[i] = strings.TrimSpace(cells[i])
				}
				out = append(out, strings.Join(cells, "\t"))
			}
		}
		return strings.Join(out, "\n") + "\n"
	}
	return wantExact(filter(got.Stdout), filter(want.Stdout), what+" (only this stage's releases, padding trimmed)")
}

// extraNamespace creates a second namespace for the stage, named after its
// own, starting from scratch if an earlier run left one behind.
func extraNamespace(ctx context.Context, env *kube.Env, suffix string) (string, error) {
	name := env.Namespace + "-" + suffix
	nss := env.Client.CoreV1().Namespaces()
	if err := nss.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("delete namespace %s: %w", name, err)
	}
	if err := kube.WaitFor(ctx, "the previous "+name+" namespace to go away", func(ctx context.Context) (bool, error) {
		_, err := nss.Get(ctx, name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	}); err != nil {
		return "", err
	}
	_, err := nss.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{kube.StageLabel: strings.TrimPrefix(name, "bko-")}}}, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create namespace %s: %w", name, err)
	}
	return name, nil
}

// stageHistory — history and get print exactly what the real helm prints for
// every revision of a release the program installed and helm then upgraded,
// failed and upgraded again, past revision 10.
func stageHistory(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	web := w.chart("web", webChart("hello"))
	broken := w.chart("broken", map[string]string{
		"Chart.yaml":         "apiVersion: v2\nname: broken\nversion: 0.2.0\n",
		"templates/bad.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: broken-bad\ndata:\n  \"no spaces\": x\n",
	})
	res, err := w.run("install", "--set", "msg=one", "web", web)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	if res, err = w.run("install", "plain", web); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return err
	}
	// Revision 3 fails; the rest supersede each other up to 11, which sorts
	// before 2 as text. --history-max 0 keeps every revision.
	upgrades := [][]string{{"--set", "msg=two", "web", web}, {"web", broken}}
	for r := 4; r <= 11; r++ {
		upgrades = append(upgrades, []string{"--set", fmt.Sprintf("msg=r%d", r), "web", web})
	}
	for _, u := range upgrades {
		res, err := w.helm(append([]string{"upgrade", "--history-max", "0"}, u...)...)
		if err != nil {
			return err
		}
		want := 0
		if u[1] == broken {
			want = 1
		}
		if err := wantExit(res, want); err != nil {
			return fmt.Errorf("the grader's helm upgrade %s, on top of your release: %w", strings.Join(u[:len(u)-1], " "), err)
		}
	}

	for _, args := range [][]string{
		{"history", "web"},
		{"history", "--max", "3", "web"},
		{"history", "plain"},
		{"get", "manifest", "web"},
		{"get", "manifest", "--revision", "1", "web"},
		{"get", "manifest", "--revision", "3", "web"},
		{"get", "values", "web"},
		{"get", "values", "--revision", "1", "web"},
		{"get", "values", "--revision", "3", "web"},
		{"get", "values", "plain"},
		{"get", "values", "--all", "web"},
		{"get", "values", "-a", "--revision", "1", "web"},
		{"get", "values", "-a", "plain"},
		{"get", "notes", "web"},
		{"get", "notes", "--revision", "1", "web"},
		{"get", "notes", "--revision", "3", "web"},
	} {
		if err := sameAsHelm(w, args, nil); err != nil {
			return err
		}
	}
	for _, args := range [][]string{
		{"history", "nope"},
		{"get", "manifest", "--revision", "12", "web"},
		{"get", "values", "nope"},
	} {
		res, err := w.run(args...)
		if err != nil {
			return err
		}
		if err := wantRefusal(res, "release: not found", strings.Join(args, " ")); err != nil {
			return err
		}
	}
	return nil
}

// upChart is the chart the upgrade stages move between: a ConfigMap whose
// "changing" key differs by version, one identical in every version, and a
// third that only one version has.
func upChart(version, changing, third string) map[string]string {
	cm := func(name, data string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-" + name + "\ndata:\n" + data
	}
	return map[string]string{
		"Chart.yaml":          "apiVersion: v2\nname: up\nversion: " + version + "\n",
		"values.yaml":         "color: red\nsize: small\n",
		"templates/NOTES.txt": "color={{ .Values.color }} size={{ .Values.size }}\n",
		"templates/cm.yaml": cm("main", "  steady: \"same\"\n  changing: \""+changing+"\"\n  color: {{ .Values.color | quote }}\n  size: {{ .Values.size | quote }}\n") +
			"---\n" + cm("untouched", "  k: \"v\"\n") + "---\n" + cm(third, "  k: \"v\"\n"),
	}
}

// upCharts writes version 1 (with -dropped), version 2 (with -fresh instead)
// and a broken version 3: version 2 plus an object the API server rejects.
func upCharts(w *work) (v1, v2, bad string) {
	b := upChart("3.0.0", "new", "fresh")
	delete(b, "templates/NOTES.txt")
	b["templates/cm.yaml"] += "---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-Invalid_Name\ndata:\n  k: \"v\"\n"
	return w.chart("up-v1", upChart("1.0.0", "old", "dropped")), w.chart("up-v2", upChart("2.0.0", "new", "fresh")), w.chart("up-bad", b)
}

// editConfigMap changes a live ConfigMap the way a person with kubectl would.
func editConfigMap(ctx context.Context, env *kube.Env, name string, data, labels map[string]string) error {
	cms := env.Client.CoreV1().ConfigMaps(env.Namespace)
	cm, err := cms.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("ConfigMap %s, which the install should have created: %w", name, err)
	}
	for k, v := range data {
		cm.Data[k] = v
	}
	for k, v := range labels {
		cm.Labels[k] = v
	}
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("edit ConfigMap %s: %w", name, err)
	}
	return nil
}

func wantData(ctx context.Context, env *kube.Env, name string, want map[string]string, why string) error {
	cm, err := env.ConfigMap(ctx, name)
	if err != nil {
		return fmt.Errorf("ConfigMap %s should exist %s: %w", name, why, err)
	}
	if !reflect.DeepEqual(cm.Data, want) {
		return fmt.Errorf("ConfigMap %s should hold %v %s, got %v", name, want, why, cm.Data)
	}
	return nil
}

func wantGone(ctx context.Context, env *kube.Env, name, why string) error {
	if _, err := env.ConfigMap(ctx, name); !apierrors.IsNotFound(err) {
		return fmt.Errorf("ConfigMap %s should not exist %s (err: %v)", name, why, err)
	}
	return nil
}

// wantStep runs one command of the program and wants it to exit 0.
func wantStep(w *work, args ...string) (*runner.Result, error) {
	res, err := w.run(args...)
	if err != nil {
		return nil, err
	}
	if err := wantExit(res, 0); err != nil {
		return nil, fmt.Errorf("%s: %w", strings.Join(args, " "), err)
	}
	return res, nil
}

// wantHelmHistory reads a release with the real helm and wants each revision's
// status and description; a description ending in "*" is a prefix.
func wantHelmHistory(w *work, name string, want [][2]string) error {
	res, err := w.helm("history", "-o", "json", name)
	if err != nil {
		return err
	}
	var h []struct {
		Revision    int    `json:"revision"`
		Status      string `json:"status"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &h); err != nil {
		return fmt.Errorf("helm history %s, run by the real helm against your records: %v\n%s", name, err, tail(res.Stderr))
	}
	var got, exp []string
	for i, r := range h {
		d := r.Description
		if i < len(want) {
			if p, ok := strings.CutSuffix(want[i][1], "*"); ok && strings.HasPrefix(d, p) {
				d = want[i][1]
			}
		}
		got = append(got, fmt.Sprintf("%d %s %q", r.Revision, r.Status, d))
	}
	for i, r := range want {
		exp = append(exp, fmt.Sprintf("%d %s %q", i+1, r[0], r[1]))
	}
	if strings.Join(got, "\n") != strings.Join(exp, "\n") {
		return fmt.Errorf("helm history %s, run by the real helm against your records, should list (revision, status, description; * ends a prefix):\n  %s\ngot:\n  %s", name, strings.Join(exp, "\n  "), strings.Join(got, "\n  "))
	}
	return nil
}

// stageUpgrade — upgrade records the next revision and patches the cluster
// with Helm's three-way merge: what the chart declares is reset, what only the
// cluster knows survives, dropped objects go and new ones are created. Values
// carry over only when none are given, a failed upgrade leaves the deployed
// revision deployed, and an unowned object in the way records nothing.
func stageUpgrade(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	ns := env.Namespace
	v1, v2, bad := upCharts(w)
	summary := func(first string, rev int, notes string) string {
		return first + "NAME: up-web\nLAST DEPLOYED: *\nNAMESPACE: " + ns + "\nSTATUS: deployed\nREVISION: " + strconv.Itoa(rev) + "\nNOTES:\n" + notes + "\n"
	}
	upgraded := "Release \"up-web\" has been upgraded. Happy Helming!\n"

	if _, err := wantStep(w, "install", "--set", "color=blue", "up-web", v1); err != nil {
		return err
	}
	start := time.Now().Add(-time.Second)
	if err := editConfigMap(ctx, env, "up-web-main", map[string]string{"steady": "edited-live", "changing": "edited-live", "liveadded": "yes"}, map[string]string{"live": "label"}); err != nil {
		return err
	}
	if err := editConfigMap(ctx, env, "up-web-untouched", map[string]string{"k": "edited-live", "extra": "live"}, nil); err != nil {
		return err
	}
	res, err := wantStep(w, "upgrade", "up-web", v2)
	if err != nil {
		return err
	}
	if err := wantSummary(res.Stdout, summary(upgraded, 2, "color=blue size=small"), "upgrade up-web to version 2 with no values given: the last revision's color=blue carries over"); err != nil {
		return err
	}
	why := "after the upgrade: keys the chart declares take the chart's value, even where the chart did not change them, and keys only the cluster has survive"
	if err := wantData(ctx, env, "up-web-main", map[string]string{"steady": "same", "changing": "new", "color": "blue", "size": "small", "liveadded": "yes"}, why); err != nil {
		return err
	}
	if err := wantData(ctx, env, "up-web-untouched", map[string]string{"k": "v", "extra": "live"}, why+" (web-untouched is the same in both versions, and still patched)"); err != nil {
		return err
	}
	cm, err := env.ConfigMap(ctx, "up-web-main")
	if err != nil {
		return err
	}
	if cm.Labels["live"] != "label" {
		return fmt.Errorf("the label live=label, added to up-web-main in the cluster, should survive the upgrade: the chart never mentions it. Labels are %v", cm.Labels)
	}
	if err := wantMarks(cm, "up-web", ns); err != nil {
		return err
	}
	if err := wantGone(ctx, env, "up-web-dropped", "after the upgrade: version 2 of the chart no longer has it"); err != nil {
		return err
	}
	fresh, err := env.ConfigMap(ctx, "up-web-fresh")
	if err != nil {
		return fmt.Errorf("the upgrade should create up-web-fresh, new in version 2: %w", err)
	}
	if err := wantMarks(fresh, "up-web", ns); err != nil {
		return err
	}
	r1, err := wantRecord(ctx, env, "up-web", 1, "superseded", start.Add(-time.Minute), time.Now().Add(time.Second))
	if err != nil {
		return fmt.Errorf("%w (an upgrade supersedes the revision it replaces)", err)
	}
	r2, err := wantRecord(ctx, env, "up-web", 2, "deployed", start, time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	for _, c := range []struct {
		path      string
		got, want any
	}{
		{"info.description", dig(r2, "info", "description"), "Upgrade complete"},
		{"info.first_deployed (the install's)", dig(r2, "info", "first_deployed"), dig(r1, "info", "first_deployed")},
		{"chart.metadata.version", dig(r2, "chart", "metadata", "version"), "2.0.0"},
		{"config (the last revision's, since none were given)", r2["config"], map[string]any{"color": "blue"}},
		{"manifest (what template prints for version 2)", strings.Contains(fmt.Sprint(r2["manifest"]), "name: up-web-fresh") && !strings.Contains(fmt.Sprint(r2["manifest"]), "up-web-dropped"), true},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			return fmt.Errorf("revision 2's %s should be %s, got %s", c.path, jsonText(c.want), jsonText(c.got))
		}
	}

	// Values carry over only when none are given, unless asked.
	for _, c := range []struct {
		args   []string
		rev    int
		notes  string
		config any
		what   string
	}{
		{[]string{"--set", "size=big"}, 3, "color=red size=big", map[string]any{"size": "big"}, "--set size=big: any value given replaces the last revision's, so color is the chart's again"},
		{[]string{"--reuse-values", "--set", "color=green"}, 4, "color=green size=big", map[string]any{"color": "green", "size": "big"}, "--reuse-values --set color=green: the new values over the last revision's"},
		{nil, 5, "color=green size=big", map[string]any{"color": "green", "size": "big"}, "no values: the last revision's again"},
		{[]string{"--reset-values"}, 6, "color=red size=small", nil, "--reset-values: the chart's defaults only, and no user values recorded"},
	} {
		args := append(append([]string{"upgrade"}, c.args...), "up-web", v2)
		res, err := wantStep(w, args...)
		if err != nil {
			return err
		}
		if err := wantSummary(res.Stdout, summary(upgraded, c.rev, c.notes), "upgrade "+c.what); err != nil {
			return err
		}
		rec, err := wantRecord(ctx, env, "up-web", c.rev, "deployed", start, time.Now().Add(time.Second))
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(rec["config"], c.config) {
			return fmt.Errorf("upgrade %s\nrevision %d's config should be %s, got %s", c.what, c.rev, jsonText(c.config), jsonText(rec["config"]))
		}
	}

	if res, err = w.run("upgrade", "up-nosuch", v2); err != nil {
		return err
	}
	if err := wantRefusal(res, `UPGRADE FAILED: "up-nosuch" has no deployed releases`, "upgrade up-nosuch, a release that does not exist"); err != nil {
		return err
	}
	if err := wantRevisions(ctx, env, "up-nosuch", 0); err != nil {
		return err
	}
	res, err = wantStep(w, "upgrade", "--install", "up-via", v2)
	if err != nil {
		return err
	}
	if err := wantSummary(strings.ReplaceAll(res.Stdout, "up-via", "up-web"), summary("Release \"up-web\" does not exist. Installing it now.\n", 1, "color=red size=small"), "upgrade --install up-via, which does not exist yet (up-via written as up-web here)"); err != nil {
		return err
	}
	rec, err := wantRecord(ctx, env, "up-via", 1, "deployed", start, time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	if d := dig(rec, "info", "description"); d != "Install complete" {
		return fmt.Errorf("upgrade --install of a new release is an install: revision 1's description should be \"Install complete\", got %v", d)
	}
	if res, err = wantStep(w, "upgrade", "-i", "up-via", v2); err != nil {
		return err
	}
	if !strings.HasPrefix(res.Stdout, "Release \"up-via\" has been upgraded. Happy Helming!\n") {
		return fmt.Errorf("upgrade -i up-via, which exists now, is a plain upgrade, printing Release \"up-via\" has been upgraded. Happy Helming! first; got:\n%s", tail(res.Stdout))
	}

	// A failed upgrade is recorded, and the deployed revision stays deployed.
	if res, err = w.run("upgrade", "up-web", bad); err != nil {
		return err
	}
	if err := wantRefusal(res, "Error: UPGRADE FAILED: failed to create resource: ", "upgrade up-web to a chart with the invalid name up-web-Invalid_Name"); err != nil {
		return err
	}
	rec, err = wantRecord(ctx, env, "up-web", 7, "failed", start, time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	if d, _ := dig(rec, "info", "description").(string); !strings.HasPrefix(d, `Upgrade "up-web" failed: failed to create resource: `) || !strings.Contains(d, "is invalid") {
		return fmt.Errorf(`a failed upgrade's info.description is Upgrade "up-web" failed: <the error>, got %q`, d)
	}
	if _, err := wantRecord(ctx, env, "up-web", 6, "deployed", start, time.Now().Add(time.Second)); err != nil {
		return fmt.Errorf("%w (a failed upgrade supersedes nothing)", err)
	}
	// The next upgrade starts from revision 6, the deployed one, whose
	// up-web-fresh is already gone from the cluster.
	if err := env.Client.CoreV1().ConfigMaps(ns).Delete(ctx, "up-web-fresh", metav1.DeleteOptions{}); err != nil {
		return err
	}
	if res, err = wantStep(w, "upgrade", "up-web", v1); err != nil {
		return fmt.Errorf("%w\n(up-web-fresh, which version 1 drops, was already deleted by hand: deleting what is not there is not an error)", err)
	}
	if err := wantSummary(res.Stdout, summary(upgraded, 8, "color=red size=small"), "upgrade up-web back to version 1 after the failed revision 7"); err != nil {
		return err
	}
	if err := wantData(ctx, env, "up-web-dropped", map[string]string{"k": "v"}, "again: version 1 has it"); err != nil {
		return err
	}
	if err := wantHelmHistory(w, "up-web", [][2]string{
		{"superseded", "Install complete"}, {"superseded", "Upgrade complete"}, {"superseded", "Upgrade complete"},
		{"superseded", "Upgrade complete"}, {"superseded", "Upgrade complete"}, {"superseded", "Upgrade complete"},
		{"failed", `Upgrade "up-web" failed: failed to create resource: *`}, {"deployed", "Upgrade complete"},
	}); err != nil {
		return fmt.Errorf("%w\n(revision 8 supersedes the deployed revision 6; the failed 7 stays failed)", err)
	}

	// An object new to the release, in the way and unowned, refuses the
	// upgrade before anything is recorded or touched.
	if _, err := wantStep(w, "install", "up-own", v1); err != nil {
		return err
	}
	if err := seedConfigMap(ctx, env, "up-own-fresh", nil, nil); err != nil {
		return err
	}
	if res, err = w.run("upgrade", "up-own", v2); err != nil {
		return err
	}
	if err := wantRefusal(res, `UPGRADE FAILED: Unable to continue with update: ConfigMap "up-own-fresh" in namespace "`+ns+`" exists and cannot be imported into the current release: invalid ownership metadata; label validation error: missing key "app.kubernetes.io/managed-by": must be set to "Helm"`, "upgrade up-own to version 2, whose up-own-fresh exists unowned"); err != nil {
		return err
	}
	if err := wantRevisions(ctx, env, "up-own", 1); err != nil {
		return fmt.Errorf("%w: the ownership check refuses before a revision is recorded", err)
	}
	if err := wantData(ctx, env, "up-own-dropped", map[string]string{"k": "v"}, "after the refused upgrade: nothing is touched"); err != nil {
		return err
	}
	return wantData(ctx, env, "up-own-fresh", nil, "after the refused upgrade, as it was seeded: empty")
}

// stageRollback — a rollback is a new revision copying an old one's chart,
// values, notes and manifest, reached with the same three-way patch as an
// upgrade. No revision means the one below the latest, whatever its status;
// a failed rollback is recorded, and supersedes the revision it started from.
func stageRollback(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	ns := env.Namespace
	v1, v2, bad := upCharts(w)
	start := time.Now().Add(-time.Second)
	if _, err := wantStep(w, "install", "--set", "color=blue", "rb", v1); err != nil {
		return err
	}
	if _, err := wantStep(w, "upgrade", "--set", "size=big", "rb", v2); err != nil {
		return err
	}
	if err := editConfigMap(ctx, env, "rb-main", map[string]string{"steady": "edited", "liveadded": "yes"}, nil); err != nil {
		return err
	}
	success := "Rollback was a success! Happy Helming!\n"
	res, err := wantStep(w, "rollback", "rb", "1")
	if err != nil {
		return err
	}
	if err := wantExact(res.Stdout, success, "rollback rb 1"); err != nil {
		return err
	}
	why := "after rollback rb 1: revision 1's manifest and values, patched three ways like an upgrade"
	if err := wantData(ctx, env, "rb-main", map[string]string{"steady": "same", "changing": "old", "color": "blue", "size": "small", "liveadded": "yes"}, why); err != nil {
		return err
	}
	if err := wantGone(ctx, env, "rb-fresh", why+": revision 1 does not have it"); err != nil {
		return err
	}
	dropped, err := env.ConfigMap(ctx, "rb-dropped")
	if err != nil {
		return fmt.Errorf("rb-dropped should be created again %s: %w", why, err)
	}
	if err := wantMarks(dropped, "rb", ns); err != nil {
		return err
	}
	r1, err := wantRecord(ctx, env, "rb", 1, "superseded", start, time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	if _, err := wantRecord(ctx, env, "rb", 2, "superseded", start, time.Now().Add(time.Second)); err != nil {
		return fmt.Errorf("%w (the rollback supersedes the deployed revision)", err)
	}
	r3, err := wantRecord(ctx, env, "rb", 3, "deployed", start, time.Now().Add(time.Second))
	if err != nil {
		return fmt.Errorf("%w (a rollback adds a revision; it never rewinds to an old one)", err)
	}
	for _, c := range []struct {
		path      string
		got, want any
	}{
		{"info.description", dig(r3, "info", "description"), "Rollback to 1"},
		{"info.notes (revision 1's)", dig(r3, "info", "notes"), "color=blue size=small\n"},
		{"info.first_deployed (the install's)", dig(r3, "info", "first_deployed"), dig(r1, "info", "first_deployed")},
		{"chart.metadata.version (revision 1's chart)", dig(r3, "chart", "metadata", "version"), "1.0.0"},
		{"config (revision 1's values)", r3["config"], map[string]any{"color": "blue"}},
		{"manifest (revision 1's)", r3["manifest"], r1["manifest"]},
	} {
		if !reflect.DeepEqual(c.got, c.want) {
			return fmt.Errorf("after rollback rb 1, revision 3's %s should be %s, got %s", c.path, jsonText(c.want), jsonText(c.got))
		}
	}

	// No revision, or 0, is the one below the latest.
	for _, args := range [][]string{{"rollback", "rb"}, {"rollback", "rb", "0"}, {"rollback", "rb", "4"}} {
		if res, err = wantStep(w, args...); err != nil {
			return err
		}
		if err := wantExact(res.Stdout, success, strings.Join(args, " ")); err != nil {
			return err
		}
	}
	r4, err := wantRecord(ctx, env, "rb", 4, "superseded", start, time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	if got, want := dig(r4, "info", "first_deployed"), dig(r1, "info", "first_deployed"); got != want {
		return fmt.Errorf("revision 4, rollback rb to revision 2, should keep the install's info.first_deployed %v, got %v", want, got)
	}
	if err := wantGone(ctx, env, "rb-dropped", "after rollback rb 4: revision 4, a rollback to 2, holds version 2 of the chart"); err != nil {
		return err
	}

	if _, err := wantStep(w, "install", "rb-solo", v1); err != nil {
		return err
	}
	for _, c := range []struct {
		args []string
		text string
	}{
		{[]string{"rollback", "rb", "99"}, "Error: release has no 99 version"},
		{[]string{"rollback", "rb-solo"}, "Error: release has no 0 version"},
		{[]string{"rollback", "rb-nosuch", "1"}, "Error: release: not found"},
		{[]string{"rollback", "rb-nosuch"}, "Error: release: not found"},
		{[]string{"rollback", "rb", "abc"}, `Error: could not convert revision to a number: strconv.Atoi: parsing "abc": invalid syntax`},
	} {
		if res, err = w.run(c.args...); err != nil {
			return err
		}
		if err := wantRefusal(res, c.text, strings.Join(c.args, " ")); err != nil {
			return err
		}
	}
	if err := wantRevisions(ctx, env, "rb", 6); err != nil {
		return fmt.Errorf("%w: a refused rollback records nothing", err)
	}

	// Rolling back from a failed upgrade goes to the revision below it.
	if res, err = w.run("upgrade", "rb", bad); err != nil {
		return err
	}
	if err := wantError(res); err != nil {
		return fmt.Errorf("upgrade rb to a chart with the invalid name rb-Invalid_Name: %w", err)
	}
	if res, err = wantStep(w, "rollback", "rb"); err != nil {
		return err
	}
	// Revision 7's manifest is invalid, so rolling back to it fails.
	if res, err = w.run("rollback", "rb", "7"); err != nil {
		return err
	}
	if err := wantRefusal(res, "Error: failed to create resource: ", "rollback rb 7, whose manifest has the invalid name rb-Invalid_Name"); err != nil {
		return err
	}
	if strings.Contains(res.Stderr, "FAILED") {
		return fmt.Errorf("a failed rollback prints the error alone, with no ROLLBACK FAILED in front, as Helm does; stderr was:\n%s", tail(res.Stderr))
	}
	if err := wantHelmHistory(w, "rb", [][2]string{
		{"superseded", "Install complete"}, {"superseded", "Upgrade complete"}, {"superseded", "Rollback to 1"},
		{"superseded", "Rollback to 2"}, {"superseded", "Rollback to 3"}, {"superseded", "Rollback to 4"},
		{"failed", `Upgrade "rb" failed: failed to create resource: *`}, {"superseded", "Rollback to 6"},
		{"failed", `Rollback "rb" failed: failed to create resource: *`},
	}); err != nil {
		return fmt.Errorf("%w\n(rollback rb after the failed 7 is Rollback to 6; the failed rollback to 7 supersedes 8, leaving nothing deployed)", err)
	}
	res, err = w.helm("get", "values", "rb", "--revision", "8")
	if err != nil {
		return err
	}
	return wantExact(res.Stdout, "USER-SUPPLIED VALUES:\nsize: big\n", "helm get values rb --revision 8, run by the real helm: a rollback to 6 holds revision 6's values, which go back through 4 to revision 2's")
}

// hookCM is a ConfigMap hook that records the revision it was rendered for.
func hookCM(name, events, policy string) string {
	p := ""
	if policy != "" {
		p = "\n    \"helm.sh/hook-delete-policy\": " + policy
	}
	return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-" + name + "\n  annotations:\n    \"helm.sh/hook\": " + events + p + "\ndata:\n  revision: {{ .Release.Revision | quote }}\n"
}

// hookJob is a Job hook in busybox, as small as a pod gets.
func hookJob(name, events, policy, script string) string {
	p := ""
	if policy != "" {
		p = "\n    \"helm.sh/hook-delete-policy\": " + policy
	}
	return `apiVersion: batch/v1
kind: Job
metadata:
  name: {{ .Release.Name }}-` + name + `
  annotations:
    "helm.sh/hook": ` + events + p + `
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      terminationGracePeriodSeconds: 0
      containers:
        - name: c
          image: busybox:1.36
          command: ["sh", "-c", "` + script + `"]
          resources:
            requests: {cpu: 5m, memory: 8Mi}
            limits: {cpu: 10m, memory: 16Mi}
`
}

// hkChart has a ConfigMap hook per install, upgrade and rollback event, a
// pre-install Job that takes a few seconds, a hook deleted once it succeeds,
// one for an event Helm does not know, and, switched on by values, a failing
// Job and a pre-upgrade hook with no before-hook-creation policy.
func hkChart() map[string]string {
	f := map[string]string{
		"Chart.yaml":            "apiVersion: v2\nname: hk\nversion: 1.0.0\n",
		"values.yaml":           "fail: false\nstrict: false\n",
		"templates/main.yaml":   "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-main\ndata:\n  k: v\n",
		"templates/wait.yaml":   "{{- if not .Values.fail }}\n" + hookJob("wait", "pre-install", "", "sleep 2") + "{{- end }}\n",
		"templates/gone.yaml":   hookCM("gone", "post-install", "hook-succeeded"),
		"templates/bogus.yaml":  hookCM("bogus", "pre-foo", ""),
		"templates/fail.yaml":   "{{- if .Values.fail }}\n" + hookJob("fail", "pre-install", "hook-failed", "exit 3") + "{{- end }}\n",
		"templates/strict.yaml": "{{- if .Values.strict }}\n" + hookCM("strict", "pre-upgrade", "hook-succeeded") + "{{- end }}\n",
	}
	for _, e := range []string{"pre-install", "post-install", "pre-upgrade", "post-upgrade", "pre-rollback", "post-rollback"} {
		f["templates/cm-"+e+".yaml"] = hookCM(e, e, "")
	}
	return f
}

// wantHookRevision checks a hook ConfigMap: there, carrying the revision it
// was rendered for, and without the marks of a release's own objects.
func wantHookRevision(ctx context.Context, env *kube.Env, name, rev, why string) error {
	cm, err := env.ConfigMap(ctx, name)
	if err != nil {
		return fmt.Errorf("hook ConfigMap %s should exist %s: %w", name, why, err)
	}
	if !reflect.DeepEqual(cm.Data, map[string]string{"revision": rev}) {
		return fmt.Errorf("hook ConfigMap %s should hold revision=%q %s, got %v", name, rev, why, cm.Data)
	}
	_, l := cm.Labels["app.kubernetes.io/managed-by"]
	_, a := cm.Annotations["meta.helm.sh/release-name"]
	if l || a {
		return fmt.Errorf("hook ConfigMap %s carries the release's ownership marks; Helm creates hooks without them (the release does not own its hooks). Labels %v, annotations %v", name, cm.Labels, cm.Annotations)
	}
	return nil
}

// stageHooks — hooks stay out of the manifest and are recorded with the
// release; they run around install, upgrade and rollback, unmarked and
// unchecked for ownership, each waited on until ready, deleted as their
// policy says, and a rollback replays the target revision's stored hooks.
func stageHooks(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	ns := env.Namespace
	jobs := env.Client.BatchV1().Jobs(ns)
	defer func() {
		bg := metav1.DeletePropagationBackground
		for _, j := range []string{"hk-web-wait", "hk-fail-fail"} {
			jobs.Delete(context.WithoutCancel(ctx), j, metav1.DeleteOptions{PropagationPolicy: &bg})
		}
	}()
	files := hkChart()
	hk := w.chart("hk", files)

	// A hook skips the ownership check, and before-hook-creation, the
	// default policy, replaces whatever has its name.
	if err := seedConfigMap(ctx, env, "hk-web-pre-install", nil, nil); err != nil {
		return err
	}
	res, err := wantStep(w, "install", "hk-web", hk)
	if err != nil {
		return err
	}
	if err := wantSummary(res.Stdout, "NAME: hk-web\nLAST DEPLOYED: *\nNAMESPACE: "+ns+"\nSTATUS: deployed\nREVISION: 1\n", "install hk-web"); err != nil {
		return err
	}
	job, err := jobs.Get(ctx, "hk-web-wait", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("install hk-web should leave the pre-install Job hk-web-wait (its default policy deletes it only before the next creation): %w", err)
	}
	if job.Status.Succeeded != 1 || job.Status.CompletionTime == nil {
		return fmt.Errorf("install hk-web returned before its pre-install Job hk-web-wait completed: a Job hook is waited on until it completes. Job status: %+v", job.Status)
	}
	main, err := env.ConfigMap(ctx, "hk-web-main")
	if err != nil {
		return fmt.Errorf("install hk-web should create hk-web-main: %w", err)
	}
	if main.CreationTimestamp.Before(job.Status.CompletionTime) {
		return fmt.Errorf("hk-web-main was created at %s, before the pre-install Job hk-web-wait completed at %s: pre-install hooks finish before the release's objects are created", main.CreationTimestamp.UTC(), job.Status.CompletionTime.UTC())
	}
	for _, h := range []string{"pre-install", "post-install"} {
		if err := wantHookRevision(ctx, env, "hk-web-"+h, "1", "after install"); err != nil {
			return err
		}
	}
	for _, c := range []struct{ name, why string }{
		{"hk-web-gone", "after install: its policy hook-succeeded deletes it once its event's hooks succeed"},
		{"hk-web-bogus", "at all: its hook event pre-foo is unknown, and Helm drops such a document, neither hook nor object"},
		{"hk-web-pre-upgrade", "after install: it is an upgrade hook"},
		{"hk-web-pre-rollback", "after install: it is a rollback hook"},
	} {
		if err := wantGone(ctx, env, c.name, c.why); err != nil {
			return err
		}
	}
	rec, err := wantRecord(ctx, env, "hk-web", 1, "deployed", time.Now().Add(-time.Minute), time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	if m := fmt.Sprint(rec["manifest"]); strings.Contains(m, "helm.sh/hook") || !strings.Contains(m, "name: hk-web-main") {
		return fmt.Errorf("the record's manifest should hold hk-web-main and none of the hooks, got:\n%s", indent(m))
	}
	render := strings.NewReplacer(`{{ .Release.Name }}`, "hk-web", `{{ .Release.Revision | quote }}`, `"1"`)
	type hookWant struct {
		file, kind string
		events     []any
		policies   any
		phase      string
	}
	ran := []any{"before-hook-creation"}
	// Stored order: by kind as Helm installs them, then by path.
	want := []hookWant{
		{"cm-post-install", "ConfigMap", []any{"post-install"}, ran, "Succeeded"},
		{"cm-post-rollback", "ConfigMap", []any{"post-rollback"}, nil, ""},
		{"cm-post-upgrade", "ConfigMap", []any{"post-upgrade"}, nil, ""},
		{"cm-pre-install", "ConfigMap", []any{"pre-install"}, ran, "Succeeded"},
		{"cm-pre-rollback", "ConfigMap", []any{"pre-rollback"}, nil, ""},
		{"cm-pre-upgrade", "ConfigMap", []any{"pre-upgrade"}, nil, ""},
		{"gone", "ConfigMap", []any{"post-install"}, []any{"hook-succeeded"}, "Succeeded"},
		{"wait", "Job", []any{"pre-install"}, ran, "Succeeded"},
	}
	hooks, _ := rec["hooks"].([]any)
	var got, exp []string
	for _, h := range hooks {
		got = append(got, jsonText(map[string]any{"name": dig(h, "name"), "kind": dig(h, "kind"), "path": dig(h, "path"), "events": dig(h, "events"),
			"delete_policies": dig(h, "delete_policies"), "phase": dig(h, "last_run", "phase"), "weight": dig(h, "weight")}))
	}
	for _, h := range want {
		exp = append(exp, jsonText(map[string]any{"name": "hk-web-" + strings.TrimPrefix(h.file, "cm-"), "kind": h.kind, "path": "hk/templates/" + h.file + ".yaml", "events": h.events,
			"delete_policies": h.policies, "phase": h.phase, "weight": nil}))
	}
	if strings.Join(got, "\n") != strings.Join(exp, "\n") {
		return fmt.Errorf("revision 1's hooks should be, in this order (kind, then path), with a hook that ran given the default delete policy:\n  %s\ngot:\n  %s", strings.Join(exp, "\n  "), strings.Join(got, "\n  "))
	}
	for i, h := range want {
		body := files["templates/"+h.file+".yaml"]
		if h.kind == "Job" {
			body = strings.TrimSuffix(strings.TrimPrefix(body, "{{- if not .Values.fail }}\n"), "{{- end }}\n")
		}
		if m := dig(hooks[i], "manifest"); m != strings.TrimSpace(render.Replace(body)) {
			return fmt.Errorf("hook %s's manifest should be the rendered document alone, no --- or # Source line:\n%s\ngot:\n%s", h.file, indent(strings.TrimSpace(render.Replace(body))), indent(fmt.Sprint(m)))
		}
		started, _ := dig(hooks[i], "last_run", "started_at").(string)
		if _, err := time.Parse(time.RFC3339Nano, started); (err == nil) != (h.phase != "") {
			return fmt.Errorf("hook %s's last_run.started_at should be the time it ran, or \"\" when it never has, got %q", h.file, started)
		}
	}
	if res, err = w.helm("status", "hk-web"); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("helm status hk-web, run by the real helm against your record with hooks: %w", err)
	}

	// Upgrade hooks are rendered for their revision; before-hook-creation
	// clears the last one's.
	for _, rev := range []string{"2", "3"} {
		if _, err := wantStep(w, "upgrade", "hk-web", hk); err != nil {
			return err
		}
		for _, h := range []string{"pre-upgrade", "post-upgrade"} {
			if err := wantHookRevision(ctx, env, "hk-web-"+h, rev, "after the upgrade to revision "+rev); err != nil {
				return err
			}
		}
	}
	if err := wantHookRevision(ctx, env, "hk-web-pre-install", "1", "after the upgrades: install hooks do not run on upgrade"); err != nil {
		return err
	}
	if _, err := wantStep(w, "rollback", "hk-web", "1"); err != nil {
		return err
	}
	for _, h := range []string{"pre-rollback", "post-rollback"} {
		if err := wantHookRevision(ctx, env, "hk-web-"+h, "1", "after rollback hk-web 1: a rollback runs the hooks stored with revision 1, not a fresh render for revision 4"); err != nil {
			return err
		}
	}

	// A hook with no before-hook-creation policy fails on an object in its way.
	if err := seedConfigMap(ctx, env, "hk-web-strict", nil, nil); err != nil {
		return err
	}
	if res, err = w.run("upgrade", "--set", "strict=true", "hk-web", hk); err != nil {
		return err
	}
	for _, s := range []string{"Error: UPGRADE FAILED: pre-upgrade hooks failed: warning: Hook pre-upgrade hk/templates/strict.yaml failed: ", `configmaps "hk-web-strict" already exists`} {
		if err := wantRefusal(res, s, "upgrade hk-web with its hook hk-web-strict (policy hook-succeeded only) already in the cluster"); err != nil {
			return err
		}
	}
	rec, err = wantRecord(ctx, env, "hk-web", 5, "failed", time.Now().Add(-time.Minute), time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	if d, _ := dig(rec, "info", "description").(string); !strings.HasPrefix(d, `Upgrade "hk-web" failed: pre-upgrade hooks failed: `) {
		return fmt.Errorf(`the failed upgrade's description should start Upgrade "hk-web" failed: pre-upgrade hooks failed: , got %q`, d)
	}
	if _, err := wantRecord(ctx, env, "hk-web", 4, "deployed", time.Now().Add(-time.Minute), time.Now().Add(time.Second)); err != nil {
		return err
	}

	// A failing pre-install Job fails the install before any object exists.
	if res, err = w.run("install", "--set", "fail=true", "hk-fail", hk); err != nil {
		return err
	}
	for _, s := range []string{"Error: INSTALLATION FAILED: failed pre-install: ", "job hk-fail-fail failed: BackoffLimitExceeded"} {
		if err := wantRefusal(res, s, "install hk-fail, whose pre-install Job exits 3 with backoffLimit 0"); err != nil {
			return err
		}
	}
	rec, err = wantRecord(ctx, env, "hk-fail", 1, "failed", time.Now().Add(-time.Minute), time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	if d, _ := dig(rec, "info", "description").(string); !strings.HasPrefix(d, `Release "hk-fail" failed: failed pre-install: `) {
		return fmt.Errorf(`the failed install's description should start Release "hk-fail" failed: failed pre-install: , got %q`, d)
	}
	if err := wantGone(ctx, env, "hk-fail-main", "after its pre-install hook failed: the release's objects come after the pre-install hooks"); err != nil {
		return err
	}
	if _, err := jobs.Get(ctx, "hk-fail-fail", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return fmt.Errorf("Job hk-fail-fail should be deleted once it failed, as its policy hook-failed says (err: %v)", err)
	}
	for _, h := range rec["hooks"].([]any) {
		if dig(h, "name") == "hk-fail-fail" && dig(h, "last_run", "phase") != "Failed" {
			return fmt.Errorf("the failed Job hook's last_run.phase should be Failed, got %v", dig(h, "last_run", "phase"))
		}
	}
	return nil
}

// hwHooks are the hw chart's hooks in template-path order: path, kind, name
// and the raw hook-weight annotation ("" for none).
var hwHooks = [][4]string{
	{"01-z-cm", "ConfigMap", "a-cm", `"0"`},
	{"02-y-sec", "Secret", "b-sec", `"0"`},
	{"03-x-sa", "ServiceAccount", "c-sa", ""},
	{"04-cm-zz", "ConfigMap", "zz", ""},
	{"05-cm-aa", "ConfigMap", "aa", ""},
	{"06-neg", "ConfigMap", "neg-ten", `"-10"`},
	{"07-neg5", "ConfigMap", "neg-five", `"-5"`},
	{"08-pos", "ConfigMap", "pos-ten", `"10"`},
	{"09-two", "ConfigMap", "pos-two", `"2"`},
	{"10-str", "ConfigMap", "str-abc", `"abc"`},
	{"11-plus", "ConfigMap", "str-plus3", `"+3"`},
	{"12-space", "ConfigMap", "str-space-1", `" 1"`},
	{"13-float", "ConfigMap", "str-float", `"1.5"`},
	{"14-big", "ConfigMap", "pos-hundred", `"100"`},
	{"15-sec-zz", "Secret", "zz", `"0"`},
	{"16-sa-a", "ServiceAccount", "aa", `"0"`},
	{"16b-intweight", "ConfigMap", "int-weight", `"3"`},
	{"17-post-a", "ConfigMap", "post-a", `"1"`},
	{"18-post-b", "ConfigMap", "post-b", `"-1"`},
}

// hwChart has the hooks above, all pre-install but the two post- ones, and
// one object of its own.
func hwChart() map[string]string {
	f := map[string]string{
		"Chart.yaml":          "apiVersion: v2\nname: hw\nversion: 1.0.0\n",
		"templates/main.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-main\n",
	}
	for _, h := range hwHooks {
		event := "pre-install"
		if strings.HasPrefix(h[2], "post-") {
			event = "post-install"
		}
		body := "apiVersion: v1\nkind: " + h[1] + "\nmetadata:\n  name: {{ .Release.Name }}-" + h[2] + "\n  annotations:\n    \"helm.sh/hook\": " + event + "\n"
		if h[3] != "" {
			body += "    \"helm.sh/hook-weight\": " + h[3] + "\n"
		}
		if h[1] == "ConfigMap" {
			body += "data:\n  k: v\n"
		}
		f["templates/"+h[0]+".yaml"] = body
	}
	return f
}

// creationOrder lists the stage namespace's ConfigMaps, Secrets and
// ServiceAccounts whose names start with prefix, as Kind/name in the order
// they were created: resourceVersion is one counter for the whole cluster.
func creationOrder(ctx context.Context, env *kube.Env, prefix string) ([]string, error) {
	type item struct {
		id string
		rv int64
	}
	var items []item
	add := func(kind string, m metav1.ObjectMeta) {
		if strings.HasPrefix(m.Name, prefix) {
			rv, _ := strconv.ParseInt(m.ResourceVersion, 10, 64)
			items = append(items, item{kind + "/" + m.Name, rv})
		}
	}
	core := env.Client.CoreV1()
	cms, err := core.ConfigMaps(env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, o := range cms.Items {
		add("ConfigMap", o.ObjectMeta)
	}
	secs, err := core.Secrets(env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, o := range secs.Items {
		add("Secret", o.ObjectMeta)
	}
	sas, err := core.ServiceAccounts(env.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, o := range sas.Items {
		add("ServiceAccount", o.ObjectMeta)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].rv < items[j].rv })
	var out []string
	for _, it := range items {
		out = append(out, it.id)
	}
	return out, nil
}

// stageHookWeights — an event's hooks run by weight, an integer or 0, then by
// name, then by kind in install order; they are stored, printed by get hooks
// and template, in kind-then-path order; and an unquoted weight, which is not
// a string, fails the install as it does for Helm.
func stageHookWeights(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	hw := w.chart("hw", hwChart())
	if _, err := wantStep(w, "install", "hw", hw); err != nil {
		return err
	}
	got, err := creationOrder(ctx, env, "hw-")
	if err != nil {
		return err
	}
	var want []string
	for _, id := range []string{
		"ConfigMap/neg-ten", "ConfigMap/neg-five",
		"ConfigMap/a-cm", "ServiceAccount/aa", "ConfigMap/aa", "Secret/b-sec", "ServiceAccount/c-sa",
		"ConfigMap/str-abc", "ConfigMap/str-float", "ConfigMap/str-space-1", "Secret/zz", "ConfigMap/zz",
		"ConfigMap/pos-two", "ConfigMap/int-weight", "ConfigMap/str-plus3", "ConfigMap/pos-ten", "ConfigMap/pos-hundred",
		"ConfigMap/main",
		"ConfigMap/post-b", "ConfigMap/post-a",
	} {
		k, n, _ := strings.Cut(id, "/")
		want = append(want, k+"/hw-"+n)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		return fmt.Errorf("install hw should create its hooks lowest weight first (a weight that is not an integer is 0, \"+3\" is 3), then by name, then by kind in install order; the release's own objects between the pre- and post-install hooks. Expected this creation order:\n  %s\ngot:\n  %s", strings.Join(want, "\n  "), strings.Join(got, "\n  "))
	}

	rec, err := wantRecord(ctx, env, "hw", 1, "deployed", time.Now().Add(-time.Minute), time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	weights := map[string]any{"neg-ten": -10.0, "neg-five": -5.0, "pos-ten": 10.0, "pos-two": 2.0, "str-plus3": 3.0, "pos-hundred": 100.0, "int-weight": 3.0, "post-a": 1.0, "post-b": -1.0}
	hooks, _ := rec["hooks"].([]any)
	for _, h := range hooks {
		name := strings.TrimPrefix(fmt.Sprint(dig(h, "name")), "hw-")
		if g := dig(h, "weight"); !reflect.DeepEqual(g, weights[name]) {
			return fmt.Errorf("hook hw-%s's recorded weight should be %v (left out when 0, which an unparseable weight is), got %v", name, weights[name], g)
		}
	}
	for _, args := range [][]string{
		{"get", "hooks", "hw"},
		{"get", "hooks", "--revision", "1", "hw"},
		{"template", "hw", hw},
	} {
		if err := sameAsHelm(w, args, nil); err != nil {
			return err
		}
	}

	hwint := w.chart("hwint", map[string]string{
		"Chart.yaml":       "apiVersion: v2\nname: hwint\nversion: 1.0.0\n",
		"templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: hwint-x\n  annotations:\n    helm.sh/hook: pre-install\n    helm.sh/hook-weight: 3\ndata: {k: v}\n",
	})
	res, err := w.run("install", "hwint", hwint)
	if err != nil {
		return err
	}
	if err := wantRefusal(res, `Error: INSTALLATION FAILED: failed pre-install: unable to build kubernetes object for deleting hook hwint/templates/x.yaml: unable to decode "": json: cannot unmarshal number into Go struct field ObjectMeta.metadata.annotations of type string`, "install hwint, whose hook-weight is the YAML number 3, not a string"); err != nil {
		return err
	}
	if _, err := wantRecord(ctx, env, "hwint", 1, "failed", time.Now().Add(-time.Minute), time.Now().Add(time.Second)); err != nil {
		return err
	}
	return wantGone(ctx, env, "hwint-x", "after install hwint failed building its hook")
}

// testPod is a Pod test hook in busybox running script.
func testPod(name, event, weight, script string) string {
	wt := ""
	if weight != "" {
		wt = "\n    \"helm.sh/hook-weight\": " + weight
	}
	return `apiVersion: v1
kind: Pod
metadata:
  name: "{{ .Release.Name }}-` + name + `"
  annotations:
    "helm.sh/hook": ` + event + wt + `
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  containers:
    - name: c
      image: busybox:1.36
      command: ["sh", "-c", "` + script + `"]
      resources:
        requests: {cpu: 5m, memory: 8Mi}
        limits: {cpu: 10m, memory: 16Mi}
`
}

// tstChart has four Pod test hooks, one spelled the Helm 2 way and one
// weighted first, a ConfigMap test hook, and, with fail=true, a failing Pod.
func tstChart() map[string]string {
	return map[string]string{
		"Chart.yaml":                      "apiVersion: v2\nname: tst\nversion: 1.0.0\n",
		"values.yaml":                     "fail: false\n",
		"templates/NOTES.txt":             "Notes for {{ .Release.Name }}.\n",
		"templates/main.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-main\ndata: {k: v}\n",
		"templates/tests/a-ok.yaml":       testPod("ok", "test", "", "echo ok-says-hello; echo second line"),
		"templates/tests/b-legacy.yaml":   testPod("legacy", "test-success", "", "echo legacy ran"),
		"templates/tests/c-fail.yaml":     "{{- if .Values.fail }}\n" + testPod("bad", "test", "", "echo bad-says-bye >&2; exit 2") + "{{- end }}\n",
		"templates/tests/d-weighted.yaml": testPod("weighted", "test", `"-1"`, "echo weighted"),
		"templates/tests/e-cm.yaml":       "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: \"{{ .Release.Name }}-test-cm\"\n  annotations:\n    \"helm.sh/hook\": test\ndata: {k: v}\n",
	}
}

// testRuns is each recorded hook's name with its last_run, in stored order.
func testRuns(rec map[string]any) [][2]string {
	var out [][2]string
	hooks, _ := rec["hooks"].([]any)
	for _, h := range hooks {
		out = append(out, [2]string{fmt.Sprint(dig(h, "name")), jsonText(dig(h, "last_run"))})
	}
	return out
}

// wantTestOutput wants a test run's stdout to be the status block the real
// helm prints for the record it left, plus tail.
func wantTestOutput(w *work, res *runner.Result, tail, what string) error {
	st, err := w.helm("status", "tst")
	if err != nil {
		return err
	}
	if err := wantExit(st, 0); err != nil {
		return fmt.Errorf("helm status tst, by the real helm after %s: %w", what, err)
	}
	return wantExact(res.Stdout, st.Stdout+tail, what+": the release as helm status prints it once the tests have run")
}

// stageTest — install leaves test hooks alone; test runs them in hook order
// into the same revision, stops at the first failure, honours --filter and
// --logs as helm does, and status shows the runs, in stored order.
func stageTest(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	ns := env.Namespace
	pods := env.Client.CoreV1().Pods(ns)
	tst := w.chart("tst", tstChart())
	res, err := wantStep(w, "install", "tst", tst)
	if err != nil {
		return err
	}
	if err := wantSummary(res.Stdout, "NAME: tst\nLAST DEPLOYED: *\nNAMESPACE: "+ns+"\nSTATUS: deployed\nREVISION: 1\nNOTES:\nNotes for tst.\n", "install tst, whose test hooks have never run (no TEST SUITE line: None is for a release without tests)"); err != nil {
		return err
	}
	if _, err := pods.Get(ctx, "tst-ok", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return fmt.Errorf("install tst should not create its test Pod tst-ok: test hooks run only on test (err: %v)", err)
	}
	if err := sameAsHelm(w, []string{"status", "tst"}, nil); err != nil {
		return err
	}

	test := func(args ...string) (*runner.Result, error) {
		return w.run(append(append([]string{"test", "--timeout", "120s"}, args...), "tst")...)
	}
	if res, err = test(); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("test tst: %w", err)
	}
	if err := wantTestOutput(w, res, "", "test tst"); err != nil {
		return err
	}
	if err := wantRevisions(ctx, env, "tst", 1); err != nil {
		return fmt.Errorf("test tst records the runs into revision 1, adding none: %w", err)
	}
	rec, err := wantRecord(ctx, env, "tst", 1, "deployed", time.Now().Add(-5*time.Minute), time.Now().Add(time.Second))
	if err != nil {
		return err
	}
	start := map[string]time.Time{}
	for _, h := range rec["hooks"].([]any) {
		n := fmt.Sprint(dig(h, "name"))
		t, err := time.Parse(time.RFC3339Nano, fmt.Sprint(dig(h, "last_run", "started_at")))
		if err != nil || dig(h, "last_run", "phase") != "Succeeded" {
			return fmt.Errorf("after test tst, hook %s's last_run should be a Succeeded run with its times, got %s", n, jsonText(dig(h, "last_run")))
		}
		start[n] = t
		if !reflect.DeepEqual(dig(h, "delete_policies"), []any{"before-hook-creation"}) || !reflect.DeepEqual(dig(h, "events"), []any{"test"}) {
			return fmt.Errorf("after test tst, hook %s should be recorded with events [test] (test-success is the same event) and the default delete policy it ran with, got events %v, delete_policies %v", n, dig(h, "events"), dig(h, "delete_policies"))
		}
	}
	order := []string{"tst-weighted", "tst-legacy", "tst-ok", "tst-test-cm"}
	for i := 1; i < len(order); i++ {
		if start[order[i]].Before(start[order[i-1]]) {
			return fmt.Errorf("test tst should run its hooks by weight, then name: %s, recorded as started at %s, ran before %s, started at %s", order[i], start[order[i]].Format(time.StampMicro), order[i-1], start[order[i-1]].Format(time.StampMicro))
		}
	}
	ok1, err := pods.Get(ctx, "tst-ok", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("test tst should leave its test Pod tst-ok: the default policy deletes a hook only before it is created again: %w", err)
	}

	// A second run replaces the Pods; --filter runs one and moves the
	// others, which keep their runs, to the front.
	if res, err = test(); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("test tst, a second time: %w", err)
	}
	ok2, err := pods.Get(ctx, "tst-ok", metav1.GetOptions{})
	if err != nil || ok2.UID == ok1.UID {
		return fmt.Errorf("a second test tst should delete test Pod tst-ok and create it again (before-hook-creation), err %v", err)
	}
	if rec, err = wantRecord(ctx, env, "tst", 1, "deployed", time.Now().Add(-5*time.Minute), time.Now().Add(time.Second)); err != nil {
		return err
	}
	before := map[string]string{}
	for _, r := range testRuns(rec) {
		before[r[0]] = r[1]
	}
	if res, err = test("--filter", "name=tst-ok"); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("test --filter name=tst-ok tst: %w", err)
	}
	if err := wantTestOutput(w, res, "", "test --filter name=tst-ok tst"); err != nil {
		return err
	}
	if rec, err = wantRecord(ctx, env, "tst", 1, "deployed", time.Now().Add(-5*time.Minute), time.Now().Add(time.Second)); err != nil {
		return err
	}
	var names []string
	for _, r := range testRuns(rec) {
		names = append(names, r[0])
		if (r[0] == "tst-ok") == (r[1] == before[r[0]]) {
			return fmt.Errorf("test --filter name=tst-ok tst should run tst-ok alone and leave the other hooks' last_run as it was; hook %s went from %s to %s", r[0], before[r[0]], r[1])
		}
	}
	if want := []string{"tst-test-cm", "tst-legacy", "tst-weighted", "tst-ok"}; !reflect.DeepEqual(names, want) {
		return fmt.Errorf("after test --filter name=tst-ok tst, the record's hooks should be those the filter skipped, in their order, then those it ran: %v, got %v", want, names)
	}

	// The upgrade's tests stop at the first failure; --logs walks every
	// test hook by weight, run this time or not, and the ConfigMap has none.
	if _, err := wantStep(w, "upgrade", "--set", "fail=true", "tst", tst); err != nil {
		return err
	}
	if res, err = test(); err != nil {
		return err
	}
	if err := wantRefusal(res, "Error: 1 error occurred:\n\t* pod tst-bad failed\n", "test tst at revision 2, whose test Pod tst-bad exits 2"); err != nil {
		return err
	}
	if err := wantTestOutput(w, res, "", "test tst at revision 2"); err != nil {
		return err
	}
	if rec, err = wantRecord(ctx, env, "tst", 2, "deployed", time.Now().Add(-5*time.Minute), time.Now().Add(time.Second)); err != nil {
		return err
	}
	for _, r := range testRuns(rec) {
		ran := r[1] != `{"completed_at":"","phase":"","started_at":""}`
		if ran != (r[0] == "tst-weighted" || r[0] == "tst-bad") {
			return fmt.Errorf("test tst at revision 2 should run tst-weighted, then tst-bad, which fails, and stop there; hook %s's last_run is %s", r[0], r[1])
		}
	}
	if res, err = test("--logs"); err != nil {
		return err
	}
	if err := wantRefusal(res, `unable to get pod logs for tst-test-cm: pods "tst-test-cm" not found`, "test --logs tst, whose ConfigMap test hook tst-test-cm has no pod logs"); err != nil {
		return err
	}
	logs := "\nPOD LOGS: tst-weighted\nweighted\n\nPOD LOGS: tst-bad\nbad-says-bye\n\nPOD LOGS: tst-legacy\nlegacy ran\n\nPOD LOGS: tst-ok\nok-says-hello\nsecond line\n\n"
	if err := wantTestOutput(w, res, logs, "test --logs tst"); err != nil {
		return err
	}
	if res, err = w.run("test", "nosuch"); err != nil {
		return err
	}
	return wantRefusal(res, "Error: release: not found", "test nosuch, a release that does not exist")
}
