// Package helm holds the assertions for the "Build your own Helm" course —
// one function per stage, registered by slug.
//
// Every stage writes a chart to a temporary directory, runs the learner's
// built binary on it, and judges exit code, stdout and, from install onwards,
// what the cluster holds. Flags always come before positional arguments, as
// both Go's flag package and the real helm accept.
package helm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

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
	register(Stage{Slug: "chart-load", Run: stageChartLoad})
	register(Stage{Slug: "values-merge", Run: stageValuesMerge})
	register(Stage{Slug: "template-engine", Run: stageTemplateEngine})
	register(Stage{Slug: "builtin-objects", Run: stageBuiltinObjects})
	register(Stage{Slug: "template-functions", Run: stageTemplateFunctions})
	register(Stage{Slug: "named-templates", Run: stageNamedTemplates})
	register(Stage{Slug: "subcharts", Run: stageSubcharts})
	register(Stage{Slug: "conditions", Run: stageConditions})
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
		if res.ExitCode == 0 {
			return fmt.Errorf("required %q was given %s, which must fail the render, but the program exited 0 with:\n%s", req.msg, req.why, tail(res.Stdout))
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
		if res.ExitCode == 0 {
			return fmt.Errorf("Helm removes %s from sprig's functions, so a chart cannot read the environment of whoever installs it; a template calling it must not render, but the program exited 0 with:\n%s", name, tail(res.Stdout))
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
---
# Source: demo/templates/other.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-demo-other
`, "release web: named templates from _helpers.tpl and other.yaml, called with include, template and tpl"); err != nil {
		return err
	}

	files["templates/missing.yaml"] = `name: {{ include "demo.nope" . }}` + "\n"
	if res, err = w.run("template", "web", w.chart("missing", files)); err != nil {
		return err
	}
	if res.ExitCode == 0 {
		return fmt.Errorf("templates/missing.yaml includes \"demo.nope\", which no file defines, so the render must fail, but the program exited 0 with:\n%s", tail(res.Stdout))
	}
	if !strings.Contains(res.Stderr, "demo.nope") {
		return fmt.Errorf("an include of an undefined template failed, but the error should name it, \"demo.nope\"; stderr was:\n%s", tail(res.Stderr))
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
// defines and globals, and cache, with no values.yaml at all.
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
  parentDb: '{"extra":{"added":true,"kept":true},"global":{"env":"prod","region":"eu","tier":"data"},"name":"orders","port":6543}'
  parentCache: '{"enabled":true,"global":{"env":"prod","region":"eu"}}'
`, `release web of shop, whose charts/ holds db and cache: shop's values under
db: override db's defaults, shop's global wins over db's, and the merged db
values are what shop's own templates see under .Values.db`); err != nil {
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
`
	files["values.yaml"] = `global:
  env: prod
db:
  port: 6543
cache:
  enabled: "yes"
tags:
  backend: false
`
	files["templates/app.yaml"] = `apiVersion: v1
kind: ConfigMap
metadata:
  name: app
data:
  db: {{ toJson .Values.db | squote }}
`
	for _, sub := range []string{"queue", "metrics"} {
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
		{nil, src("cache", "db", "queue"),
			`cache.enabled is "yes", not a boolean, so it is skipped and cache stays on; queue's own default queue.enabled: true beats tags.backend: false; metrics has only tags, and backend is false`},
		{[]string{"db.enabled=false"}, src("cache", "queue"), "db.enabled is false"},
		{[]string{"tags.ops=true"}, src("cache", "db", "metrics", "queue"), "metrics is tagged backend and ops, and one true tag is enough"},
		{[]string{"tags.backend=true", "queue.enabled=false"}, src("cache", "db", "metrics"), "queue.enabled: false is a condition, and a condition beats tags"},
		{[]string{"global.cache=false"}, src("db", "queue"), "cache's condition is cache.enabled,global.cache: the first is not a boolean, the second is false"},
		{[]string{"cache.enabled=false", "tags.frontend=true"}, src("db", "queue"), "cache.enabled: false decides before the frontend tag is looked at"},
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
