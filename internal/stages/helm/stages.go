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
`
	dir := w.chart("demo", map[string]string{
		"Chart.yaml":              chartYAML,
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

	want := func(release, ns string) string {
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
  replicas: "2"
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
	for _, c := range []struct{ flag, release, ns string }{
		{"-n", "web", "team-a"},
		{"--namespace", "api", "team-b"},
	} {
		res, err := w.run("template", c.flag, c.ns, c.release, dir)
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return err
		}
		if err := wantExact(res.Stdout, want(c.release, c.ns), fmt.Sprintf("release %s in namespace %s (%s)", c.release, c.ns, c.flag)); err != nil {
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

	files["templates/req.yaml"] = `tag: {{ required "image.tag is required" .Values.image.tag }}` + "\n"
	res, err = w.run("template", "demo", w.chart("required", files))
	if err != nil {
		return err
	}
	if res.ExitCode == 0 {
		return fmt.Errorf("required \"image.tag is required\" was given an empty string, which must fail the render, but the program exited 0 with:\n%s", tail(res.Stdout))
	}
	if !strings.Contains(res.Stderr, "image.tag is required") {
		return fmt.Errorf("required failed the render, but the error should carry its message, \"image.tag is required\"; stderr was:\n%s", tail(res.Stderr))
	}
	if res, err = w.run("template", "--set", "image.tag=1.0", "demo", w.chart("required", files)); err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("with --set image.tag=1.0, required has its value: %w", err)
	}
	if !strings.Contains(res.Stdout, "tag: 1.0") {
		return fmt.Errorf("required returns the value it checks, so req.yaml should render \"tag: 1.0\"; got:\n%s", tail(res.Stdout))
	}
	delete(files, "templates/req.yaml")

	files["templates/env.yaml"] = `home: {{ env "HOME" }}` + "\n"
	if res, err = w.run("template", "demo", w.chart("env", files)); err != nil {
		return err
	}
	if res.ExitCode == 0 {
		return fmt.Errorf("Helm removes env from sprig's functions, so a chart cannot read the environment of whoever installs it; a template calling env must not render, but the program exited 0 with:\n%s", tail(res.Stdout))
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
