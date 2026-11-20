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
	"strings"

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
		"templates/configmap.yaml",
		"templates/sub/deeper.yaml",
	}
	if err := wantLines(res.Stdout, want); err != nil {
		return err
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
		if res.ExitCode == 0 {
			return fmt.Errorf("%s: %s, so this is not a chart Helm would load, but the program exited 0 with:\n%s",
				bad.name, bad.why, tail(res.Stdout))
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

func wantLines(stdout string, want []string) error {
	if got := lines(stdout); strings.Join(got, "\n") != strings.Join(want, "\n") {
		return fmt.Errorf("expected exactly these lines, in this order:\n  %s\ngot:\n%s",
			strings.Join(want, "\n  "), tail(stdout))
	}
	return nil
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
