// Your Helm.
//
// Stage 1: `chart DIR` loads a chart directory, validates Chart.yaml, and
// prints the chart's name and version followed by its template files.
//
// Stage 2: `values [-f FILE]... [--set k=v]... DIR` prints the chart's values
// merged with the user's, as YAML.
//
// Stage 3: `template [-f FILE]... [--set k=v]... NAME DIR` renders every
// template with the merged values and prints the manifests as helm template
// does.
//
// Stage 4: templates see .Release, .Chart, .Template and .Files; `-n` and
// `--namespace` name the release's namespace.
//
// Stage 5: templates call Helm's functions: sprig's, less env and expandenv,
// plus toYaml and required.

package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/Masterminds/semver/v3"
	"github.com/Masterminds/sprig/v3"
	"sigs.k8s.io/yaml"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app <chart> [flags] ARGS")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

// multi is a flag that may be repeated, keeping every value in order.
type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func run(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	var files, sets multi
	var namespace string
	fs.StringVar(&namespace, "n", "default", "release namespace")
	fs.StringVar(&namespace, "namespace", "default", "release namespace")
	fs.Var(&files, "f", "values file (repeatable)")
	fs.Var(&sets, "set", "key=value (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	args = fs.Args()

	switch cmd {
	case "chart":
		if len(args) != 1 {
			return fmt.Errorf("usage: chart DIR")
		}
		c, err := load(args[0])
		if err != nil {
			return err
		}
		fmt.Println(c.Metadata.Name, c.Metadata.Version)
		for _, t := range c.Templates {
			fmt.Println(t.Name)
		}
		return nil
	case "values":
		if len(args) != 1 {
			return fmt.Errorf("usage: values [-f FILE]... [--set k=v]... DIR")
		}
		c, err := load(args[0])
		if err != nil {
			return err
		}
		user, err := userValues(files, sets)
		if err != nil {
			return err
		}
		out, err := yaml.Marshal(coalesce(copyMap(c.Values), user))
		if err != nil {
			return err
		}
		fmt.Print(string(out))
		return nil
	case "template":
		if len(args) != 2 {
			return fmt.Errorf("usage: template [-f FILE]... [--set k=v]... NAME DIR")
		}
		c, err := load(args[1])
		if err != nil {
			return err
		}
		user, err := userValues(files, sets)
		if err != nil {
			return err
		}
		rel := Release{Name: args[0], Namespace: namespace, Revision: 1, IsInstall: true, Service: "Helm"}
		docs, err := render(c, coalesce(copyMap(c.Values), user), rel)
		if err != nil {
			return err
		}
		for _, d := range docs {
			fmt.Printf("---\n# Source: %s\n%s\n", d.Name, d.Data)
		}
		return nil
	}
	return fmt.Errorf("unknown command %q", cmd)
}

// Metadata is Chart.yaml. Templates see it as .Chart, so the Go field names
// are the ones chart authors write: .Chart.Name, .Chart.AppVersion.
type Metadata struct {
	APIVersion  string `json:"apiVersion"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	AppVersion  string `json:"appVersion,omitempty"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type,omitempty"`
}

// File is one file of a chart, named by its slash path from the chart root.
type File struct {
	Name string
	Data []byte
}

type Chart struct {
	Metadata  Metadata
	Values    map[string]any
	Templates []File
	Files     Files
}

// Files is every chart file that is not a template, Chart.yaml or values.yaml.
type Files map[string][]byte

// Get is .Files.Get in a template: the file's contents, or "" if it is absent.
func (f Files) Get(name string) string { return string(f[name]) }

// Release is .Release in a template.
type Release struct {
	Name      string
	Namespace string
	Revision  int
	IsInstall bool
	IsUpgrade bool
	Service   string
}

func load(dir string) (*Chart, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "Chart.yaml"))
	if err != nil {
		return nil, fmt.Errorf("%s is not a chart: %w", dir, err)
	}
	c := &Chart{Files: Files{}}
	if err := yaml.Unmarshal(raw, &c.Metadata); err != nil {
		return nil, fmt.Errorf("cannot load Chart.yaml: %w", err)
	}
	// Helm 2 charts often had no apiVersion, and Helm 3 still loads them as v1.
	if c.Metadata.APIVersion == "" {
		c.Metadata.APIVersion = "v1"
	}
	if err := validate(c.Metadata); err != nil {
		return nil, err
	}
	c.Values = map[string]any{}
	if raw, err := os.ReadFile(filepath.Join(dir, "values.yaml")); err == nil {
		if err := yaml.Unmarshal(raw, &c.Values); err != nil {
			return nil, fmt.Errorf("cannot load values.yaml: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		// Helm never loads dotfiles directly under templates/.
		if strings.HasPrefix(rel, "templates/.") {
			return nil
		}
		if rel == "Chart.yaml" || rel == "values.yaml" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.HasPrefix(rel, "templates/") {
			c.Templates = append(c.Templates, File{Name: rel, Data: data})
		} else {
			c.Files[rel] = data
		}
		return nil
	})
	// Helm sorts templates by whole path, not directory by directory.
	sort.Slice(c.Templates, func(i, j int) bool { return c.Templates[i].Name < c.Templates[j].Name })
	return c, err
}

// validate applies the checks Helm makes before it will use a chart.
func validate(m Metadata) error {
	switch {
	case m.Name == "":
		return fmt.Errorf("validation: chart.metadata.name is required")
	case m.Name != filepath.Base(m.Name):
		return fmt.Errorf("validation: chart.metadata.name %q is invalid", m.Name)
	case m.Version == "":
		return fmt.Errorf("validation: chart.metadata.version is required")
	}
	if _, err := semver.NewVersion(m.Version); err != nil {
		return fmt.Errorf("validation: chart.metadata.version %q is invalid", m.Version)
	}
	if m.Type != "" && m.Type != "application" && m.Type != "library" {
		return fmt.Errorf("validation: chart.metadata.type must be application or library")
	}
	return nil
}

// userValues merges -f files in order, then --set flags in order. A null is
// kept here, not applied: it means "delete the chart's default", which only
// coalesce can do.
func userValues(files, sets []string) (map[string]any, error) {
	user := map[string]any{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var m map[string]any
		if err := yaml.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		mergeMaps(user, m)
	}
	for _, s := range sets {
		if err := parseSet(user, s); err != nil {
			return nil, fmt.Errorf("failed parsing --set data: %w", err)
		}
	}
	return user, nil
}

// mergeMaps overlays src onto dst: maps merge key by key, anything else,
// lists and nulls included, replaces what was there.
func mergeMaps(dst, src map[string]any) {
	for k, v := range src {
		sv, sok := v.(map[string]any)
		dv, dok := dst[k].(map[string]any)
		if sok && dok {
			mergeMaps(dv, sv)
		} else {
			dst[k] = v
		}
	}
}

// coalesce overlays the user's values onto the chart's defaults, where a null
// deletes the key instead of setting it.
func coalesce(dst, src map[string]any) map[string]any {
	for k, v := range src {
		switch sv := v.(type) {
		case nil:
			delete(dst, k)
		case map[string]any:
			dv, ok := dst[k].(map[string]any)
			if !ok {
				dv = map[string]any{}
			}
			dst[k] = coalesce(dv, sv)
		default:
			dst[k] = v
		}
	}
	return dst
}

func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			v = copyMap(sub)
		}
		out[k] = v
	}
	return out
}

// parseSet applies one --set argument: comma-separated a.b.c=value pairs.
// ponytail: no list syntax ({a,b}, a[0]) or escaped commas; add when a stage needs them.
func parseSet(into map[string]any, arg string) error {
	for _, pair := range strings.Split(arg, ",") {
		key, val, ok := strings.Cut(pair, "=")
		if !ok {
			return fmt.Errorf("key %q has no value", pair)
		}
		path := strings.Split(key, ".")
		m := into
		for _, p := range path[:len(path)-1] {
			next, ok := m[p].(map[string]any)
			if !ok {
				next = map[string]any{}
				m[p] = next
			}
			m = next
		}
		m[path[len(path)-1]] = typedVal(val)
	}
	return nil
}

// typedVal reads a --set value the way Helm does: booleans, null and integers
// are typed, except that a leading zero keeps "007" a string.
func typedVal(v string) any {
	switch strings.ToLower(v) {
	case "true":
		return true
	case "false":
		return false
	case "null":
		return nil
	case "0":
		return int64(0)
	}
	if v != "" && v[0] != '0' {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return v
}

// render executes every template of the chart and returns the non-empty
// results in path order. All files are parsed into one set so that any of them
// can call a template another defines.
func render(c *Chart, vals map[string]any, rel Release) ([]File, error) {
	t := template.New("gotpl").Option("missingkey=zero").Funcs(funcMap())
	for _, f := range c.Templates {
		if _, err := t.New(c.Metadata.Name + "/" + f.Name).Parse(string(f.Data)); err != nil {
			return nil, err
		}
	}
	var out []File
	for _, f := range c.Templates {
		base := filepath.Base(f.Name)
		if strings.HasPrefix(base, "_") || strings.HasSuffix(base, "NOTES.txt") {
			continue
		}
		name := c.Metadata.Name + "/" + f.Name
		data := map[string]any{
			"Values":   vals,
			"Release":  rel,
			"Chart":    c.Metadata,
			"Files":    c.Files,
			"Template": map[string]string{"Name": name, "BasePath": c.Metadata.Name + "/templates"},
		}
		var b strings.Builder
		if err := t.ExecuteTemplate(&b, name, data); err != nil {
			return nil, err
		}
		// missingkey=zero prints a missing map key as "<no value>"; Helm blanks it.
		text := strings.TrimSpace(strings.ReplaceAll(b.String(), "<no value>", ""))
		if text != "" {
			out = append(out, File{Name: name, Data: []byte(text)})
		}
	}
	return out, nil
}

// funcMap is sprig's text functions with Helm's changes: nothing that reads
// the environment of whoever renders the chart, and Helm's own additions.
func funcMap() template.FuncMap {
	f := sprig.TxtFuncMap()
	delete(f, "env")
	delete(f, "expandenv")
	f["toYaml"] = toYAML
	f["required"] = required
	return f
}

// toYAML drops the trailing newline so that `toYaml x | nindent 4` controls
// all of the surrounding whitespace itself.
func toYAML(v any) string {
	data, err := yaml.Marshal(v)
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(string(data), "\n")
}

// required fails the render when a value is missing or an empty string, and
// otherwise passes the value through.
func required(msg string, v any) (any, error) {
	if s, ok := v.(string); v == nil || ok && s == "" {
		return v, errors.New(msg)
	}
	return v, nil
}
