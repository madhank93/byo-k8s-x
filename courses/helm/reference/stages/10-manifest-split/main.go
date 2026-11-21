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
//
// Stage 6: templates define named templates and call them with template,
// include and tpl.
//
// Stage 7: charts under charts/ render with the parent, each seeing its own
// slice of the values plus the parent's globals.
//
// Stage 8: Chart.yaml's dependency conditions and tags switch subcharts off.
//
// Stage 9: merged values are validated against each chart's
// values.schema.json before anything renders.
//
// Stage 10: rendered files split into documents, ordered by kind as Helm
// installs them; --show-only picks templates.

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/Masterminds/semver/v3"
	"github.com/Masterminds/sprig/v3"
	"github.com/santhosh-tekuri/jsonschema/v6"
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
	var show multi
	fs.Var(&show, "s", "only show manifests from this template (repeatable)")
	fs.Var(&show, "show-only", "only show manifests from this template (repeatable)")
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
		vals, err := checkedValues(c, user)
		if err != nil {
			return err
		}
		out, err := yaml.Marshal(vals)
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
		if err := missingDeps(c); err != nil {
			return err
		}
		user, err := userValues(files, sets)
		if err != nil {
			return err
		}
		rel := Release{Name: args[0], Namespace: namespace, Revision: 1, IsInstall: true, Service: "Helm"}
		vals, err := checkedValues(c, user)
		if err != nil {
			return err
		}
		docs, err := render(c, vals, rel)
		if err != nil {
			return err
		}
		ms, err := manifests(docs)
		if err != nil {
			return err
		}
		if len(show) > 0 {
			if ms, err = showOnly(ms, show); err != nil {
				return err
			}
		}
		for _, m := range ms {
			fmt.Printf("---\n# Source: %s\n%s\n", m.Name, m.Data)
		}
		return nil
	}
	return fmt.Errorf("unknown command %q", cmd)
}

// Metadata is Chart.yaml, with the fields Helm's chart.Metadata has. Templates
// see it as .Chart, so the Go field names are the ones chart authors write:
// .Chart.Name, .Chart.AppVersion.
type Metadata struct {
	Name         string            `json:"name,omitempty"`
	Home         string            `json:"home,omitempty"`
	Sources      []string          `json:"sources,omitempty"`
	Version      string            `json:"version,omitempty"`
	Description  string            `json:"description,omitempty"`
	Keywords     []string          `json:"keywords,omitempty"`
	Maintainers  []Maintainer      `json:"maintainers,omitempty"`
	Icon         string            `json:"icon,omitempty"`
	APIVersion   string            `json:"apiVersion,omitempty"`
	Condition    string            `json:"condition,omitempty"`
	Tags         string            `json:"tags,omitempty"`
	AppVersion   string            `json:"appVersion,omitempty"`
	Deprecated   bool              `json:"deprecated,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	KubeVersion  string            `json:"kubeVersion,omitempty"`
	Dependencies []Dependency      `json:"dependencies,omitempty"`
	Type         string            `json:"type,omitempty"`
}

type Maintainer struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
	URL   string `json:"url,omitempty"`
}

// Dependency is one entry of Chart.yaml's dependencies.
type Dependency struct {
	Name         string   `json:"name"`
	Version      string   `json:"version,omitempty"`
	Repository   string   `json:"repository"`
	Condition    string   `json:"condition,omitempty"`
	Tags         []string `json:"tags,omitempty"`
	Enabled      bool     `json:"enabled,omitempty"`
	ImportValues []any    `json:"import-values,omitempty"`
	Alias        string   `json:"alias,omitempty"`
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
	Subcharts []*Chart
	Schema    []byte // values.schema.json, if the chart has one
	Files     Files
}

// Files is every chart file Helm does not read itself: not a template,
// Chart.yaml, Chart.lock, values.yaml, values.schema.json or requirements.*,
// and nothing under charts/ but its .prov files.
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
	if raw, err := os.ReadFile(filepath.Join(dir, "values.schema.json")); err == nil {
		c.Schema = raw
	} else if !errors.Is(err, fs.ErrNotExist) {
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
		// Files Helm reads itself, and subcharts, are not .Files.
		switch rel {
		case "Chart.yaml", "Chart.lock", "values.yaml", "values.schema.json", "requirements.yaml", "requirements.lock":
			return nil
		}
		if strings.HasPrefix(rel, "charts/") {
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
	if err != nil {
		return nil, err
	}
	// Helm sorts templates by whole path, not directory by directory.
	sort.Slice(c.Templates, func(i, j int) bool { return c.Templates[i].Name < c.Templates[j].Name })

	// Helm loads every entry of charts/ as a chart.
	// ponytail: unpacked directories only; .tgz archives arrive with dependency build.
	entries, err := os.ReadDir(filepath.Join(dir, "charts"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	for _, e := range entries {
		// Helm ignores entries starting with . or _, and keeps provenance
		// files as plain files of the parent.
		if strings.HasPrefix(e.Name(), ".") || strings.HasPrefix(e.Name(), "_") {
			continue
		}
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".prov") {
			data, err := os.ReadFile(filepath.Join(dir, "charts", e.Name()))
			if err != nil {
				return nil, err
			}
			c.Files["charts/"+e.Name()] = data
			continue
		}
		if !e.IsDir() {
			return nil, fmt.Errorf("error unpacking subchart %s in %s: not a chart directory", e.Name(), c.Metadata.Name)
		}
		sub, err := load(filepath.Join(dir, "charts", e.Name()))
		if err != nil {
			return nil, fmt.Errorf("error unpacking subchart %s in %s: %w", e.Name(), c.Metadata.Name, err)
		}
		c.Subcharts = append(c.Subcharts, sub)
	}
	return c, nil
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
// deletes the default instead of setting it, and stays a null where there is
// no default to delete. keepNull keeps every null, for values bound for a
// subchart, whose own merge deletes its defaults with them.
func coalesce(dst, src map[string]any, keepNull bool) map[string]any {
	for k, v := range src {
		switch sv := v.(type) {
		case nil:
			if _, ok := dst[k]; ok && !keepNull {
				delete(dst, k)
			} else {
				dst[k] = nil
			}
		case map[string]any:
			dv, ok := dst[k].(map[string]any)
			if !ok {
				dv = map[string]any{}
			}
			dst[k] = coalesce(dv, sv, keepNull)
		default:
			dst[k] = v
		}
	}
	return dst
}

// chartValues lays the user's values over the chart's defaults, then does the
// same for each subchart under its name, after handing it the parent's
// globals. The result is stored back, so the parent sees the merged subchart.
func chartValues(c *Chart, user map[string]any) map[string]any {
	subs := map[string]bool{}
	for _, sub := range c.Subcharts {
		subs[sub.Metadata.Name] = true
	}
	v := copyMap(c.Values)
	for k, uv := range user {
		coalesce(v, map[string]any{k: uv}, subs[k])
	}
	for _, sub := range c.Subcharts {
		sv, _ := v[sub.Metadata.Name].(map[string]any)
		if sv == nil {
			sv = map[string]any{}
		}
		sg, _ := sv["global"].(map[string]any)
		if sg == nil {
			sg = map[string]any{}
		}
		g, _ := v["global"].(map[string]any)
		// ponytail: whole keys, parent wins; Helm merges nested global maps key by key.
		for k, val := range g {
			sg[k] = val
		}
		sv["global"] = sg
		v[sub.Metadata.Name] = chartValues(sub, sv)
	}
	return v
}

// enabledValues drops the subcharts that conditions and tags switch off, then
// merges the values of the ones left. Conditions are judged against values
// merged with every subchart present, so a subchart's own defaults count.
func enabledValues(c *Chart, user map[string]any) map[string]any {
	all := chartValues(c, user)
	tags, _ := all["tags"].(map[string]any)
	prune(c, all, tags)
	return chartValues(c, user)
}

// checkedValues is the values a render sees, refused when any chart's schema
// rejects its part of them.
func checkedValues(c *Chart, user map[string]any) (map[string]any, error) {
	vals := enabledValues(c, user)
	if msg := schemaErrors(c, vals); msg != "" {
		return nil, fmt.Errorf("values don't meet the specifications of the schema(s) in the following chart(s):\n%s", msg)
	}
	return vals, nil
}

// schemaErrors validates each chart's values against its own schema, the
// subcharts' against their slice, and lists the failures under chart names.
func schemaErrors(c *Chart, vals map[string]any) string {
	var b strings.Builder
	if c.Schema != nil {
		if err := validateSchema(c.Schema, vals); err != nil {
			fmt.Fprintf(&b, "%s:\n%s\n", c.Metadata.Name, err)
		}
	}
	for _, sub := range c.Subcharts {
		sv, _ := vals[sub.Metadata.Name].(map[string]any)
		b.WriteString(schemaErrors(sub, sv))
	}
	return b.String()
}

// validateSchema uses the library and the resource name Helm does, so the
// messages match Helm's: "- at '/replicas': minimum: got 0, want 1".
func validateSchema(schemaJSON []byte, vals map[string]any) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaJSON))
	if err != nil {
		return err
	}
	const url = "file:///values.schema.json"
	comp := jsonschema.NewCompiler()
	if err := comp.AddResource(url, doc); err != nil {
		return err
	}
	sch, err := comp.Compile(url)
	if err != nil {
		return err
	}
	if err := sch.Validate(vals); err != nil {
		return errors.New(strings.TrimPrefix(err.Error(), "jsonschema validation failed with '"+url+"#'\n"))
	}
	return nil
}

// prune removes disabled subcharts, at every level. Tags come from the top
// chart's values, and a subchart's own tags: defaults fill in for its
// dependencies.
func prune(c *Chart, vals, tags map[string]any) {
	var keep []*Chart
	for _, sub := range c.Subcharts {
		if enabled(c.Metadata.Dependencies, sub.Metadata.Name, vals, tags) {
			sv, _ := vals[sub.Metadata.Name].(map[string]any)
			st := map[string]any{}
			if own, ok := sub.Values["tags"].(map[string]any); ok {
				for k, v := range own {
					st[k] = v
				}
			}
			for k, v := range tags {
				st[k] = v
			}
			prune(sub, sv, st)
			keep = append(keep, sub)
		}
	}
	c.Subcharts = keep
}

// enabled applies Helm's rule: the first condition path holding a boolean
// decides; failing that, any listed tag set true enables and a false one
// disables; with neither, the subchart stays on.
// ponytail: dependencies match by name; alias arrives with dependency build.
func enabled(deps []Dependency, name string, vals, tags map[string]any) bool {
	for _, d := range deps {
		if d.Name != name {
			continue
		}
		for _, p := range strings.Split(strings.TrimSpace(d.Condition), ",") {
			if b, ok := pathValue(vals, p).(bool); ok {
				return b
			}
		}
		on := true
		for _, t := range d.Tags {
			if b, ok := tags[t].(bool); ok {
				if b {
					return true
				}
				on = false
			}
		}
		return on
	}
	return true
}

// missingDeps refuses a chart whose Chart.yaml lists a dependency that
// charts/ does not hold, switched off or not.
func missingDeps(c *Chart) error {
	have := map[string]bool{}
	for _, sub := range c.Subcharts {
		have[sub.Metadata.Name] = true
	}
	var missing []string
	for _, d := range c.Metadata.Dependencies {
		if !have[d.Name] {
			missing = append(missing, d.Name)
		}
	}
	if missing == nil {
		return nil
	}
	return fmt.Errorf("An error occurred while checking for chart dependencies. You may need to run `helm dependency build` to fetch missing dependencies: found in Chart.yaml, but missing in charts/ directory: %s", strings.Join(missing, ", "))
}

// pathValue reads a dotted path such as db.enabled, or nil if it is not there.
func pathValue(vals map[string]any, path string) any {
	var v any = vals
	for _, k := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
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
	if c.Metadata.Type == "library" {
		return nil, errors.New("library charts are not installable")
	}
	// Every chart's templates, named by their path from the top chart, each
	// with the data its chart's templates see.
	var all []File
	data := map[string]map[string]any{}
	var walk func(c *Chart, prefix string, vals map[string]any)
	walk = func(c *Chart, prefix string, vals map[string]any) {
		top := map[string]any{"Values": vals, "Release": rel, "Chart": c.Metadata, "Files": c.Files, "BasePath": prefix + "/templates"}
		for _, f := range c.Templates {
			// A library chart lends its defines and renders nothing.
			if c.Metadata.Type == "library" && !strings.HasPrefix(filepath.Base(f.Name), "_") {
				continue
			}
			all = append(all, File{Name: prefix + "/" + f.Name, Data: f.Data})
			data[prefix+"/"+f.Name] = top
		}
		for _, sub := range c.Subcharts {
			sv, _ := vals[sub.Metadata.Name].(map[string]any)
			walk(sub, prefix+"/charts/"+sub.Metadata.Name, sv)
		}
	}
	walk(c, c.Metadata.Name, vals)
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	t := template.New("gotpl").Option("missingkey=zero").Funcs(funcMap())
	bindIncludes(t, map[string]int{})
	for _, f := range parseOrder(all) {
		if _, err := t.New(f.Name).Parse(string(f.Data)); err != nil {
			return nil, err
		}
	}
	var out []File
	for _, f := range all {
		base := filepath.Base(f.Name)
		if strings.HasPrefix(base, "_") {
			continue
		}
		name, top := f.Name, data[f.Name]
		d := map[string]any{
			"Values":   top["Values"],
			"Release":  rel,
			"Chart":    top["Chart"],
			"Files":    top["Files"],
			"Template": map[string]string{"Name": name, "BasePath": top["BasePath"].(string)},
		}
		var b strings.Builder
		if err := t.ExecuteTemplate(&b, name, d); err != nil {
			return nil, err
		}
		// missingkey=zero prints a missing map key as "<no value>"; Helm blanks it.
		text := strings.TrimSpace(strings.ReplaceAll(b.String(), "<no value>", ""))
		// NOTES.txt renders like any template, so its errors count, but it is
		// not a manifest.
		if text != "" && !strings.HasSuffix(base, "NOTES.txt") {
			out = append(out, File{Name: name, Data: []byte(text)})
		}
	}
	return out, nil
}

// parseOrder is the order Helm parses templates in: deepest paths first, and
// within a depth in reverse. A later define replaces an earlier one, so the
// shallowest, lexically first definition of a name wins.
func parseOrder(files []File) []File {
	out := append([]File(nil), files...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i].Name, out[j].Name
		if da, db := strings.Count(a, "/"), strings.Count(b, "/"); da != db {
			return da > db
		}
		return a > b
	})
	return out
}

// installOrder is Helm's InstallOrder: the kinds a release creates first.
var installOrder = []string{
	"PriorityClass", "Namespace", "NetworkPolicy", "ResourceQuota", "LimitRange",
	"PodSecurityPolicy", "PodDisruptionBudget", "ServiceAccount", "Secret",
	"SecretList", "ConfigMap", "StorageClass", "PersistentVolume",
	"PersistentVolumeClaim", "CustomResourceDefinition", "ClusterRole",
	"ClusterRoleList", "ClusterRoleBinding", "ClusterRoleBindingList", "Role",
	"RoleList", "RoleBinding", "RoleBindingList", "Service", "DaemonSet", "Pod",
	"ReplicationController", "ReplicaSet", "Deployment", "HorizontalPodAutoscaler",
	"StatefulSet", "Job", "CronJob", "IngressClass", "Ingress", "APIService",
}

// docSep is Helm's document separator, kept exact: it also decides what
// whitespace and comments stay with each document.
var docSep = regexp.MustCompile("(?:^|\\s*\n)---\\s*")

// manifests splits rendered files into documents, each named by its file,
// and orders them by kind: known kinds by installOrder, then unknown kinds
// alphabetically, and within a kind in file-path and document order.
func manifests(files []File) ([]File, error) {
	rank := map[string]int{}
	for i, k := range installOrder {
		rank[k] = i
	}
	var out []File
	var kinds []string
	for _, f := range files {
		for _, d := range docSep.Split(strings.TrimSpace(string(f.Data)), -1) {
			if d == "" {
				continue
			}
			d = strings.TrimSpace(d)
			var head struct {
				Kind string `json:"kind"`
			}
			if err := yaml.Unmarshal([]byte(d), &head); err != nil {
				return nil, fmt.Errorf("YAML parse error on %s: %w", f.Name, err)
			}
			out = append(out, File{Name: f.Name, Data: []byte(d)})
			kinds = append(kinds, head.Kind)
		}
	}
	idx := make([]int, len(out))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool {
		a, b := kinds[idx[i]], kinds[idx[j]]
		ra, aok := rank[a]
		rb, bok := rank[b]
		switch {
		case !aok && !bok:
			return a < b
		case !aok || !bok:
			return aok
		}
		return ra < rb
	})
	sorted := make([]File, len(out))
	for i, j := range idx {
		sorted[i] = out[j]
	}
	return sorted, nil
}

// showOnly keeps the manifests whose template path, without the chart name,
// matches each pattern in turn; a pattern matching nothing is an error.
// ponytail: matches documents directly; Helm re-splits its printed output,
// which differs only for a document that itself begins with "---".
func showOnly(ms []File, patterns []string) ([]File, error) {
	var out []File
	for _, p := range patterns {
		found := false
		for _, m := range ms {
			_, rel, _ := strings.Cut(m.Name, "/")
			if ok, _ := filepath.Match(filepath.ToSlash(p), rel); ok {
				out = append(out, m)
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("could not find template %s in chart", p)
		}
	}
	return out, nil
}

// bindIncludes gives a template set its include and tpl, closing over that
// set: tpl's clone is bound again, so a define made inside tpl text can be
// included. depth counts nested includes of each name, which Helm caps at 1000.
func bindIncludes(t *template.Template, depth map[string]int) *template.Template {
	return t.Funcs(template.FuncMap{
		"include": func(name string, data any) (string, error) {
			if depth[name] > 1000 {
				return "", fmt.Errorf("rendering template has a nested reference name: %s: unable to execute template", name)
			}
			depth[name]++
			defer func() { depth[name]-- }()
			var b strings.Builder
			err := t.ExecuteTemplate(&b, name, data)
			return b.String(), err
		},
		"tpl": func(text string, data any) (string, error) {
			c, err := t.Clone()
			if err != nil {
				return "", err
			}
			if c, err = bindIncludes(c, depth).New("gotpl").Parse(text); err != nil {
				return "", err
			}
			var b strings.Builder
			if err := c.Execute(&b, data); err != nil {
				return "", err
			}
			return strings.ReplaceAll(b.String(), "<no value>", ""), nil
		},
	})
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
