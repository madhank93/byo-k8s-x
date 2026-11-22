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
//
// Stage 11: `install [-n NS] [-f FILE]... [--set k=v]... NAME DIR` creates the
// rendered objects in the cluster, marked as the release's, and refuses to take
// over objects it does not own. The namespace defaults to the kubeconfig's.
//
// Stage 12: install records the release as a Secret per revision, in Helm's
// own format, and refuses a name that already has one.
//
// Stage 13: `list [-a] [-A] [-q]` and `status NAME` read the records back and
// print them as helm does.
//
// Stage 14: `history [--max N] NAME` and `get manifest|values|notes
// [--revision N] NAME` read any revision.
//
// Stage 15: `upgrade [-i] [--reuse-values|--reset-values] NAME DIR` adds a
// revision, patching live objects with a three-way merge of the old manifest,
// the new one and what is in the cluster.
//
// Stage 16: `rollback NAME [REVISION]` records a new revision holding an old
// one's chart, values and manifest, and moves the cluster to it.
//
// Stage 17: documents annotated helm.sh/hook are kept out of the manifest
// and run around install, upgrade and rollback, each waited on and deleted
// as its delete policy says.
//
// Stage 18: an event's hooks run lowest weight first, then by name, then in
// the order kinds install; `get hooks NAME` prints them as they are stored.
//
// Stage 19: `test [--filter [!]name=N]... [--logs] NAME` runs the latest
// revision's test hooks and records how they went, which status then shows.
//
// Stage 20: `uninstall [--keep-history] [--ignore-not-found] [--dry-run]
// NAME...` deletes a release's objects, bar those whose resource policy is
// keep, runs its delete hooks, and drops its records or marks the last one
// uninstalled; `install --replace` and `upgrade -i` reuse such a name.
//
// Stage 21: `dependency list|update|build DIR` resolves Chart.yaml's
// dependencies into charts/ and pins them in Chart.lock; `package DIR` writes
// the chart as Helm's archive, which load reads back.

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/Masterminds/sprig/v3"
	"github.com/santhosh-tekuri/jsonschema/v6"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app <chart> [flags] ARGS")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	if cmd == "get" && len(args) > 0 {
		cmd, args = "get "+args[0], args[1:]
	}
	if (cmd == "dependency" || cmd == "dep") && len(args) > 0 {
		cmd, args = "dependency "+args[0], args[1:]
	}
	do := run
	if cmd == "package" || strings.HasPrefix(cmd, "dependency ") {
		do = runPackaging
	}
	if err := do(cmd, args); err != nil {
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
	fs.StringVar(&namespace, "n", "", "release namespace")
	fs.StringVar(&namespace, "namespace", "", "release namespace")
	fs.Var(&files, "f", "values file (repeatable)")
	fs.Var(&sets, "set", "key=value (repeatable)")
	var show multi
	fs.Var(&show, "s", "only show manifests from this template (repeatable)")
	fs.Var(&show, "show-only", "only show manifests from this template (repeatable)")
	var all, allNamespaces, short bool
	fs.BoolVar(&all, "a", false, "list releases in every state")
	fs.BoolVar(&all, "all", false, "list releases in every state")
	fs.BoolVar(&allNamespaces, "A", false, "list releases in every namespace")
	fs.BoolVar(&allNamespaces, "all-namespaces", false, "list releases in every namespace")
	fs.BoolVar(&short, "q", false, "print release names only")
	fs.BoolVar(&short, "short", false, "print release names only")
	histMax := fs.Int("max", 256, "maximum number of revisions in the history")
	revision := fs.Int("revision", 0, "the revision to get, 0 for the latest")
	var up upgradeOpts
	var keepHistory, ignoreNotFound, dryRun, replace, uninstalled bool
	fs.BoolVar(&keepHistory, "keep-history", false, "keep the release's records, marked uninstalled")
	fs.BoolVar(&ignoreNotFound, "ignore-not-found", false, "treat a release that does not exist as uninstalled")
	fs.BoolVar(&dryRun, "dry-run", false, "change nothing")
	fs.BoolVar(&replace, "replace", false, "reuse the name of an uninstalled or failed release")
	fs.BoolVar(&uninstalled, "uninstalled", false, "list uninstalled releases only")
	fs.BoolVar(&up.install, "i", false, "install the release if it does not exist")
	fs.BoolVar(&up.install, "install", false, "install the release if it does not exist")
	fs.BoolVar(&up.reuse, "reuse-values", false, "merge the new values over the last release's")
	fs.BoolVar(&up.reset, "reset-values", false, "use only the new values and the chart's defaults")
	var filters multi
	fs.Var(&filters, "filter", "name=NAME or !name=NAME, the test hooks to run or skip (repeatable)")
	logs := fs.Bool("logs", false, "print the test pods' logs")
	fs.DurationVar(&hookTimeout, "timeout", 5*time.Minute, "how long to wait for each hook")
	if err := fs.Parse(args); err != nil {
		return err
	}
	args = fs.Args()
	if namespace == "" {
		namespace = kubeNamespace()
	}

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
		rel := Release{Name: args[0], Namespace: namespace, Revision: 1, IsInstall: true, Service: "Helm"}
		r, err := renderRelease(args[1], files, sets, rel)
		if err != nil {
			return err
		}
		ms := r.Manifests
		if len(show) > 0 {
			if ms, err = showOnly(ms, show); err != nil {
				return err
			}
		}
		fmt.Print(manifestText(ms))
		if len(show) == 0 {
			fmt.Print(hookText(r.Hooks))
		}
		return nil
	case "install":
		if len(args) != 2 {
			return fmt.Errorf("usage: install [-n NS] [-f FILE]... [--set k=v]... NAME DIR")
		}
		if err := install(args[0], args[1], namespace, files, sets, replace); err != nil {
			return fmt.Errorf("INSTALLATION FAILED: %w", err)
		}
		return nil
	case "upgrade":
		if len(args) != 2 {
			return fmt.Errorf("usage: upgrade [-n NS] [-i] [-f FILE]... [--set k=v]... NAME DIR")
		}
		return upgrade(args[0], args[1], namespace, files, sets, up)
	case "rollback":
		if len(args) != 1 && len(args) != 2 {
			return fmt.Errorf("usage: rollback [-n NS] NAME [REVISION]")
		}
		to := 0
		if len(args) == 2 {
			n, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("could not convert revision to a number: %w", err)
			}
			to = n
		}
		return rollback(args[0], namespace, to)
	case "test":
		if len(args) != 1 {
			return fmt.Errorf(`"helm test" requires 1 argument`)
		}
		return releaseTest(args[0], namespace, filters, *logs)
	case "list":
		if len(args) != 0 {
			return fmt.Errorf(`"helm list" accepts no arguments`)
		}
		if allNamespaces {
			namespace = ""
		}
		return list(namespace, all, short, uninstalled)
	case "uninstall":
		if len(args) == 0 {
			return fmt.Errorf(`"helm uninstall" requires at least 1 argument`)
		}
		return uninstall(args, namespace, keepHistory, ignoreNotFound, dryRun)
	case "status":
		if len(args) != 1 {
			return fmt.Errorf(`"helm status" requires 1 argument`)
		}
		r, err := revisionOf(namespace, args[0], 0)
		if err != nil {
			return err
		}
		printRelease(r)
		return nil
	case "history":
		if len(args) != 1 {
			return fmt.Errorf(`"helm history" requires 1 argument`)
		}
		return history(namespace, args[0], *histMax)
	case "get manifest", "get values", "get notes", "get hooks":
		if len(args) != 1 {
			return fmt.Errorf(`"helm %s" requires 1 argument`, cmd)
		}
		r, err := revisionOf(namespace, args[0], *revision)
		if err != nil {
			return err
		}
		switch cmd {
		case "get manifest":
			fmt.Println(r.Manifest)
		case "get hooks":
			fmt.Print(hookText(r.Hooks))
		case "get notes":
			if r.Info.Notes != "" {
				fmt.Printf("NOTES:\n%s\n", r.Info.Notes)
			}
		case "get values":
			vals, title := r.Config, "USER-SUPPLIED VALUES:"
			if all {
				vals, title = chartValues(&Chart{Values: r.Chart.Values}, r.Config), "COMPUTED VALUES:"
			}
			out, err := yaml.Marshal(vals)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n%s", title, out)
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
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type Chart struct {
	Metadata  Metadata
	Values    map[string]any
	Templates []File
	Subcharts []*Chart
	Schema    []byte // values.schema.json, if the chart has one
	Files     Files
	Raw       []byte // values.yaml as written, nil if the chart has none
	Lock      *Lock  // Chart.lock, if the chart has one
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

// load reads a chart directory, or a packaged chart when dir is a file.
// A directory's .helmignore governs its whole tree, subcharts included.
func load(dir string) (*Chart, error) {
	if fi, err := os.Stat(dir); err == nil && !fi.IsDir() {
		return loadArchive(dir)
	}
	rules, err := ignoreRules(dir)
	if err != nil {
		return nil, err
	}
	return loadDir(dir, "", rules)
}

// loadDir reads the chart in dir, which sits at prefix inside the directory
// whose ignore rules apply; an archive's contents are loaded with none.
func loadDir(dir, prefix string, rules []string) (*Chart, error) {
	skip := func(rel string, isDir bool) bool { return ignored(rules, prefix+rel, isDir) }
	// read is a file Helm reads itself, or nil when it is absent or ignored.
	read := func(name string) ([]byte, error) {
		if skip(name, false) {
			return nil, nil
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return raw, err
	}
	raw, err := read("Chart.yaml")
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, errors.New("Chart.yaml file is missing")
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
	if c.Schema, err = read("values.schema.json"); err != nil {
		return nil, err
	}
	if raw, err := read("Chart.lock"); err != nil {
		return nil, err
	} else if raw != nil {
		c.Lock = &Lock{}
		if err := yaml.Unmarshal(raw, c.Lock); err != nil {
			return nil, fmt.Errorf("cannot load Chart.lock: %w", err)
		}
	}
	c.Values = map[string]any{}
	if c.Raw, err = read("values.yaml"); err != nil {
		return nil, err
	} else if c.Raw != nil {
		if err := yaml.Unmarshal(c.Raw, &c.Values); err != nil {
			return nil, fmt.Errorf("cannot load values.yaml: %w", err)
		}
	}
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || rel == "." {
			return err
		}
		rel = filepath.ToSlash(rel)
		if skip(rel, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
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

	// Helm loads every entry of charts/ as a chart, packaged or not.
	var entries []fs.DirEntry
	if !skip("charts", true) {
		entries, err = os.ReadDir(filepath.Join(dir, "charts"))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
	for _, e := range entries {
		// Helm ignores entries starting with . or _, and keeps provenance
		// files as plain files of the parent.
		if strings.HasPrefix(e.Name(), ".") || strings.HasPrefix(e.Name(), "_") || skip("charts/"+e.Name(), e.IsDir()) {
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
		if !e.IsDir() && !strings.HasSuffix(e.Name(), ".tgz") {
			return nil, fmt.Errorf("error unpacking subchart %s in %s: not a chart directory", e.Name(), c.Metadata.Name)
		}
		p := filepath.Join(dir, "charts", e.Name())
		var sub *Chart
		if e.IsDir() {
			sub, err = loadDir(p, prefix+"charts/"+e.Name()+"/", rules)
		} else {
			sub, err = loadArchive(p)
		}
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
	if err := checkDeps(c); err != nil {
		return fmt.Errorf("An error occurred while checking for chart dependencies. You may need to run `helm dependency build` to fetch missing dependencies: %w", err)
	}
	return nil
}

// checkDeps lists, in Chart.yaml's order, the dependencies charts/ lacks.
func checkDeps(c *Chart) error {
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
	return fmt.Errorf("found in Chart.yaml, but missing in charts/ directory: %s", strings.Join(missing, ", "))
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
func render(c *Chart, vals map[string]any, rel Release) ([]File, string, error) {
	if c.Metadata.Type == "library" {
		return nil, "", errors.New("library charts are not installable")
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
			return nil, "", err
		}
	}
	var out []File
	var notes string
	notesFile := c.Metadata.Name + "/templates/NOTES.txt"
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
			return nil, "", err
		}
		// missingkey=zero prints a missing map key as "<no value>"; Helm blanks it.
		text := strings.ReplaceAll(b.String(), "<no value>", "")
		// Every NOTES.txt renders, so its errors count, but only the top
		// chart's is kept.
		if strings.HasSuffix(base, "NOTES.txt") {
			if name == notesFile {
				notes = text
			}
			continue
		}
		if text = strings.TrimSpace(text); text != "" {
			out = append(out, File{Name: name, Data: []byte(text)})
		}
	}
	return out, notes, nil
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

// kubeNamespace is the kubeconfig context's namespace, as kubectl picks it,
// or "default" when there is no kubeconfig or the context names none.
func kubeNamespace() string {
	ns, _, err := kubeConfig().Namespace()
	if err != nil || ns == "" {
		return "default"
	}
	return ns
}

func kubeConfig() clientcmd.ClientConfig {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
}

// releaseName is Helm's rule: a DNS subdomain of at most 53 characters, so
// that names built from it still fit Kubernetes' limits.
var releaseName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// Rendered is a chart rendered for one release.
type Rendered struct {
	Chart     *Chart
	Config    map[string]any // the user's values, before the chart's defaults
	Manifests []File         // in install order, hooks left out
	Hooks     []*Hook
	Notes     string
}

// renderRelease is the whole pipeline for a release: load, merge and check
// the values, render, split and order.
func renderRelease(dir string, files, sets []string, rel Release) (*Rendered, error) {
	user, err := userValues(files, sets)
	if err != nil {
		return nil, err
	}
	return renderWith(dir, user, rel)
}

// renderWith renders a chart for a release with the user's values already
// decided, as upgrade does after choosing whether to reuse the last ones.
func renderWith(dir string, user map[string]any, rel Release) (*Rendered, error) {
	if !releaseName.MatchString(rel.Name) || len(rel.Name) > 53 {
		return nil, fmt.Errorf("release name %q: invalid release name, must match regex %s and the length must not be longer than 53", rel.Name, releaseName)
	}
	c, err := load(dir)
	if err != nil {
		return nil, err
	}
	if err := missingDeps(c); err != nil {
		return nil, err
	}
	vals, err := checkedValues(c, user)
	if err != nil {
		return nil, err
	}
	docs, notes, err := render(c, vals, rel)
	if err != nil {
		return nil, err
	}
	ms, err := manifests(docs)
	if err != nil {
		return nil, err
	}
	ms, hooks := splitHooks(ms)
	return &Rendered{Chart: c, Config: user, Manifests: ms, Hooks: hooks, Notes: notes}, nil
}

// The marks that say which release an object belongs to.
const (
	managedBy      = "app.kubernetes.io/managed-by"
	releaseNameKey = "meta.helm.sh/release-name"
	releaseNSKey   = "meta.helm.sh/release-namespace"
)

func install(name, dir, ns string, files, sets []string, replace bool) error {
	k, err := connect()
	if err != nil {
		return err
	}
	// Any revision at all, whatever its status, holds the name, unless
	// replace reuses an uninstalled or failed one.
	h, err := k.history(ns, name)
	if err != nil {
		return err
	}
	var last *Record
	if len(h) > 0 {
		last = h[len(h)-1]
		if st := last.Info.Status; !replace || (st != "uninstalled" && st != "failed") {
			return errors.New("cannot re-use a name that is still in use")
		}
	}
	rel := Release{Name: name, Namespace: ns, Revision: 1, IsInstall: true, Service: "Helm"}
	r, err := renderRelease(dir, files, sets, rel)
	if err != nil {
		return err
	}
	objs, err := k.objects(r.Manifests, ns)
	if err != nil {
		return err
	}
	// Every object is checked before any is created, so a refusal leaves
	// nothing behind, not even a record.
	for _, o := range objs {
		live, err := k.get(o)
		if err != nil {
			return err
		}
		if live != nil {
			if err := owned(live, name, ns); err != nil {
				return fmt.Errorf("Unable to continue with install: %w", err)
			}
		}
	}
	now := time.Now()
	version := 1
	if last != nil {
		version = last.Version + 1
		if last.Info.Status != "failed" {
			last.Info.Status, last.Info.Description = "superseded", "superseded by new release"
			if err := k.saveRecord(last, true); err != nil {
				return err
			}
		}
	}
	rec := &Record{
		Name: name, Namespace: ns, Version: version,
		Info: &Info{FirstDeployed: Time{now}, LastDeployed: Time{now}, Status: "pending-install",
			Description: "Initial install underway", Notes: r.Notes},
		Chart:    storedChart(r.Chart),
		Config:   r.Config,
		Manifest: manifestText(r.Manifests),
		Hooks:    r.Hooks,
	}
	if err := k.saveRecord(rec, false); err != nil {
		return err
	}
	fail := func(err error) error {
		rec.Info.Status = "failed"
		rec.Info.Description = fmt.Sprintf("Release %q failed: %s", name, err)
		if serr := k.saveRecord(rec, true); serr != nil {
			return serr
		}
		return err
	}
	if err := k.execHooks(rec, "pre-install"); err != nil {
		return fail(fmt.Errorf("failed pre-install: %s", err))
	}
	// An object already marked as this release's, one an uninstall kept, is
	// adopted.
	if err := k.update(nil, objs, name, ns); err != nil {
		return fail(err)
	}
	if err := k.execHooks(rec, "post-install"); err != nil {
		return fail(fmt.Errorf("failed post-install: %s", err))
	}
	rec.Info.Status, rec.Info.Description = "deployed", "Install complete"
	if err := k.saveRecord(rec, true); err != nil {
		return err
	}
	printRelease(rec)
	return nil
}

// manifestText is the rendered documents as template prints them and as the
// release records them.
func manifestText(ms []File) string {
	var b strings.Builder
	for _, m := range ms {
		fmt.Fprintf(&b, "---\n# Source: %s\n%s\n", m.Name, m.Data)
	}
	return b.String()
}

// printRelease is the summary install prints.
func printRelease(r *Record) {
	fmt.Printf("NAME: %s\n", r.Name)
	fmt.Printf("LAST DEPLOYED: %s\n", r.Info.LastDeployed.Format(time.ANSIC))
	fmt.Printf("NAMESPACE: %s\n", r.Namespace)
	fmt.Printf("STATUS: %s\n", r.Info.Status)
	fmt.Printf("REVISION: %d\n", r.Version)
	var tests []*Hook
	for _, h := range r.Hooks {
		if slices.Contains(h.Events, "test") {
			tests = append(tests, h)
		}
	}
	if len(tests) == 0 {
		fmt.Println("TEST SUITE: None")
	}
	for _, h := range tests {
		if h.LastRun.StartedAt.IsZero() {
			continue
		}
		fmt.Printf("TEST SUITE:     %s\nLast Started:   %s\nLast Completed: %s\nPhase:          %s\n",
			h.Name, h.LastRun.StartedAt.Format(time.ANSIC), h.LastRun.CompletedAt.Format(time.ANSIC), h.LastRun.Phase)
	}
	if r.Info.Notes != "" {
		fmt.Printf("NOTES:\n%s\n", strings.TrimSpace(r.Info.Notes))
	}
}

// Record is Helm's release.Release, with the JSON names Helm stores it under.
type Record struct {
	Name      string         `json:"name,omitempty"`
	Info      *Info          `json:"info,omitempty"`
	Chart     *StoredChart   `json:"chart,omitempty"`
	Config    map[string]any `json:"config,omitempty"`
	Manifest  string         `json:"manifest,omitempty"`
	Hooks     []*Hook        `json:"hooks,omitempty"`
	Version   int            `json:"version,omitempty"`
	Namespace string         `json:"namespace,omitempty"`
}

type Info struct {
	FirstDeployed Time   `json:"first_deployed"`
	LastDeployed  Time   `json:"last_deployed"`
	Deleted       Time   `json:"deleted"`
	Description   string `json:"description,omitempty"`
	Status        string `json:"status,omitempty"`
	Notes         string `json:"notes,omitempty"`
}

// Time is a time as Helm stores one: the zero time is "", which
// time.Time's own JSON methods refuse.
type Time struct{ time.Time }

func (t Time) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		return []byte(`""`), nil
	}
	return t.Time.MarshalJSON()
}

func (t *Time) UnmarshalJSON(b []byte) error {
	if string(b) == `""` || string(b) == "null" {
		return nil
	}
	return t.Time.UnmarshalJSON(b)
}

// StoredChart is the top chart as a release keeps it: no subcharts.
type StoredChart struct {
	Metadata  Metadata       `json:"metadata"`
	Lock      any            `json:"lock"`
	Templates []File         `json:"templates"`
	Values    map[string]any `json:"values"`
	Schema    []byte         `json:"schema"`
	Files     []File         `json:"files"`
}

func storedChart(c *Chart) *StoredChart {
	s := &StoredChart{Metadata: c.Metadata, Templates: c.Templates, Values: c.Values, Schema: c.Schema}
	for name, data := range c.Files {
		s.Files = append(s.Files, File{Name: name, Data: data})
	}
	sort.Slice(s.Files, func(i, j int) bool { return s.Files[i].Name < s.Files[j].Name })
	return s
}

// saveRecord writes a revision as Helm's Secrets driver does: gzipped JSON,
// base64-encoded, labelled so revisions are found without decoding them.
func (k *kube) saveRecord(r *Record, update bool) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(raw); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("sh.helm.release.v1.%s.v%d", r.Name, r.Version),
			Labels: map[string]string{
				"name": r.Name, "owner": "helm", "status": r.Info.Status,
				"version": strconv.Itoa(r.Version), "modifiedAt": strconv.FormatInt(time.Now().Unix(), 10),
			},
		},
		Type: "helm.sh/release.v1",
		Data: map[string][]byte{"release": []byte(base64.StdEncoding.EncodeToString(gz.Bytes()))},
	}
	secrets := k.cs.CoreV1().Secrets(r.Namespace)
	if update {
		_, err = secrets.Update(k.ctx, s, metav1.UpdateOptions{})
	} else {
		_, err = secrets.Create(k.ctx, s, metav1.CreateOptions{})
	}
	return err
}

func decodeRecord(s *corev1.Secret) (*Record, error) {
	gz, err := base64.StdEncoding.DecodeString(string(s.Data["release"]))
	if err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, err
	}
	r := &Record{}
	if err := json.NewDecoder(zr).Decode(r); err != nil {
		return nil, fmt.Errorf("%s: %w", s.Name, err)
	}
	return r, nil
}

// records decodes every release Secret matching the selector in ns, or in
// every namespace when ns is "".
func (k *kube) records(ns, selector string) ([]*Record, error) {
	list, err := k.cs.CoreV1().Secrets(ns).List(k.ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	var out []*Record
	for i := range list.Items {
		r, err := decodeRecord(&list.Items[i])
		if err != nil {
			continue // helm skips a release Secret it cannot decode
		}
		out = append(out, r)
	}
	return out, nil
}

// history is every revision of a release, oldest first.
func (k *kube) history(ns, name string) ([]*Record, error) {
	h, err := k.records(ns, "owner=helm,name="+name)
	if err != nil {
		return nil, err
	}
	sort.Slice(h, func(i, j int) bool { return h[i].Version < h[j].Version })
	return h, nil
}

// mark labels and annotates an object as the release's.
func mark(o *unstructured.Unstructured, name, ns string) {
	l := o.GetLabels()
	if l == nil {
		l = map[string]string{}
	}
	l[managedBy] = "Helm"
	o.SetLabels(l)
	a := o.GetAnnotations()
	if a == nil {
		a = map[string]string{}
	}
	a[releaseNameKey] = name
	a[releaseNSKey] = ns
	o.SetAnnotations(a)
}

// owned refuses an object that does not carry the release's marks, listing
// every mark that is missing or wrong, as Helm does.
func owned(live *unstructured.Unstructured, name, ns string) error {
	var errs []string
	check := func(what string, m map[string]string, key, want string) {
		got, ok := m[key]
		switch {
		case !ok:
			errs = append(errs, fmt.Sprintf("%s validation error: missing key %q: must be set to %q", what, key, want))
		case got != want:
			errs = append(errs, fmt.Sprintf("%s validation error: key %q must equal %q: current value is %q", what, key, want, got))
		}
	}
	check("label", live.GetLabels(), managedBy, "Helm")
	check("annotation", live.GetAnnotations(), releaseNameKey, name)
	check("annotation", live.GetAnnotations(), releaseNSKey, ns)
	if errs == nil {
		return nil
	}
	return fmt.Errorf("%s %q in namespace %q exists and cannot be imported into the current release: invalid ownership metadata; %s",
		live.GetKind(), live.GetName(), live.GetNamespace(), strings.Join(errs, "; "))
}

// kube reaches any kind: a dynamic client, and a mapper from kinds to the
// resources that serve them.
type kube struct {
	ctx    context.Context
	cs     kubernetes.Interface
	dyn    dynamic.Interface
	mapper meta.RESTMapper
}

func connect() (*kube, error) {
	cfg, err := kubeConfig().ClientConfig()
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &kube{ctx: context.Background(), cs: cs, dyn: dyn,
		mapper: restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))}, nil
}

// object is one manifest parsed, with the endpoint that serves it.
type object struct {
	obj *unstructured.Unstructured
	res dynamic.ResourceInterface
}

// objects parses manifests. A namespaced object that names no namespace goes
// in the release's; a cluster-scoped one never has one.
func (k *kube) objects(ms []File, ns string) ([]object, error) {
	var out []object
	for _, m := range ms {
		o := &unstructured.Unstructured{}
		if err := yaml.Unmarshal(m.Data, &o.Object); err != nil {
			return nil, fmt.Errorf("%s: %w", m.Name, err)
		}
		gvk := o.GroupVersionKind()
		mapping, err := k.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("%s: resource mapping not found for name: %q kind: %q: %w", m.Name, o.GetName(), gvk.Kind, err)
		}
		if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
			o.SetNamespace("")
			out = append(out, object{o, k.dyn.Resource(mapping.Resource)})
			continue
		}
		if o.GetNamespace() == "" {
			o.SetNamespace(ns)
		}
		out = append(out, object{o, k.dyn.Resource(mapping.Resource).Namespace(o.GetNamespace())})
	}
	return out, nil
}

// get is the live object, or nil when there is none.
func (k *kube) get(o object) (*unstructured.Unstructured, error) {
	live, err := o.res.Get(k.ctx, o.obj.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return live, err
}

var errNotFound = errors.New("release: not found")

// list prints the latest revision of every release in ns (every namespace
// when ns is ""), sorted by name. Without all, only deployed and failed
// releases are shown.
func list(ns string, all, short, uninstalled bool) error {
	k, err := connect()
	if err != nil {
		return err
	}
	recs, err := k.records(ns, "owner=helm")
	if err != nil {
		return err
	}
	latest := map[string]*Record{}
	var keys []string
	for _, r := range recs {
		key := r.Namespace + "/" + r.Name
		if cur, ok := latest[key]; !ok {
			keys = append(keys, key)
		} else if cur.Version > r.Version {
			continue
		}
		latest[key] = r
	}
	var shown []*Record
	for _, key := range keys {
		r := latest[key]
		st := r.Info.Status
		if all || (uninstalled && st == "uninstalled") || (!uninstalled && (st == "deployed" || st == "failed")) {
			shown = append(shown, r)
		}
	}
	sort.SliceStable(shown, func(i, j int) bool { return shown[i].Name < shown[j].Name })
	if short {
		for _, r := range shown {
			fmt.Println(r.Name)
		}
		return nil
	}
	rows := [][]string{{"NAME", "NAMESPACE", "REVISION", "UPDATED", "STATUS", "CHART", "APP VERSION"}}
	for _, r := range shown {
		rows = append(rows, []string{r.Name, r.Namespace, strconv.Itoa(r.Version), r.Info.LastDeployed.String(),
			r.Info.Status, r.Chart.Metadata.Name + "-" + r.Chart.Metadata.Version, r.Chart.Metadata.AppVersion})
	}
	printTable(rows)
	return nil
}

// printTable lays rows out as Helm's tables do: every cell padded to its
// column's width, the last column too, and cells joined by a tab.
func printTable(rows [][]string) {
	widths := make([]int, len(rows[0]))
	for _, row := range rows {
		for i, cell := range row {
			widths[i] = max(widths[i], len(cell))
		}
	}
	for _, row := range rows {
		cells := make([]string, len(row))
		for i, cell := range row {
			cells[i] = fmt.Sprintf("%-*s", widths[i], cell)
		}
		fmt.Println(strings.Join(cells, "\t"))
	}
}

// revisionOf is one revision of a release, or its latest when revision is 0.
func revisionOf(ns, name string, revision int) (*Record, error) {
	k, err := connect()
	if err != nil {
		return nil, err
	}
	h, err := k.history(ns, name)
	if err != nil {
		return nil, err
	}
	for i := len(h) - 1; i >= 0; i-- {
		if revision == 0 || h[i].Version == revision {
			return h[i], nil
		}
	}
	return nil, errNotFound
}

// history prints the last limit revisions of a release, oldest first.
func history(ns, name string, limit int) error {
	k, err := connect()
	if err != nil {
		return err
	}
	h, err := k.history(ns, name)
	if err != nil {
		return err
	}
	if len(h) == 0 {
		return errNotFound
	}
	h = h[len(h)-min(max(limit, 0), len(h)):]
	rows := [][]string{{"REVISION", "UPDATED", "STATUS", "CHART", "APP VERSION", "DESCRIPTION"}}
	for _, r := range h {
		rows = append(rows, []string{strconv.Itoa(r.Version), r.Info.LastDeployed.Format(time.ANSIC), r.Info.Status,
			r.Chart.Metadata.Name + "-" + r.Chart.Metadata.Version, r.Chart.Metadata.AppVersion, r.Info.Description})
	}
	printTable(rows)
	return nil
}

type upgradeOpts struct{ install, reuse, reset bool }

// upgrade renders the chart as the release's next revision and moves the live
// objects from the deployed revision's manifest to the new one.
func upgrade(name, dir, ns string, files, sets []string, opts upgradeOpts) error {
	k, err := connect()
	if err != nil {
		return err
	}
	h, err := k.history(ns, name)
	if err != nil {
		return err
	}
	if opts.install && (len(h) == 0 || h[len(h)-1].Info.Status == "uninstalled") {
		fmt.Printf("Release %q does not exist. Installing it now.\n", name)
		return install(name, dir, ns, files, sets, len(h) > 0)
	}
	if err := upgradeRelease(k, h, name, dir, ns, files, sets, opts); err != nil {
		return fmt.Errorf("UPGRADE FAILED: %w", err)
	}
	return nil
}

func upgradeRelease(k *kube, h []*Record, name, dir, ns string, files, sets []string, opts upgradeOpts) error {
	if len(h) == 0 {
		return fmt.Errorf("%q has no deployed releases", name)
	}
	last := h[len(h)-1]
	// The patch starts from the deployed revision; with none, from the last
	// if it failed or was superseded.
	var current *Record
	for _, r := range h {
		if r.Info.Status == "deployed" {
			current = r
		}
	}
	if current == nil {
		if last.Info.Status != "failed" && last.Info.Status != "superseded" {
			return fmt.Errorf("%q has no deployed releases", name)
		}
		current = last
	}
	user, err := userValues(files, sets)
	if err != nil {
		return err
	}
	switch {
	case opts.reset:
	case opts.reuse:
		under(user, current.Config)
	case len(user) == 0:
		user = current.Config
	}
	rel := Release{Name: name, Namespace: ns, Revision: last.Version + 1, IsUpgrade: true, Service: "Helm"}
	r, err := renderWith(dir, user, rel)
	if err != nil {
		return err
	}
	target, err := k.objects(r.Manifests, ns)
	if err != nil {
		return err
	}
	old, err := k.objects(parseManifest(current.Manifest), ns)
	if err != nil {
		return err
	}
	// Only objects the release does not have yet are checked for ownership.
	orig := map[string]*unstructured.Unstructured{}
	for _, o := range old {
		orig[objectKey(o.obj)] = o.obj
	}
	for _, o := range target {
		if orig[objectKey(o.obj)] != nil {
			continue
		}
		live, err := k.get(o)
		if err != nil {
			return err
		}
		if live != nil {
			if err := owned(live, name, ns); err != nil {
				return fmt.Errorf("Unable to continue with update: %w", err)
			}
		}
	}
	rec := &Record{
		Name: name, Namespace: ns, Version: rel.Revision,
		Info: &Info{FirstDeployed: current.Info.FirstDeployed, LastDeployed: Time{time.Now()},
			Status: "pending-upgrade", Description: "Preparing upgrade", Notes: r.Notes},
		Chart:    storedChart(r.Chart),
		Config:   user,
		Manifest: manifestText(r.Manifests),
		Hooks:    r.Hooks,
	}
	if err := k.saveRecord(rec, false); err != nil {
		return err
	}
	fail := func(err error) error {
		rec.Info.Status = "failed"
		rec.Info.Description = fmt.Sprintf("Upgrade %q failed: %s", name, err)
		if serr := k.saveRecord(rec, true); serr != nil {
			return serr
		}
		return err
	}
	if err := k.execHooks(rec, "pre-upgrade"); err != nil {
		return fail(fmt.Errorf("pre-upgrade hooks failed: %s", err))
	}
	if err := k.update(old, target, name, ns); err != nil {
		return fail(err)
	}
	if err := k.execHooks(rec, "post-upgrade"); err != nil {
		return fail(fmt.Errorf("post-upgrade hooks failed: %s", err))
	}
	current.Info.Status = "superseded"
	if err := k.saveRecord(current, true); err != nil {
		return err
	}
	rec.Info.Status, rec.Info.Description = "deployed", "Upgrade complete"
	if err := k.saveRecord(rec, true); err != nil {
		return err
	}
	fmt.Printf("Release %q has been upgraded. Happy Helming!\n", name)
	printRelease(rec)
	return nil
}

// under fills dst with the keys of src it lacks, merging nested maps; what dst
// already says wins.
func under(dst, src map[string]any) {
	for k, v := range src {
		dv, ok := dst[k]
		if !ok {
			dst[k] = v
			continue
		}
		dm, dok := dv.(map[string]any)
		sm, sok := v.(map[string]any)
		if dok && sok {
			under(dm, sm)
		}
	}
}

// parseManifest splits a recorded manifest back into its documents.
func parseManifest(text string) []File {
	var out []File
	for _, d := range docSep.Split(strings.TrimSpace(text), -1) {
		if strings.TrimSpace(d) != "" {
			out = append(out, File{Name: "manifest", Data: []byte(d)})
		}
	}
	return out
}

func objectKey(o *unstructured.Unstructured) string {
	return o.GroupVersionKind().GroupKind().String() + "/" + o.GetNamespace() + "/" + o.GetName()
}

// update moves the cluster from the old objects to the target ones: creates
// what is missing, patches what exists with a three-way merge, then deletes
// what the target no longer has.
func (k *kube) update(old, target []object, name, ns string) error {
	orig := map[string]*unstructured.Unstructured{}
	for _, o := range old {
		orig[objectKey(o.obj)] = o.obj
	}
	keep := map[string]bool{}
	for _, o := range target {
		keep[objectKey(o.obj)] = true
		mark(o.obj, name, ns)
		live, err := k.get(o)
		if err != nil {
			return err
		}
		if live == nil {
			if _, err := o.res.Create(k.ctx, o.obj, metav1.CreateOptions{FieldManager: "helm"}); err != nil {
				return fmt.Errorf("failed to create resource: %w", err)
			}
			continue
		}
		// An object the release adopts starts from what is live.
		original := orig[objectKey(o.obj)]
		if original == nil {
			original = live
		}
		if err := k.patch(o, original, live); err != nil {
			return fmt.Errorf("cannot patch %q with kind %s: %w", o.obj.GetName(), o.obj.GetKind(), err)
		}
	}
	for _, o := range old {
		if keep[objectKey(o.obj)] {
			continue
		}
		if err := o.res.Delete(k.ctx, o.obj.GetName(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// patch is Helm's three-way merge: the change from the old manifest to the new
// one, computed against the live object, so that a field only the cluster
// knows survives while a field the chart declares is reset to the chart's
// value. Built-in kinds get a strategic merge patch, anything else a JSON
// merge patch.
func (k *kube) patch(o object, original, live *unstructured.Unstructured) error {
	oj, err := json.Marshal(original.Object)
	if err != nil {
		return err
	}
	mj, err := json.Marshal(o.obj.Object)
	if err != nil {
		return err
	}
	lj, err := json.Marshal(live.Object)
	if err != nil {
		return err
	}
	var p []byte
	pt := types.StrategicMergePatchType
	if typed, err := scheme.Scheme.New(o.obj.GroupVersionKind()); err == nil {
		meta, err := strategicpatch.NewPatchMetaFromStruct(typed)
		if err != nil {
			return err
		}
		p, err = strategicpatch.CreateThreeWayMergePatch(oj, mj, lj, meta, true)
		if err != nil {
			return err
		}
	} else {
		pt = types.MergePatchType
		if p, err = jsonmergepatch.CreateThreeWayJSONMergePatch(oj, mj, lj); err != nil {
			return err
		}
	}
	if string(p) == "{}" {
		return nil
	}
	_, err = o.res.Patch(k.ctx, o.obj.GetName(), pt, p, metav1.PatchOptions{FieldManager: "helm"})
	return err
}

// rollback records the next revision as a copy of revision to (the one below
// the latest when to is 0) and moves the cluster from the latest revision's
// manifest to it.
func rollback(name, ns string, to int) error {
	k, err := connect()
	if err != nil {
		return err
	}
	h, err := k.history(ns, name)
	if err != nil {
		return err
	}
	if len(h) == 0 {
		return errNotFound
	}
	current := h[len(h)-1]
	if to == 0 {
		to = current.Version - 1
	}
	var prev *Record
	for _, r := range h {
		if r.Version == to {
			prev = r
		}
	}
	if prev == nil {
		return fmt.Errorf("release has no %d version", to)
	}
	rec := &Record{
		Name: name, Namespace: ns, Version: current.Version + 1,
		Info: &Info{FirstDeployed: current.Info.FirstDeployed, LastDeployed: Time{time.Now()},
			Status: "pending-rollback", Description: fmt.Sprintf("Rollback to %d", to), Notes: prev.Info.Notes},
		Chart:    prev.Chart,
		Config:   prev.Config,
		Manifest: prev.Manifest,
	}
	// The target's stored hooks run again, not a fresh render of them.
	for _, h := range prev.Hooks {
		c := *h
		rec.Hooks = append(rec.Hooks, &c)
	}
	if err := k.saveRecord(rec, false); err != nil {
		return err
	}
	old, err := k.objects(parseManifest(current.Manifest), ns)
	if err != nil {
		return err
	}
	target, err := k.objects(parseManifest(prev.Manifest), ns)
	if err != nil {
		return err
	}
	// ponytail: a failed rollback hook leaves the revision pending-rollback, as Helm does.
	if err := k.execHooks(rec, "pre-rollback"); err != nil {
		return err
	}
	if err := k.update(old, target, name, ns); err != nil {
		// Unlike a failed upgrade, a failed rollback supersedes the latest.
		current.Info.Status = "superseded"
		rec.Info.Status = "failed"
		rec.Info.Description = fmt.Sprintf("Rollback %q failed: %s", name, err)
		for _, r := range []*Record{current, rec} {
			if serr := k.saveRecord(r, true); serr != nil {
				return serr
			}
		}
		return err
	}
	if err := k.execHooks(rec, "post-rollback"); err != nil {
		return err
	}
	for _, r := range h {
		if r.Info.Status == "deployed" {
			r.Info.Status = "superseded"
			if err := k.saveRecord(r, true); err != nil {
				return err
			}
		}
	}
	rec.Info.Status = "deployed"
	if err := k.saveRecord(rec, true); err != nil {
		return err
	}
	fmt.Println("Rollback was a success! Happy Helming!")
	return nil
}

// Hook is Helm's release.Hook: one document annotated helm.sh/hook.
type Hook struct {
	Name           string        `json:"name,omitempty"`
	Kind           string        `json:"kind,omitempty"`
	Path           string        `json:"path,omitempty"`
	Manifest       string        `json:"manifest,omitempty"`
	Events         []string      `json:"events,omitempty"`
	LastRun        HookExecution `json:"last_run"`
	Weight         int           `json:"weight,omitempty"`
	DeletePolicies []string      `json:"delete_policies,omitempty"`
}

type HookExecution struct {
	StartedAt   Time   `json:"started_at"`
	CompletedAt Time   `json:"completed_at"`
	Phase       string `json:"phase"`
}

var hookEvents = map[string]string{
	"pre-install": "pre-install", "post-install": "post-install",
	"pre-delete": "pre-delete", "post-delete": "post-delete",
	"pre-upgrade": "pre-upgrade", "post-upgrade": "post-upgrade",
	"pre-rollback": "pre-rollback", "post-rollback": "post-rollback",
	"test": "test", "test-success": "test",
}

// hookTimeout bounds the wait for each hook to be ready.
var hookTimeout time.Duration

// splitHooks takes the hooks out of ordered manifests, keeping both in that
// order. A document naming an event Helm does not know is dropped entirely.
func splitHooks(ms []File) ([]File, []*Hook) {
	var plain []File
	var hooks []*Hook
	for _, m := range ms {
		var head struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name        string            `json:"name"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		yaml.Unmarshal(m.Data, &head)
		a := head.Metadata.Annotations
		events, ok := a["helm.sh/hook"]
		if !ok {
			plain = append(plain, m)
			continue
		}
		h := &Hook{Name: head.Metadata.Name, Kind: head.Kind, Path: m.Name, Manifest: string(m.Data)}
		for _, e := range strings.Split(events, ",") {
			if ev, ok := hookEvents[strings.ToLower(strings.TrimSpace(e))]; ok {
				h.Events = append(h.Events, ev)
			} else {
				h = nil
				break
			}
		}
		if h == nil {
			fmt.Fprintf(os.Stderr, "info: skipping unknown hook: %q\n", events)
			continue
		}
		// Helm ignores a weight that is not an integer.
		h.Weight, _ = strconv.Atoi(a["helm.sh/hook-weight"])
		if p, ok := a["helm.sh/hook-delete-policy"]; ok {
			for _, v := range strings.Split(p, ",") {
				h.DeletePolicies = append(h.DeletePolicies, strings.ToLower(strings.TrimSpace(v)))
			}
		}
		hooks = append(hooks, h)
	}
	return plain, hooks
}

func hookText(hs []*Hook) string {
	var b strings.Builder
	for _, h := range hs {
		fmt.Fprintf(&b, "---\n# Source: %s\n%s\n", h.Path, h.Manifest)
	}
	return b.String()
}

// oneError is the multierror text Helm wraps a single hook error in.
func oneError(err error) error {
	return fmt.Errorf("1 error occurred:\n\t* %s\n\n", err)
}

// execHooks runs a release's hooks for one event, lowest weight first, then
// by name, each created without ownership marks and waited on. Every hook
// that runs is recorded in rec, which the caller saves.
func (k *kube) execHooks(rec *Record, event string) error {
	var hs []*Hook
	for _, h := range rec.Hooks {
		if slices.Contains(h.Events, event) {
			hs = append(hs, h)
		}
	}
	sort.SliceStable(hs, func(i, j int) bool {
		if hs[i].Weight != hs[j].Weight {
			return hs[i].Weight < hs[j].Weight
		}
		return hs[i].Name < hs[j].Name
	})
	for _, h := range hs {
		if len(h.DeletePolicies) == 0 {
			h.DeletePolicies = []string{"before-hook-creation"}
		}
		if err := k.deleteHook(rec.Namespace, h, "before-hook-creation"); err != nil {
			return err
		}
		objs, err := k.objects([]File{{Name: h.Path, Data: []byte(h.Manifest)}}, rec.Namespace)
		if err != nil {
			return fmt.Errorf("unable to build kubernetes object for %s hook %s: %w", event, h.Path, err)
		}
		h.LastRun = HookExecution{StartedAt: Time{time.Now()}, Phase: "Running"}
		if _, err := objs[0].res.Create(k.ctx, objs[0].obj, metav1.CreateOptions{FieldManager: "helm"}); err != nil {
			h.LastRun.CompletedAt, h.LastRun.Phase = Time{time.Now()}, "Failed"
			return fmt.Errorf("warning: Hook %s %s failed: %w", event, h.Path, oneError(err))
		}
		err = k.waitHook(objs[0])
		h.LastRun.CompletedAt = Time{time.Now()}
		if err != nil {
			h.LastRun.Phase = "Failed"
			k.deleteHook(rec.Namespace, h, "hook-failed")
			return oneError(err)
		}
		h.LastRun.Phase = "Succeeded"
	}
	for i := len(hs) - 1; i >= 0; i-- {
		if err := k.deleteHook(rec.Namespace, hs[i], "hook-succeeded"); err != nil {
			return err
		}
	}
	return nil
}

// waitHook waits for a Job to complete or a Pod to succeed; any other kind is
// ready once created.
func (k *kube) waitHook(o object) error {
	deadline := time.Now().Add(hookTimeout)
	for {
		live, err := o.res.Get(k.ctx, o.obj.GetName(), metav1.GetOptions{})
		if err != nil {
			return err
		}
		switch o.obj.GetKind() {
		case "Job":
			conds, _, _ := unstructured.NestedSlice(live.Object, "status", "conditions")
			for _, c := range conds {
				c, _ := c.(map[string]any)
				if c["status"] != "True" {
					continue
				}
				switch c["type"] {
				case "Complete":
					return nil
				case "Failed":
					return fmt.Errorf("job %s failed: %s", o.obj.GetName(), c["reason"])
				}
			}
		case "Pod":
			switch phase, _, _ := unstructured.NestedString(live.Object, "status", "phase"); phase {
			case "Succeeded":
				return nil
			case "Failed":
				return fmt.Errorf("pod %s failed", o.obj.GetName())
			}
		default:
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for the condition")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// deleteHook deletes a hook's object if the hook has this delete policy, and
// waits for it to be gone so that the next create does not collide with it.
func (k *kube) deleteHook(ns string, h *Hook, policy string) error {
	if h.Kind == "CustomResourceDefinition" || !slices.Contains(h.DeletePolicies, policy) {
		return nil
	}
	// The object is decoded the way the API server would, so a value the
	// chart left untyped, such as an unquoted weight, fails here.
	j, err := yaml.YAMLToJSON([]byte(h.Manifest))
	if err == nil {
		var typed struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		}
		err = json.Unmarshal(j, &typed)
	}
	if err != nil {
		return fmt.Errorf("unable to build kubernetes object for deleting hook %s: unable to decode \"\": %w", h.Path, err)
	}
	objs, err := k.objects([]File{{Name: h.Path, Data: []byte(h.Manifest)}}, ns)
	if err != nil {
		return fmt.Errorf("unable to build kubernetes object for deleting hook %s: %w", h.Path, err)
	}
	o := objs[0]
	bg := metav1.DeletePropagationBackground
	if err := o.res.Delete(k.ctx, o.obj.GetName(), metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	deadline := time.Now().Add(hookTimeout)
	for {
		if live, err := k.get(o); err != nil || live == nil {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for the condition")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// releaseTest runs the latest revision's test hooks, in place: no new
// revision, the same record rewritten. A filter that picks hooks moves the
// ones it skipped to the front of the stored list, as Helm does.
func releaseTest(name, ns string, filters []string, logs bool) error {
	k, err := connect()
	if err != nil {
		return err
	}
	rec, err := revisionOf(ns, name, 0)
	if err != nil {
		return err
	}
	var include, exclude []string
	for _, f := range filters {
		if n, ok := strings.CutPrefix(f, "!name="); ok {
			exclude = append(exclude, n)
		} else if n, ok := strings.CutPrefix(f, "name="); ok {
			include = append(include, n)
		}
	}
	picks := func(h *Hook) bool {
		return !slices.Contains(exclude, h.Name) && (len(include) == 0 || slices.Contains(include, h.Name))
	}
	var run, skipped []*Hook
	for _, h := range rec.Hooks {
		if picks(h) {
			run = append(run, h)
		} else {
			skipped = append(skipped, h)
		}
	}
	rec.Hooks = run
	runErr := k.execHooks(rec, "test")
	rec.Hooks = append(skipped, rec.Hooks...)
	if err := k.saveRecord(rec, true); err != nil {
		return err
	}
	printRelease(rec)
	if logs {
		fmt.Println()
		// Every picked test hook, run this time or not, lowest weight first.
		hs := slices.Clone(rec.Hooks)
		sort.SliceStable(hs, func(i, j int) bool {
			if hs[i].Weight != hs[j].Weight {
				return hs[i].Weight < hs[j].Weight
			}
			return hs[i].Name < hs[j].Name
		})
		for _, h := range hs {
			if !slices.Contains(h.Events, "test") || !picks(h) {
				continue
			}
			r, err := k.cs.CoreV1().Pods(ns).GetLogs(h.Name, &corev1.PodLogOptions{}).Stream(k.ctx)
			if err != nil {
				return fmt.Errorf("unable to get pod logs for %s: %w", h.Name, err)
			}
			fmt.Printf("POD LOGS: %s\n", h.Name)
			_, err = io.Copy(os.Stdout, r)
			r.Close()
			fmt.Println()
			if err != nil {
				return fmt.Errorf("unable to write pod logs for %s: %w", h.Name, err)
			}
		}
	}
	return runErr
}

// uninstallOrder is Helm's UninstallOrder, which is not quite installOrder
// reversed: Services go before the workloads.
var uninstallOrder = []string{
	"APIService", "Ingress", "IngressClass", "Service", "CronJob", "Job",
	"StatefulSet", "HorizontalPodAutoscaler", "Deployment", "ReplicaSet",
	"ReplicationController", "Pod", "DaemonSet", "RoleBindingList",
	"RoleBinding", "RoleList", "Role", "ClusterRoleBindingList",
	"ClusterRoleBinding", "ClusterRoleList", "ClusterRole",
	"CustomResourceDefinition", "PersistentVolumeClaim", "PersistentVolume",
	"StorageClass", "ConfigMap", "SecretList", "Secret", "ServiceAccount",
	"PodDisruptionBudget", "PodSecurityPolicy", "LimitRange", "ResourceQuota",
	"NetworkPolicy", "Namespace", "PriorityClass",
}

// uninstall removes each release in turn, stopping at the first error.
func uninstall(names []string, ns string, keepHistory, ignoreNotFound, dryRun bool) error {
	k, err := connect()
	if err != nil {
		return err
	}
	for _, name := range names {
		h, err := k.history(ns, name)
		if err != nil {
			return err
		}
		switch {
		case len(h) == 0 && dryRun:
			return errNotFound
		case len(h) == 0 && !ignoreNotFound:
			return fmt.Errorf("uninstall: Release not loaded: %s: %w", name, errNotFound)
		case len(h) > 0 && !dryRun:
			kept, err := uninstallRelease(k, h, keepHistory)
			if err != nil {
				return err
			}
			if kept != "" {
				fmt.Println("These resources were kept due to the resource policy:\n" + kept)
			}
		}
		fmt.Printf("release %q uninstalled\n", name)
	}
	return nil
}

// uninstallRelease runs the latest revision's delete hooks around deleting
// its objects, and returns the "[Kind] name" lines of those it kept.
func uninstallRelease(k *kube, h []*Record, keepHistory bool) (string, error) {
	rel := h[len(h)-1]
	if rel.Info.Status == "uninstalled" {
		if keepHistory {
			return "", fmt.Errorf("the release named %q is already deleted", rel.Name)
		}
		return "", k.purge(h)
	}
	rel.Info.Status, rel.Info.Deleted = "uninstalling", Time{time.Now()}
	rel.Info.Description = "Deletion in progress (or silently failed)"
	if err := k.execHooks(rel, "pre-delete"); err != nil {
		return "", err
	}
	if err := k.saveRecord(rel, true); err != nil {
		return "", err
	}

	rank := map[string]int{}
	for i, kind := range uninstallOrder {
		rank[kind] = i
	}
	type doc struct {
		kind, name, key, policy string
		file                    File
	}
	var docs []doc
	for i, m := range parseManifest(rel.Manifest) {
		var head struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name        string            `json:"name"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		yaml.Unmarshal(m.Data, &head)
		// Helm names the documents manifest-0, manifest-1, ... and orders
		// those of one kind by that name, as text.
		docs = append(docs, doc{head.Kind, head.Metadata.Name, fmt.Sprintf("manifest-%d", i),
			strings.ToLower(strings.TrimSpace(head.Metadata.Annotations["helm.sh/resource-policy"])), m})
	}
	sort.SliceStable(docs, func(i, j int) bool {
		a, b := docs[i], docs[j]
		ra, aok := rank[a.kind]
		rb, bok := rank[b.kind]
		switch {
		case a.kind == b.kind:
			return a.key < b.key
		case !aok && !bok:
			return a.kind < b.kind
		case !aok || !bok:
			return aok
		}
		return ra < rb
	})
	var kept string
	var doomed []File
	for _, d := range docs {
		if d.policy == "keep" {
			kept += "[" + d.kind + "] " + d.name + "\n"
		} else {
			doomed = append(doomed, d.file)
		}
	}
	objs, err := k.objects(doomed, rel.Namespace)
	if err != nil {
		return "", err
	}
	bg := metav1.DeletePropagationBackground
	for _, o := range objs {
		if err := o.res.Delete(k.ctx, o.obj.GetName(), metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil && !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("failed to delete release: %s", rel.Name)
		}
	}

	var errs []string
	if err := k.execHooks(rel, "post-delete"); err != nil {
		errs = append(errs, err.Error())
	}
	rel.Info.Status, rel.Info.Description = "uninstalled", "Uninstallation complete"
	if keepHistory {
		err = k.saveRecord(rel, true)
	} else {
		err = k.purge(h)
	}
	if err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return kept, fmt.Errorf("uninstallation completed with %d error(s): %s", len(errs), strings.Join(errs, "; "))
	}
	return kept, nil
}

// purge deletes the Secret of every revision.
func (k *kube) purge(h []*Record) error {
	for _, r := range h {
		err := k.cs.CoreV1().Secrets(r.Namespace).Delete(k.ctx, fmt.Sprintf("sh.helm.release.v1.%s.v%d", r.Name, r.Version), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// Lock is Chart.lock: the versions dependency update resolved, and a digest
// of Chart.yaml's dependencies with them, which tells build the two still agree.
type Lock struct {
	Generated    time.Time    `json:"generated"`
	Digest       string       `json:"digest"`
	Dependencies []Dependency `json:"dependencies"`
}

// runPackaging is the commands that work on chart directories and archives
// and never reach a cluster.
func runPackaging(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	dest := fs.String("d", ".", "where to write the archive")
	fs.StringVar(dest, "destination", ".", "where to write the archive")
	version := fs.String("version", "", "set the chart's version")
	appVersion := fs.String("app-version", "", "set the chart's appVersion")
	var update bool
	fs.BoolVar(&update, "u", false, "update dependencies before packaging")
	fs.BoolVar(&update, "dependency-update", false, "update dependencies before packaging")
	if err := fs.Parse(args); err != nil {
		return err
	}
	args = fs.Args()
	if cmd == "package" {
		if len(args) == 0 {
			return fmt.Errorf(`"helm package" requires at least 1 argument`)
		}
		for _, dir := range args {
			if update {
				if err := depUpdate(dir, io.Discard); err != nil {
					return err
				}
			}
			name, err := packageChart(dir, *dest, *version, *appVersion)
			if err != nil {
				return err
			}
			fmt.Printf("Successfully packaged chart and saved it to: %s\n", name)
		}
		return nil
	}
	if len(args) > 1 {
		return fmt.Errorf(`"helm %s" accepts at most 1 argument`, cmd)
	}
	dir := "."
	if len(args) == 1 {
		dir = filepath.Clean(args[0])
	}
	switch cmd {
	case "dependency list":
		return depList(dir)
	case "dependency update":
		return depUpdate(dir, os.Stdout)
	case "dependency build":
		return depBuild(dir, os.Stdout)
	}
	return fmt.Errorf("unknown command %q", cmd)
}

// depList prints each dependency with what charts/ holds for it, then warns
// about charts in charts/ that Chart.yaml does not declare.
func depList(dir string) error {
	c, err := load(dir)
	if err != nil {
		return err
	}
	if c.Metadata.Dependencies == nil {
		fmt.Printf("WARNING: no dependencies at %s\n", filepath.Join(dir, "charts"))
		return nil
	}
	rows := [][]string{{"NAME", "VERSION", "REPOSITORY", "STATUS"}}
	for _, d := range c.Metadata.Dependencies {
		rows = append(rows, []string{d.Name, d.Version, d.Repository, depStatus(dir, d, c)})
	}
	printTable(rows)
	fmt.Println()
	found, _ := filepath.Glob(filepath.Join(dir, "charts/*"))
	for _, f := range found {
		fi, err := os.Stat(f)
		if err != nil || (!fi.IsDir() && filepath.Ext(f) != ".tgz") {
			continue
		}
		sub, err := load(f)
		if err != nil {
			fmt.Printf("WARNING: %q is not a chart.\n", f)
			continue
		}
		if !slices.ContainsFunc(c.Metadata.Dependencies, func(d Dependency) bool { return d.Name == sub.Metadata.Name }) {
			fmt.Printf("WARNING: %q is not in Chart.yaml.\n", f)
		}
	}
	return nil
}

// depStatus is Helm's verdict on one dependency: an archive named for it is
// checked first, then an unpacked directory.
func depStatus(dir string, d Dependency, parent *Chart) string {
	archives, err := filepath.Glob(filepath.Join(dir, "charts", d.Name+"-*.tgz"))
	if err != nil {
		return "bad pattern"
	}
	var semverNamed []string
	for _, a := range archives {
		v := strings.TrimPrefix(strings.TrimSuffix(filepath.Base(a), ".tgz"), d.Name+"-")
		if _, err := semver.StrictNewVersion(v); err == nil {
			semverNamed = append(semverNamed, a)
		}
	}
	switch {
	case len(archives) == 1:
		semverNamed = archives
	case len(semverNamed) > 1:
		return "too many matches"
	}
	if len(semverNamed) == 1 {
		sub, err := load(semverNamed[0])
		switch {
		case err != nil:
			return "corrupt"
		case sub.Metadata.Name != d.Name:
			return "misnamed"
		}
		if s := versionStatus(sub.Metadata.Version, d.Version); s != "" {
			return s
		}
		return "ok"
	}
	for _, sub := range parent.Subcharts {
		if sub.Metadata.Name == d.Name {
			if s := versionStatus(sub.Metadata.Version, d.Version); s != "" {
				return s
			}
			return "unpacked"
		}
	}
	return "missing"
}

// versionStatus is "" when version satisfies the constraint.
func versionStatus(version, constraint string) string {
	if version == constraint {
		return ""
	}
	c, err := semver.NewConstraint(constraint)
	if err != nil {
		return "invalid version"
	}
	v, err := semver.NewVersion(version)
	if err != nil {
		return "invalid version"
	}
	if !c.Check(v) {
		return "wrong version"
	}
	return ""
}

// chartDir loads a chart that dependency update or build may write into.
func chartDir(dir string) (*Chart, error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("could not find %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return nil, errors.New("only unpacked charts can be updated")
	}
	return load(dir)
}

// depUpdate resolves every dependency to a version, fetches them into
// charts/, and writes Chart.lock unless its digest is unchanged.
func depUpdate(dir string, out io.Writer) error {
	c, err := chartDir(dir)
	if err != nil {
		return err
	}
	reqs := c.Metadata.Dependencies
	if reqs == nil {
		return nil
	}
	if err := checkRepos(dir, reqs); err != nil {
		return err
	}
	updateRepos(reqs, out)
	lock, err := resolveDeps(dir, reqs)
	if err != nil {
		return err
	}
	if err := fetchDeps(dir, lock.Dependencies, out); err != nil {
		return err
	}
	if c.Lock != nil && c.Lock.Digest == lock.Digest {
		return nil
	}
	data, err := yaml.Marshal(lock)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "Chart.lock"), data, 0o644)
}

// depBuild fetches the locked versions, refusing a lock whose digest no
// longer matches Chart.yaml. Without a lock it is an update.
func depBuild(dir string, out io.Writer) error {
	c, err := chartDir(dir)
	if err != nil {
		return err
	}
	if c.Lock == nil {
		return depUpdate(dir, out)
	}
	if err := checkRepos(dir, c.Metadata.Dependencies); err != nil {
		return err
	}
	sum, err := hashDeps(c.Metadata.Dependencies, c.Lock.Dependencies)
	if err != nil {
		return err
	}
	if sum != c.Lock.Digest {
		return errors.New("the lock file (Chart.lock) is out of sync with the dependencies file (Chart.yaml). Please update the dependencies")
	}
	// Build fetches only from repositories `helm repo add` knows, and this
	// program keeps no such list, so a locked chart repository is refused.
	var missing []string
	for _, d := range c.Lock.Dependencies {
		if d.Repository != "" && !strings.HasPrefix(d.Repository, "file://") && !strings.HasPrefix(d.Repository, "oci://") {
			missing = append(missing, d.Repository)
		}
	}
	if missing != nil {
		return fmt.Errorf("no repository definition for %s. Please add the missing repos via 'helm repo add'", strings.Join(missing, ", "))
	}
	return fetchDeps(dir, c.Lock.Dependencies, out)
}

// checkRepos is Helm's first pass over the dependencies: a file://
// repository must exist, and one that is not a URL names a repository
// `helm repo add` would have to know, which this program keeps no list of.
func checkRepos(dir string, reqs []Dependency) error {
	var missing []string
	note := false
	for _, d := range reqs {
		r := d.Repository
		if r == "" {
			continue
		}
		if strings.HasPrefix(r, "file://") {
			if _, err := localPath(r, dir); err != nil {
				return err
			}
			continue
		}
		if _, err := url.ParseRequestURI(r); err == nil {
			continue
		}
		missing = append(missing, r)
		if !strings.Contains(r, "//") && !strings.HasPrefix(r, "@") && !strings.HasPrefix(r, "alias:") {
			note = true
		}
	}
	if missing == nil {
		return nil
	}
	msg := fmt.Sprintf("no repository definition for %s. Please add them via 'helm repo add'", strings.Join(missing, ", "))
	if note {
		msg += `
Note that repositories must be URLs or aliases. For example, to refer to the "example"
repository, use "https://charts.example.com/" or "@example" instead of
"example". Don't forget to add the repo, too ('helm repo add').`
	}
	return errors.New(msg)
}

func isRemote(repo string) bool {
	return strings.HasPrefix(repo, "http://") || strings.HasPrefix(repo, "https://")
}

// Index is a chart repository's index.yaml: every version of every chart,
// with the URLs its archive can be fetched from.
type Index struct {
	APIVersion string `json:"apiVersion"`
	Entries    map[string][]struct {
		Version string   `json:"version"`
		URLs    []string `json:"urls"`
	} `json:"entries"`
}

// repoCache is where Helm keeps downloaded repository indexes.
func repoCache() string {
	if d := os.Getenv("HELM_REPOSITORY_CACHE"); d != "" {
		return d
	}
	if d := os.Getenv("HELM_CACHE_HOME"); d != "" {
		return filepath.Join(d, "repository")
	}
	d, _ := os.UserCacheDir()
	return filepath.Join(d, "helm", "repository")
}

// managerName is the name Helm files a repository it was never told about
// under: helm-manager- and the sha256 of its URL as written.
func managerName(repo string) string {
	return fmt.Sprintf("helm-manager-%x", sha256.Sum256([]byte(repo)))
}

// fetch GETs a URL, wanting 200.
func fetch(u string) ([]byte, error) {
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch %s : %s", u, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// updateRepos downloads the index of each dependency's chart repository into
// the cache. A failure is reported and passed over: resolving the dependency
// then finds no cached index. Helm fetches them in parallel, so the lines
// come in no fixed order, and a dependency whose name an earlier one already
// has is skipped.
func updateRepos(reqs []Dependency, out io.Writer) {
	var repos []string
	seen := map[string]bool{}
	for _, d := range reqs {
		if isRemote(d.Repository) && !seen[d.Name] {
			seen[d.Name] = true
			repos = append(repos, d.Repository)
		}
	}
	if repos == nil {
		return
	}
	fmt.Fprintln(out, "Getting updates for unmanaged Helm repositories...")
	for _, r := range repos {
		data, err := fetch(strings.TrimSuffix(r, "/") + "/index.yaml")
		if err == nil {
			err = parseIndex(data, &Index{})
		}
		if err == nil {
			err = os.MkdirAll(repoCache(), 0o755)
		}
		if err == nil {
			err = os.WriteFile(filepath.Join(repoCache(), managerName(r)+"-index.yaml"), data, 0o644)
		}
		if err != nil {
			fmt.Fprintf(out, "...Unable to get an update from the %q chart repository:\n\t%s\n", r, err)
			continue
		}
		fmt.Fprintf(out, "...Successfully got an update from the %q chart repository\n", r)
	}
}

// parseIndex reads an index, newest version of each chart first.
func parseIndex(data []byte, idx *Index) error {
	if err := yaml.Unmarshal(data, idx); err != nil {
		return err
	}
	if idx.APIVersion == "" {
		return errors.New("no API version specified")
	}
	for _, vs := range idx.Entries {
		sort.SliceStable(vs, func(i, j int) bool {
			a, errA := semver.NewVersion(vs[i].Version)
			b, errB := semver.NewVersion(vs[j].Version)
			return errA == nil && (errB != nil || a.GreaterThan(b))
		})
	}
	return nil
}

// cachedIndex is a repository's index as the last update saved it.
func cachedIndex(repo string) (*Index, error) {
	name := managerName(repo)
	data, err := os.ReadFile(filepath.Join(repoCache(), name+"-index.yaml"))
	if err == nil {
		idx := &Index{}
		if err = parseIndex(data, idx); err == nil {
			return idx, nil
		}
	}
	return nil, fmt.Errorf("no cached repository for %s found. (try 'helm repo update'): %w", name, err)
}

// localPath resolves a file:// repository against the chart's directory.
func localPath(repo, dir string) (string, error) {
	p := strings.TrimPrefix(repo, "file://")
	if !strings.HasPrefix(p, "/") {
		p = filepath.Join(dir, p)
	}
	if _, err := os.Stat(p); errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("directory %s not found", p)
	} else if err != nil {
		return "", err
	}
	return p, nil
}

// resolveDeps picks a version for each dependency. A file:// one is whatever
// version that chart has, if it satisfies the constraint; one with no
// repository must already be unpacked in charts/ and keeps its constraint.
func resolveDeps(dir string, reqs []Dependency) (*Lock, error) {
	locked := make([]Dependency, len(reqs))
	var missing []string
	for i, d := range reqs {
		constraint, err := semver.NewConstraint(d.Version)
		if err != nil {
			return nil, fmt.Errorf("dependency %q has an invalid version/constraint format: %w", d.Name, err)
		}
		if d.Repository == "" {
			if _, err := localPath(filepath.Join("charts", d.Name), dir); err != nil {
				return nil, err
			}
			locked[i] = Dependency{Name: d.Name, Version: d.Version}
			continue
		}
		if isRemote(d.Repository) {
			idx, err := cachedIndex(d.Repository)
			if err != nil {
				return nil, err
			}
			vs, ok := idx.Entries[d.Name]
			if !ok {
				return nil, fmt.Errorf("%s chart not found in repo %s", d.Name, d.Repository)
			}
			locked[i] = Dependency{Name: d.Name, Repository: d.Repository}
			for _, e := range vs {
				if v, err := semver.NewVersion(e.Version); err == nil && len(e.URLs) > 0 && constraint.Check(v) {
					locked[i].Version = v.Original()
					break
				}
			}
			if locked[i].Version == "" {
				missing = append(missing, fmt.Sprintf("%q (repository %q, version %q)", d.Name, d.Repository, d.Version))
			}
			continue
		}
		p, err := localPath(d.Repository, dir)
		if err != nil {
			return nil, err
		}
		sub, err := load(p)
		if err != nil {
			return nil, err
		}
		v, err := semver.NewVersion(sub.Metadata.Version)
		if err != nil {
			continue
		}
		if !constraint.Check(v) {
			missing = append(missing, fmt.Sprintf("%q (repository %q, version %q)", d.Name, d.Repository, d.Version))
			continue
		}
		locked[i] = Dependency{Name: d.Name, Repository: d.Repository, Version: sub.Metadata.Version}
	}
	if missing != nil {
		return nil, fmt.Errorf("can't get a valid version for %d subchart(s): %s. Make sure a matching chart version exists in the repo, or change the version constraint in Chart.yaml", len(missing), strings.Join(missing, ", "))
	}
	sum, err := hashDeps(reqs, locked)
	if err != nil {
		return nil, err
	}
	return &Lock{Generated: time.Now(), Digest: sum, Dependencies: locked}, nil
}

// hashDeps is Helm's lock digest: sha256 of the declared and the locked
// dependencies, as one JSON array of the two lists.
func hashDeps(reqs, locked []Dependency) (string, error) {
	data, err := json.Marshal([2][]Dependency{reqs, locked})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), nil
}

// fetchDeps packages each file:// dependency into charts/ and then deletes
// every other archive there, except those of dependencies with no repository.
func fetchDeps(dir string, deps []Dependency, out io.Writer) error {
	dest := filepath.Join(dir, "charts")
	if fi, err := os.Stat(dest); err == nil && !fi.IsDir() {
		return fmt.Errorf("%q is not a directory", dest)
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, fmt.Sprintf("tmpcharts-%d", os.Getpid()))
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	fmt.Fprintf(out, "Saving %d charts\n", len(deps))
	local := map[string]bool{}
	for _, d := range deps {
		if isRemote(d.Repository) {
			fmt.Fprintf(out, "Downloading %s from repo %s\n", d.Name, d.Repository)
			if err := fetchRemote(d, tmp); err != nil {
				fmt.Fprintln(out, "Save error occurred: ", err)
				return err
			}
			continue
		}
		if d.Repository != "" {
			if err := fetchLocal(dir, d, tmp); err != nil {
				fmt.Fprintln(out, "Save error occurred: ", err)
				return err
			}
			continue
		}
		local[d.Name] = true
		fmt.Fprintf(out, "Dependency %s did not declare a repository. Assuming it exists in the charts directory\n", d.Name)
		p := filepath.Join(dest, d.Name)
		sub, err := load(p)
		if err != nil {
			return fmt.Errorf("unable to load chart '%s': %v", p, err)
		}
		if versionStatus(sub.Metadata.Version, d.Version) != "" {
			err := fmt.Errorf("dependency %s at version %s does not satisfy the constraint %s", d.Name, sub.Metadata.Version, d.Version)
			fmt.Fprintln(out, "Save error occurred: ", err)
			return err
		}
	}

	fetched, err := os.ReadDir(tmp)
	if err != nil {
		return err
	}
	present, err := os.ReadDir(dest)
	if err != nil {
		return err
	}
	keep := map[string]bool{}
	for _, f := range fetched {
		keep[f.Name()] = true
		if err := os.Rename(filepath.Join(tmp, f.Name()), filepath.Join(dest, f.Name())); err != nil {
			return err
		}
	}
	fmt.Fprintln(out, "Deleting outdated charts")
	for _, f := range present {
		if f.IsDir() || keep[f.Name()] {
			continue
		}
		p := filepath.Join(dest, f.Name())
		sub, err := load(p)
		if err != nil {
			fmt.Fprintf(out, "Could not verify %s for deletion: %s (Skipping)\n", p, err)
			continue
		}
		if !local[sub.Metadata.Name] {
			if err := os.Remove(p); err != nil {
				return err
			}
		}
	}
	return nil
}

// fetchRemote downloads a locked chart into dest, named as its URL ends. A
// relative URL in the index is relative to the repository.
func fetchRemote(d Dependency, dest string) error {
	idx, err := cachedIndex(d.Repository)
	if err != nil {
		return err
	}
	var ref string
	for _, e := range idx.Entries[d.Name] {
		if e.Version == d.Version && len(e.URLs) > 0 {
			ref = e.URLs[0]
			break
		}
	}
	if ref == "" {
		return fmt.Errorf("chart %s version %s not found in repo %s", d.Name, d.Version, d.Repository)
	}
	base, err := url.Parse(strings.TrimSuffix(d.Repository, "/") + "/")
	if err != nil {
		return err
	}
	u, err := base.Parse(ref)
	if err != nil {
		return err
	}
	data, err := fetch(u.String())
	if err != nil {
		return fmt.Errorf("could not download %s: %w", u, err)
	}
	return os.WriteFile(filepath.Join(dest, path.Base(u.Path)), data, 0o644)
}

// fetchLocal packages a file:// dependency into dest, if its chart still has
// a version the lock or constraint accepts.
func fetchLocal(dir string, d Dependency, dest string) error {
	p, err := localPath(d.Repository, dir)
	if err != nil {
		return err
	}
	sub, err := load(p)
	if err != nil {
		return err
	}
	if versionStatus(sub.Metadata.Version, d.Version) != "" {
		return fmt.Errorf("can't get a valid version for dependency %s", d.Name)
	}
	_, err = saveArchive(sub, dest)
	return err
}

// packageChart writes a chart directory as <name>-<version>.tgz in dest,
// refusing one whose charts/ lacks a declared dependency.
func packageChart(dir, dest, version, appVersion string) (string, error) {
	c, err := load(dir)
	if err != nil {
		return "", err
	}
	if version != "" {
		c.Metadata.Version = version
	}
	if appVersion != "" {
		c.Metadata.AppVersion = appVersion
	}
	if c.Metadata.Dependencies != nil {
		if err := checkDeps(c); err != nil {
			return "", err
		}
	}
	if dest == "." {
		if dest, err = os.Getwd(); err != nil {
			return "", err
		}
	}
	name, err := saveArchive(c, dest)
	if err != nil {
		return "", fmt.Errorf("failed to save: %w", err)
	}
	return name, nil
}

// helmGzipExtra is the extra field Helm writes into every archive's gzip header.
var helmGzipExtra = []byte("+aHR0cHM6Ly95b3V0dS5iZS96OVV6MWljandyTQo=")

// saveArchive writes Helm's archive layout: gzip with Helm's header, and a
// tar of regular files only under a directory named for the chart.
func saveArchive(c *Chart, dest string) (string, error) {
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return "", err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Extra = helmGzipExtra
	zw.Comment = "Helm"
	tw := tar.NewWriter(zw)
	if err := writeChart(tw, c, ""); err != nil {
		return "", err
	}
	if err := tw.Close(); err != nil {
		return "", err
	}
	if err := zw.Close(); err != nil {
		return "", err
	}
	name := filepath.Join(dest, fmt.Sprintf("%s-%s.tgz", c.Metadata.Name, c.Metadata.Version))
	return name, os.WriteFile(name, buf.Bytes(), 0o644)
}

// writeChart is one chart's entries, in Helm's order: Chart.yaml and
// Chart.lock re-serialized, values.yaml as written, the schema, templates,
// other files, then each subchart unpacked under charts/.
func writeChart(tw *tar.Writer, c *Chart, prefix string) error {
	base := filepath.ToSlash(filepath.Join(prefix, c.Metadata.Name))
	put := func(name string, data []byte) error {
		h := &tar.Header{Name: base + "/" + name, Mode: 0o644, Size: int64(len(data)), ModTime: time.Now()}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		_, err := tw.Write(data)
		return err
	}
	meta := c.Metadata
	if meta.APIVersion == "v1" {
		meta.Dependencies = nil
	}
	data, err := yaml.Marshal(meta)
	if err != nil {
		return err
	}
	if err := put("Chart.yaml", data); err != nil {
		return err
	}
	if c.Lock != nil && meta.APIVersion == "v2" {
		data, err := yaml.Marshal(c.Lock)
		if err != nil {
			return err
		}
		if err := put("Chart.lock", data); err != nil {
			return err
		}
	}
	if c.Raw != nil {
		if err := put("values.yaml", c.Raw); err != nil {
			return err
		}
	}
	if c.Schema != nil {
		if !json.Valid(c.Schema) {
			return errors.New("Invalid JSON in values.schema.json")
		}
		if err := put("values.schema.json", c.Schema); err != nil {
			return err
		}
	}
	for _, t := range c.Templates {
		if err := put(t.Name, t.Data); err != nil {
			return err
		}
	}
	// ponytail: files in path order; Helm keeps directory-walk order, which
	// differs only where a name sorts below "/" (a-b/x before a/x).
	for _, name := range slices.Sorted(maps.Keys(c.Files)) {
		if err := put(name, c.Files[name]); err != nil {
			return err
		}
	}
	for _, sub := range c.Subcharts {
		if err := writeChart(tw, sub, base+"/charts"); err != nil {
			return err
		}
	}
	return nil
}

// loadArchive unpacks a packaged chart to a scratch directory and loads that.
// Like Helm, it drops each entry's first path element, the chart's directory.
func loadArchive(file string) (*Chart, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	tmp, err := os.MkdirTemp("", "chart-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	tr := tar.NewReader(zr)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		_, rel, ok := strings.Cut(strings.ReplaceAll(h.Name, `\`, "/"), "/")
		if !ok || rel == "" {
			continue
		}
		if !filepath.IsLocal(rel) {
			return nil, errors.New("chart illegally contains content outside the base directory")
		}
		p := filepath.Join(tmp, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return nil, err
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			return nil, err
		}
	}
	return loadDir(tmp, "", nil)
}

// ignoreRules reads the chart's .helmignore, if it has one, and appends
// Helm's default rule: no dotfiles directly under templates/.
func ignoreRules(dir string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, ".helmignore"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	var rules []string
	for _, l := range strings.Split(string(raw), "\n") {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		if strings.Contains(l, "**") {
			return nil, errors.New("double-star (**) syntax is not supported")
		}
		if _, err := filepath.Match(l, "abc"); err != nil {
			return nil, err
		}
		rules = append(rules, l)
	}
	return append(rules, "templates/.?*"), nil
}

// ignored applies .helmignore to a slash path relative to the directory the
// rules came from. A pattern starting with / or holding a slash matches the
// whole path, any other the base name; a trailing slash means directories
// only. A negation ignores every path it does not match, as Helm's does.
func ignored(rules []string, rel string, isDir bool) bool {
	for _, p := range rules {
		negate := strings.HasPrefix(p, "!")
		p = strings.TrimPrefix(p, "!")
		dirOnly := strings.HasSuffix(p, "/")
		p = strings.TrimSuffix(p, "/")
		name := rel
		if !strings.Contains(p, "/") {
			name = path.Base(rel)
		}
		match, _ := filepath.Match(strings.TrimPrefix(p, "/"), name)
		switch {
		case negate && (dirOnly && !isDir || !match):
			return true
		case negate, dirOnly && !isDir:
			continue
		case match:
			return true
		}
	}
	return false
}
