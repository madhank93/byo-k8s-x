package helm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
)

// depWork runs the real helm and the program on two copies of the same charts,
// side by side, because dependency update, build and package change the
// files they are pointed at. Paths in commands and output are written ROOT.
type depWork struct {
	w     *work
	roots [2]string // the real helm's copy, then the program's
	env   []string
}

func newDepWork(w *work) *depWork {
	d := &depWork{w: w, roots: [2]string{filepath.Join(w.dir, "h"), filepath.Join(w.dir, "a")}}
	// No configured repositories, so update and build fetch nothing over the
	// network and print no "Hang tight" lines about the grader's machine.
	home := filepath.Join(w.dir, "helm-home")
	d.env = append([]string{
		"HELM_CONFIG_HOME=" + home + "/config",
		"HELM_CACHE_HOME=" + home + "/cache",
		"HELM_DATA_HOME=" + home + "/data",
		"HELM_REPOSITORY_CONFIG=" + home + "/config/repositories.yaml",
		"HELM_REPOSITORY_CACHE=" + home + "/cache/repository",
	}, w.kc...)
	return d
}

// write puts the same file in both copies.
func (d *depWork) write(rel, body string) {
	for _, r := range d.roots {
		path := filepath.Join(r, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			panic(err)
		}
	}
}

func (d *depWork) writeChart(dir string, files map[string]string) {
	for p, body := range files {
		d.write(dir+"/"+p, body)
	}
}

func (d *depWork) remove(rel string) {
	for _, r := range d.roots {
		if err := os.RemoveAll(filepath.Join(r, filepath.FromSlash(rel))); err != nil {
			panic(err)
		}
	}
}

func (d *depWork) path(i int, rel string) string {
	return filepath.Join(d.roots[i], filepath.FromSlash(rel))
}

// same runs one command on both copies and wants the same exit code, stdout
// and Error: line from each.
func (d *depWork) same(why string, args ...string) error {
	var res [2]*runner.Result
	for i, root := range d.roots {
		a := make([]string, len(args))
		for j, s := range args {
			a[j] = strings.ReplaceAll(s, "ROOT", root)
		}
		bin := d.w.bin
		if i == 0 {
			bin = "helm"
		}
		r, err := runner.InvokeEnv(d.w.ctx, bin, d.env, stages.Timeout, a...)
		if err != nil {
			return err
		}
		r.Stdout = strings.ReplaceAll(r.Stdout, root, "ROOT")
		r.Stderr = strings.ReplaceAll(r.Stderr, root, "ROOT")
		res[i] = r
	}
	what := "app " + strings.Join(args, " ") + " (" + why + ")"
	want, got := res[0], res[1]
	if err := wantExit(got, want.ExitCode); err != nil {
		return fmt.Errorf("%s, which exits %d for the real helm: %w", what, want.ExitCode, err)
	}
	if err := wantExact(got.Stdout, want.Stdout, what+", stdout as the real helm prints it"); err != nil {
		return err
	}
	return wantExact(errorLines(got.Stderr), errorLines(want.Stderr), what+", the Error: line on stderr")
}

func errorLines(s string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(l, "Error:") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n") + "\n"
}

var generatedLine = regexp.MustCompile(`(?m)^generated: .*$`)

// sameTree wants the chart's charts/ to hold the same entries in both copies,
// and Chart.lock to say the same thing but for when it was generated.
func (d *depWork) sameTree(chart, after string) error {
	var trees [2]string
	for i := range d.roots {
		var b strings.Builder
		dir := d.path(i, chart+"/charts")
		entries, err := os.ReadDir(dir)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		b.WriteString("charts/:\n")
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() {
				name += "/"
			}
			fmt.Fprintf(&b, "  %s\n", name)
		}
		lock, err := os.ReadFile(d.path(i, chart+"/Chart.lock"))
		switch {
		case os.IsNotExist(err):
			b.WriteString("no Chart.lock\n")
		case err != nil:
			return err
		default:
			b.WriteString("Chart.lock:\n" + generatedLine.ReplaceAllString(string(lock), "generated: (masked)"))
		}
		trees[i] = b.String()
	}
	return wantExact(trees[1], trees[0], "what "+chart+" holds after "+after+", next to what the real helm leaves")
}

type tarEntry struct {
	name, body string
	head       string
	mod        time.Time
}

// readArchive is a package's gzip header and its entries, Chart.lock's time
// masked.
func readArchive(path string) (string, []tarEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", nil, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return "", nil, fmt.Errorf("%s is not gzip: %w", filepath.Base(path), err)
	}
	gz := fmt.Sprintf("gzip header: name=%q comment=%q extra=%q mtime=%d os=%d", zr.Name, zr.Comment, zr.Extra, zr.ModTime.Unix(), zr.OS)
	if zr.ModTime.IsZero() {
		gz = strings.Replace(gz, fmt.Sprintf("mtime=%d", zr.ModTime.Unix()), "mtime=0", 1)
	}
	var out []tarEntry
	tr := tar.NewReader(zr)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", nil, fmt.Errorf("%s is not a tar archive: %w", filepath.Base(path), err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			return "", nil, err
		}
		b := string(body)
		if strings.HasSuffix(h.Name, "/Chart.lock") {
			b = generatedLine.ReplaceAllString(b, "generated: (masked)")
		}
		out = append(out, tarEntry{
			name: h.Name, body: b, mod: h.ModTime,
			head: fmt.Sprintf("type=%c mode=%o uid=%d gid=%d uname=%q gname=%q", h.Typeflag, h.Mode, h.Uid, h.Gid, h.Uname, h.Gname),
		})
	}
	return gz, out, nil
}

// archiveText lays an archive out for comparison: the chart's own entries in
// their order, then the subcharts' sorted, since Helm adds subcharts in map
// order.
func archiveText(gz string, es []tarEntry, top string) string {
	var own, subs []string
	for _, e := range es {
		s := fmt.Sprintf("== %s (%s)\n%s\n", e.name, e.head, e.body)
		if strings.HasPrefix(e.name, top+"/charts/") {
			subs = append(subs, s)
		} else {
			own = append(own, s)
		}
	}
	sort.Strings(subs)
	return gz + "\n" + strings.Join(own, "") + strings.Join(subs, "")
}

// sameArchive packages on both sides and wants the archives to agree in
// everything but the entries' modification time, which Helm sets to the time
// of packaging.
func (d *depWork) sameArchive(why, out, file, top string, args ...string) error {
	start := time.Now().Add(-time.Second)
	if err := d.same(why, args...); err != nil {
		return err
	}
	end := time.Now().Add(time.Second)
	var texts [2]string
	for i := range d.roots {
		gz, es, err := readArchive(d.path(i, out+"/"+file))
		if err != nil {
			return err
		}
		if i == 1 {
			for _, e := range es {
				if e.mod.Before(start) || e.mod.After(end) {
					return fmt.Errorf("%s: entry %s has modification time %s; Helm stamps every entry with the time it packages", file, e.name, e.mod.Format(time.RFC3339))
				}
			}
		}
		texts[i] = archiveText(gz, es, top)
	}
	return wantExact(texts[1], texts[0], file+" packaged by app "+strings.Join(args, " ")+", entry by entry, next to the real helm's")
}

func appChartYAML(libVersion, extra string) string {
	return fmt.Sprintf(`name: app
version: 1.0.0
apiVersion: v2
description: the parent
appVersion: "2.0"
dependencies:
  - name: lib
    version: "%s"
    repository: file://../lib
%s`, libVersion, extra)
}

const pkgdDep = `  - name: pkgd
    version: ">=1.0.0"
`

func libChart(version string) map[string]string {
	return map[string]string{
		"Chart.yaml":          "apiVersion: v2\nname: lib\nversion: " + version + "\n",
		"values.yaml":         "greeting: hello\n",
		"templates/cm.yaml":   "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-lib\ndata:\n  greeting: {{ .Values.greeting }}\n",
		"templates/NOTES.txt": "lib notes are not the parent's\n",
	}
}

var pkgdChart = map[string]string{
	"Chart.yaml":        "apiVersion: v2\nname: pkgd\nversion: 1.2.3\n",
	"templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-pkgd\n",
}

// stageDependencies — dependency list, update and build, Chart.lock and
// package agree with the real helm on two copies of the same charts, step by
// step: statuses, what charts/ holds, the lock and its digest, refusals, and
// the archive entry by entry.
func stageDependencies(ctx context.Context, env *kube.Env, bin string) error {
	w, err := newWork(ctx, env, bin)
	if err != nil {
		return err
	}
	defer w.close()
	if err := needHelm(w); err != nil {
		return err
	}
	d := newDepWork(w)
	d.writeChart("lib", libChart("0.1.0"))
	d.writeChart("pkgd", pkgdChart)
	d.writeChart("app", map[string]string{
		"Chart.yaml":        appChartYAML("^0.1.0", ""),
		"values.yaml":       "# kept as written\nk: v\n",
		"templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-app\ndata:\n  k: {{ .Values.k }}\n",
		".helmignore":       "# scratch work\n*.bak\nscratch/\n",
		"notes.txt":         "packed\n",
		"old.bak":           "ignored by .helmignore\n",
		"scratch/draft.txt": "ignored by .helmignore\n",
	})
	ns := env.Namespace

	type step struct {
		why  string
		args []string
		tree string // the chart whose files to compare afterwards
		do   func()
	}
	steps := []step{
		{why: "a chart with no dependencies", args: []string{"dependency", "list", "ROOT/lib"}},
		{why: "nothing in charts/ yet", args: []string{"dependency", "list", "ROOT/app"}},
		{why: "a dependency missing from charts/", args: []string{"template", "-n", ns, "x", "ROOT/app"}},
		{why: "an undeclared chart vendored into charts/", args: []string{"package", "-d", "ROOT/app/charts", "ROOT/pkgd"}},
		{why: "an archive Chart.yaml does not declare", args: []string{"dependency", "list", "ROOT/app"}},
		{why: "no Chart.lock yet, so build updates", args: []string{"dependency", "build", "ROOT/app"}, tree: "app"},
		{why: "lib fetched", args: []string{"dependency", "list", "ROOT/app"}},
		{why: "lib rendered from its archive in charts/", args: []string{"template", "-n", ns, "x", "ROOT/app"}},
		{why: "lib's archive deleted, rebuilt from the lock", args: []string{"dependency", "build", "ROOT/app"}, tree: "app",
			do: func() { d.remove("app/charts/lib-0.1.0.tgz") }},
		{why: "lib is now 0.1.1, which the lock's 0.1.0 does not allow", args: []string{"dependency", "build", "ROOT/app"}, tree: "app",
			do: func() { d.write("lib/Chart.yaml", "apiVersion: v2\nname: lib\nversion: 0.1.1\n") }},
		{why: "Chart.yaml now wants ^0.2.0, so the lock is out of sync", args: []string{"dependency", "build", "ROOT/app"},
			do: func() {
				d.write("lib/Chart.yaml", "apiVersion: v2\nname: lib\nversion: 0.1.0\n")
				d.write("app/Chart.yaml", appChartYAML("^0.2.0", ""))
			}},
		{why: "^0.2.0 against lib 0.1.0", args: []string{"dependency", "update", "ROOT/app"}, tree: "app"},
		{why: "charts/ holds lib 0.1.0 against ^0.2.0", args: []string{"dependency", "list", "ROOT/app"}},
		{why: "a constraint changed but still satisfied: out of sync all the same", args: []string{"dependency", "build", "ROOT/app"},
			do: func() { d.write("app/Chart.yaml", appChartYAML("~0.1", "")) }},
		{why: "update re-pins with a new digest", args: []string{"dependency", "update", "ROOT/app"}, tree: "app"},
		{why: "pkgd vendored for a dependency with no repository", args: []string{"package", "-d", "ROOT/app/charts", "ROOT/pkgd"},
			do: func() { d.write("app/Chart.yaml", appChartYAML("~0.1", pkgdDep)) }},
		{why: "a dependency with no repository must be unpacked in charts/", args: []string{"dependency", "update", "ROOT/app"}, tree: "app"},
		{why: "pkgd unpacked in charts/", args: []string{"dependency", "update", "ROOT/app"}, tree: "app",
			do: func() {
				d.remove("app/charts/pkgd-1.2.3.tgz")
				d.writeChart("app/charts/pkgd", pkgdChart)
			}},
		{why: "one archive, one unpacked", args: []string{"dependency", "list", "ROOT/app"}},
		{why: "both subcharts", args: []string{"template", "-n", ns, "x", "ROOT/app"}},
		{why: "lock in sync", args: []string{"dependency", "build", "ROOT/app"}, tree: "app"},
	}
	for _, s := range steps {
		if s.do != nil {
			s.do()
		}
		if err := d.same(s.why, s.args...); err != nil {
			return err
		}
		if s.tree != "" {
			if err := d.sameTree(s.tree, "app "+strings.Join(s.args, " ")); err != nil {
				return err
			}
		}
	}

	// An update that resolves what the lock already says leaves it alone.
	before, err := os.ReadFile(d.path(1, "app/Chart.lock"))
	if err != nil {
		return fmt.Errorf("app/Chart.lock: %w", err)
	}
	if err := d.same("nothing changed", "dependency", "update", "ROOT/app"); err != nil {
		return err
	}
	after, err := os.ReadFile(d.path(1, "app/Chart.lock"))
	if err != nil {
		return err
	}
	if !bytes.Equal(before, after) {
		return fmt.Errorf("dependency update rewrote Chart.lock although its digest did not change; Helm leaves the file, generated time and all, as it was:\nbefore:\n%s\nafter:\n%s", indent(string(before)), indent(string(after)))
	}

	if err := d.sameArchive("a chart with a lock, an archived and an unpacked subchart, and a .helmignore", "out", "app-1.0.0.tgz", "app",
		"package", "-d", "ROOT/out", "ROOT/app"); err != nil {
		return err
	}
	if err := d.sameArchive("--version and --app-version rewrite Chart.yaml", "out", "app-1.0.1.tgz", "app",
		"package", "-d", "ROOT/out", "--version", "1.0.1", "--app-version", "3.0", "ROOT/app"); err != nil {
		return err
	}
	if err := d.same("the packaged chart renders as its directory does", "template", "-n", ns, "x", "ROOT/out/app-1.0.0.tgz"); err != nil {
		return err
	}

	d.writeChart("lone", map[string]string{
		"Chart.yaml":        "apiVersion: v2\nname: lone\nversion: 0.3.0\ndependencies:\n- name: lib\n  version: 0.1.x\n  repository: file://../lib\n",
		"templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: lone\n",
	})
	if err := d.same("a dependency missing from charts/", "package", "-d", "ROOT/out", "ROOT/lone"); err != nil {
		return err
	}
	return d.sameArchive("--dependency-update fetches it first", "out", "lone-0.3.0.tgz", "lone",
		"package", "-u", "-d", "ROOT/out", "ROOT/lone")
}
