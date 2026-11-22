package helm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	env   [2][]string
}

func newDepWork(w *work) *depWork {
	d := &depWork{w: w, roots: [2]string{filepath.Join(w.dir, "h"), filepath.Join(w.dir, "a")}}
	// A helm home per side, with no configured repositories: update prints no
	// "Hang tight" lines about the grader's machine, and neither side can
	// read the repository indexes the other downloaded.
	for i, root := range d.roots {
		home := filepath.Join(root, ".helm")
		d.env[i] = append([]string{
			"HELM_CONFIG_HOME=" + home + "/config",
			"HELM_CACHE_HOME=" + home + "/cache",
			"HELM_DATA_HOME=" + home + "/data",
			"HELM_REPOSITORY_CONFIG=" + home + "/config/repositories.yaml",
			"HELM_REPOSITORY_CACHE=" + home + "/cache/repository",
		}, w.kc...)
	}
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
	return d.compare(why, false, args...)
}

// compare is same, wanting all of stderr to match when whole is set.
func (d *depWork) compare(why string, whole bool, args ...string) error {
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
		r, err := runner.InvokeEnv(d.w.ctx, bin, d.env[i], stages.Timeout, a...)
		if err != nil {
			return err
		}
		r.Stdout = sortUpdates(strings.ReplaceAll(r.Stdout, root, "ROOT"))
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
	if whole {
		return wantExact(got.Stderr, want.Stderr, what+", stderr as the real helm prints it")
	}
	return wantExact(errorLines(got.Stderr), errorLines(want.Stderr), what+", the Error: line on stderr")
}

// sortUpdates sorts each run of "...Successfully"/"...Unable" lines, with the
// tab-indented reason under each, because Helm fetches repository indexes in
// parallel and prints them as they finish.
func sortUpdates(s string) string {
	lines := strings.SplitAfter(s, "\n")
	var out, run []string
	flush := func() {
		sort.Strings(run)
		out = append(out, run...)
		run = nil
	}
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "..."):
			run = append(run, l)
		case strings.HasPrefix(l, "\t") && run != nil:
			run[len(run)-1] += l
		default:
			flush()
			out = append(out, l)
		}
	}
	flush()
	return strings.Join(out, "")
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

	steps := []depStep{
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
	if err := d.run(steps); err != nil {
		return err
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
	if err := d.sameArchive("--dependency-update fetches it first", "out", "lone-0.3.0.tgz", "lone",
		"package", "-u", "-d", "ROOT/out", "ROOT/lone"); err != nil {
		return err
	}
	if err := ignoreSteps(d, ns); err != nil {
		return err
	}
	return repoSteps(d, ns)
}

type depStep struct {
	why    string
	args   []string
	tree   string // the chart whose files to compare afterwards
	do     func()
	stderr bool // compare all of stderr, not only the Error: line
}

func (d *depWork) run(steps []depStep) error {
	for _, s := range steps {
		if s.do != nil {
			s.do()
		}
		if err := d.compare(s.why, s.stderr, s.args...); err != nil {
			return err
		}
		if s.tree != "" {
			if err := d.sameTree(s.tree, "app "+strings.Join(s.args, " ")); err != nil {
				return err
			}
		}
	}
	return nil
}

// filesCM lists a chart's .Files, which is what .helmignore decides.
const filesCM = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Chart.Name }}-files\ndata:\n  files: \"{{ range $k, $v := .Files }}{{ $k }},{{ end }}\"\n"

// ignoreSteps: a directory's .helmignore covers its whole tree, unpacked
// subcharts included and their own .helmignore not, with Helm's negation
// and its refusal of **; a packaged chart is read with no rules at all.
func ignoreSteps(d *depWork, ns string) error {
	d.writeChart("ign", map[string]string{
		"Chart.yaml":                      "apiVersion: v2\nname: ign\nversion: 0.1.0\n",
		"values.yaml":                     "k: v\n",
		"templates/files.yaml":            filesCM,
		"README.md":                       "top\n",
		"docs/README.md":                  "nested\n",
		"docs/guide.txt":                  "kept\n",
		"run.log":                         "dropped\n",
		"charts/sub/Chart.yaml":           "apiVersion: v2\nname: sub\nversion: 0.1.0\n",
		"charts/sub/templates/files.yaml": filesCM,
		"charts/sub/templates/.dot.yaml":  "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: sub-dot\n",
		"charts/sub/.helmignore":          "*.txt\n",
		"charts/sub/a.txt":                "the subchart's own .helmignore does not apply\n",
		"charts/sub/b.log":                "the parent's does\n",
	})
	steps := []depStep{
		{why: "a rule anchored at / against one that is not", args: []string{"template", "-n", ns, "x", "ROOT/ign"},
			do: func() { d.write("ign/.helmignore", "# comment\n/README.md\n*.log\n") }},
		{why: "a directory rule", args: []string{"template", "-n", ns, "x", "ROOT/ign"},
			do: func() { d.write("ign/.helmignore", "docs/\n") }},
		{why: "a negation ignores every path it does not match", args: []string{"template", "-n", ns, "x", "ROOT/ign"},
			do: func() { d.write("ign/.helmignore", "!*.yaml\n") }},
		{why: "a negation that leaves Chart.yaml out", args: []string{"template", "-n", ns, "x", "ROOT/ign"},
			do: func() { d.write("ign/.helmignore", "!*.md\n") }},
		{why: "a negation followed by a rule", args: []string{"template", "-n", ns, "x", "ROOT/ign"},
			do: func() { d.write("ign/.helmignore", "!*\n*.md\n") }},
		{why: "** is refused", args: []string{"template", "-n", ns, "x", "ROOT/ign"},
			do: func() { d.write("ign/.helmignore", "docs/**\n") }},
		{why: "a pattern that does not parse", args: []string{"package", "-d", "ROOT/out", "ROOT/ign"},
			do: func() { d.write("ign/.helmignore", "[\n") }},
	}
	if err := d.run(steps); err != nil {
		return err
	}
	d.write("ign/.helmignore", "*.log\n!*\ndocs/\n")
	if err := d.sameArchive("a negation then a directory rule", "out", "ign-0.1.0.tgz", "ign",
		"package", "-d", "ROOT/out", "ROOT/ign"); err != nil {
		return err
	}

	// No helm package writes an archive holding files its .helmignore names,
	// so the grader makes one, with a dotfile template besides.
	tgz, err := tarGz(map[string]string{
		"made/Chart.yaml":           "apiVersion: v2\nname: made\nversion: 0.1.0\n",
		"made/.helmignore":          "*.md\ntemplates/\n",
		"made/notes.md":             "an archive's .helmignore is not applied\n",
		"made/templates/files.yaml": filesCM,
		"made/templates/.dot.yaml":  "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: made-dot\n",
	})
	if err != nil {
		return err
	}
	d.write("made-0.1.0.tgz", tgz)
	return d.same("a packaged chart, loaded with no ignore rules", "template", "-n", ns, "x", "ROOT/made-0.1.0.tgz")
}

func tarGz(files map[string]string) (string, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if err := tw.WriteHeader(&tar.Header{Name: n, Mode: 0o644, Size: int64(len(files[n])), ModTime: time.Now()}); err != nil {
			return "", err
		}
		if _, err := tw.Write([]byte(files[n])); err != nil {
			return "", err
		}
	}
	if err := tw.Close(); err != nil {
		return "", err
	}
	if err := zw.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// repoSteps: dependency update against a chart repository served over HTTP
// that `helm repo add` never heard of. Its index is listed out of version
// order, one URL in it is relative, and one archive is missing.
func repoSteps(d *depWork, ns string) error {
	served := map[string][]byte{}
	for _, c := range []struct{ name, version, path string }{
		{"db", "1.3.0", "db-1.3.0.tgz"}, {"db", "2.0.0", "db-2.0.0.tgz"}, {"db", "1.4.2", "pkg/db-1.4.2.tgz"},
		{"db", "2.1.0-beta.1", "db-2.1.0-beta.1.tgz"}, {"cache", "1.0.0", "cache-1.0.0.tgz"},
	} {
		src := filepath.Join(d.w.dir, "repo-src", c.name+"-"+c.version)
		for name, body := range map[string]string{
			"Chart.yaml":        fmt.Sprintf("apiVersion: v2\nname: %s\nversion: %s\n", c.name, c.version),
			"templates/cm.yaml": fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-%s\ndata:\n  version: %q\n", c.name, c.version),
		} {
			if err := os.MkdirAll(filepath.Dir(filepath.Join(src, name)), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(src, name), []byte(body), 0o644); err != nil {
				return err
			}
		}
		out := filepath.Join(d.w.dir, "repo-out")
		if res, err := d.w.helm("package", "-d", out, src); err != nil {
			return err
		} else if res.ExitCode != 0 {
			return fmt.Errorf("the grader's helm package %s: %s", src, res.Stderr)
		}
		raw, err := os.ReadFile(filepath.Join(out, c.name+"-"+c.version+".tgz"))
		if err != nil {
			return err
		}
		served["/"+c.path] = raw
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := served[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(body)
	}))
	defer srv.Close()
	u := srv.URL
	served["/index.yaml"] = []byte(fmt.Sprintf(`apiVersion: v1
entries:
  db:
  - {name: db, version: 1.3.0, urls: [%[1]s/db-1.3.0.tgz]}
  - {name: db, version: 2.0.0, urls: [%[1]s/db-2.0.0.tgz]}
  - {name: db, version: 1.4.2, urls: [pkg/db-1.4.2.tgz]}
  - {name: db, version: 0.9.0, urls: [%[1]s/gone/db-0.9.0.tgz]}
  - {name: db, version: 2.1.0-beta.1, urls: [%[1]s/db-2.1.0-beta.1.tgz]}
  cache:
  - {name: cache, version: 1.0.0, urls: [%[1]s/cache-1.0.0.tgz]}
`, u))

	web := func(deps string) func() {
		return func() {
			d.write("web/Chart.yaml", "apiVersion: v2\nname: web\nversion: 0.1.0\ndependencies:\n"+deps)
		}
	}
	dep := func(name, version, repo string) string {
		return fmt.Sprintf("  - name: %s\n    version: %q\n    repository: %q\n", name, version, repo)
	}
	lib := dep("lib", "0.1.0", "file://../lib")
	d.write("lib/Chart.yaml", "apiVersion: v2\nname: lib\nversion: 0.1.0\n")
	d.write("web/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web\n")
	steps := []depStep{
		{why: "the newest db in ^1.0.0, whose URL in the index is relative", args: []string{"dependency", "update", "ROOT/web"}, tree: "web",
			do: web(dep("db", "^1.0.0", u) + lib)},
		{why: "db fetched from the repository", args: []string{"dependency", "list", "ROOT/web"}},
		{why: "db rendered from the downloaded archive", args: []string{"template", "-n", ns, "x", "ROOT/web"}},
		{why: "build only fetches from repositories helm repo add knows", args: []string{"dependency", "build", "ROOT/web"}},
		{why: "a constraint that admits prereleases; 1.4.2 is now outdated", args: []string{"dependency", "update", "ROOT/web"}, tree: "web",
			do: web(dep("db", ">=2.0.0-0", u))},
		{why: "two charts from one repository, and a URL ending in /", args: []string{"dependency", "update", "ROOT/web"}, tree: "web",
			do: web(dep("db", "^2", u+"/") + dep("cache", "*", u))},
		{why: "the index lists an archive the repository does not have", args: []string{"dependency", "update", "ROOT/web"}, tree: "web",
			do: web(dep("db", "~0.9", u))},
		{why: "no version in the index satisfies the constraint", args: []string{"dependency", "update", "ROOT/web"},
			do: web(dep("db", ">=5", u))},
		{why: "a chart the index does not list", args: []string{"dependency", "update", "ROOT/web"},
			do: web(dep("nope", "1.0.0", u))},
		{why: "a repository with no index.yaml, its URL ending in /", args: []string{"dependency", "update", "ROOT/web"},
			do: web(dep("db", "1.3.0", u+"/none/"))},
		{why: "repositories named, not given by URL", args: []string{"dependency", "update", "ROOT/web"}, stderr: true,
			do: web(dep("db", "1.3.0", "@stable") + dep("cache", "1.0.0", "incubator"))},
		{why: "an alias alone gets no note", args: []string{"dependency", "build", "ROOT/web"}, stderr: true,
			do: web(dep("db", "1.3.0", "@stable"))},
	}
	if err := d.run(steps[:1]); err != nil {
		return err
	}
	got, err := os.ReadFile(d.path(1, "web/charts/db-1.4.2.tgz"))
	if err != nil {
		return err
	}
	if !bytes.Equal(got, served["/pkg/db-1.4.2.tgz"]) {
		return fmt.Errorf("web/charts/db-1.4.2.tgz is not the archive the repository served; update saves a downloaded chart as it arrives")
	}
	return d.run(steps[1:])
}
