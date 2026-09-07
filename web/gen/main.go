// Command gen renders the byok8s website's data from the repo itself:
// courses.yml, each course's course.yml, its learn/ notes, and the verified
// reference snapshots under courses/<slug>/reference/stages.
//
// Everything it writes is generated wholesale on each run and gitignored —
// catalog.ts, the per-stage detail markdown the /catalog modal fetches, and
// the Starlight pages for the primer, the stage notes and the concept index.
// Run it from the repo root: `go run ./web/gen`, or `mise run gen`.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/madhank93/byo-k8s-x/internal/course"
	"github.com/madhank93/byo-k8s-x/internal/learn"
)

const (
	dataDir    = "web/src/data"
	detailsDir = "web/src/data/stage-details"
	docsLearn  = "web/src/content/docs/learn"
	repoURL    = "https://github.com/madhank93/byo-k8s-x"
	repoBlob   = repoURL + "/blob/main"
)

// courseColors gives every stage of a course the same chip color on
// /catalog, one per courses.yml entry in registry order. Kubernetes blue
// first, because kubectl is the course this repo is built around.
var courseColors = []string{
	"#326ce5", "#4fa86d", "#d29922", "#9b5de5", "#e36f0e",
}

// registry is courses.yml: every course this repo ships, in the order the
// site should present them.
type registry struct {
	Entries []struct {
		Slug string `yaml:"slug"`
		Name string `yaml:"name"`
	} `yaml:"entries"`
}

type stageEntry struct {
	Course      string
	CourseName  string
	Index       int
	Slug        string
	StageDir    string // the NN-slug identifier shared by notes and snapshots
	Title       string
	Difficulty  string
	Description string
	Verified    bool
	SourcePath  string // repo-relative path to the snapshotted main.go, if verified
	PrevPath    string // repo-relative path to the previous stage's snapshot, if any
}

type courseInfo struct {
	Slug     string
	Name     string
	Color    string
	Learn    string
	Total    int
	Verified int
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
}

func run() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	reg, err := loadRegistry(root)
	if err != nil {
		return err
	}

	// Both trees are regenerated wholesale, so clear them first: a renamed
	// or removed stage must not leave its old page behind.
	for _, dir := range []string{detailsDir, docsLearn} {
		if err := os.RemoveAll(filepath.Join(root, dir)); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return err
		}
	}

	var stages []stageEntry
	var courses []courseInfo

	for i, entry := range reg.Entries {
		c, err := course.Load(root, entry.Slug)
		if err != nil {
			return fmt.Errorf("%s: %w", entry.Slug, err)
		}
		courseDir := c.Dir()

		ci := courseInfo{
			Slug:  c.Slug,
			Name:  c.Name,
			Color: courseColors[i%len(courseColors)],
			Total: len(c.Stages),
		}
		if primer, ok := learn.Primer(courseDir); ok {
			ci.Learn = plainText(firstParagraph(primer))
		}

		for idx, s := range c.Stages {
			n := idx + 1
			stageDir := c.StageDir(n)
			e := stageEntry{
				Course:     c.Slug,
				CourseName: c.Name,
				Index:      n,
				Slug:       s.Slug,
				StageDir:   stageDir,
				Title:      s.Name,
				Difficulty: s.Difficulty,
			}

			note, hasNote := learn.Note(courseDir, stageDir)
			if hasNote {
				e.Description = summarize(plainText(firstParagraph(note)))
			}

			src := filepath.Join("courses", c.Slug, "reference", "stages", stageDir, "main.go")
			if _, err := os.Stat(filepath.Join(root, src)); err == nil {
				e.Verified = true
				e.SourcePath = src
				ci.Verified++
				if n > 1 {
					prev := filepath.Join("courses", c.Slug, "reference", "stages", c.StageDir(n-1), "main.go")
					if _, err := os.Stat(filepath.Join(root, prev)); err == nil {
						e.PrevPath = prev
					}
				}
			}

			body := stageBody(root, e, note)
			if err := writeDetail(root, e, body); err != nil {
				return fmt.Errorf("%s/%s: %w", c.Slug, s.Slug, err)
			}
			if hasNote {
				if err := writeLessonPage(root, e, body, len(c.Stages)); err != nil {
					return fmt.Errorf("%s/%s: %w", c.Slug, s.Slug, err)
				}
			}
			stages = append(stages, e)
		}

		if err := writePrimer(root, courseDir, ci); err != nil {
			return fmt.Errorf("%s: %w", c.Slug, err)
		}
		courses = append(courses, ci)
	}

	if err := writeConceptIndex(root, stages); err != nil {
		return err
	}
	return writeCatalog(root, courses, stages)
}

// repoRoot walks up from the working directory looking for courses.yml, so
// the generator runs from anywhere inside the checkout.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "courses.yml")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("not inside a byo-k8s-x checkout (no courses.yml found)")
		}
		dir = parent
	}
}

func loadRegistry(root string) (*registry, error) {
	data, err := os.ReadFile(filepath.Join(root, "courses.yml"))
	if err != nil {
		return nil, err
	}
	var reg registry
	if err := yaml.Unmarshal(data, &reg); err != nil {
		return nil, fmt.Errorf("parse courses.yml: %w", err)
	}
	if len(reg.Entries) == 0 {
		return nil, fmt.Errorf("courses.yml lists no courses")
	}
	return &reg, nil
}

// stageBody renders the shared markdown for a stage: its concept note, then
// the reference solution behind a spoiler. Both the /catalog modal and the
// lesson page show exactly this, so the two can never drift.
func stageBody(root string, e stageEntry, note string) string {
	var b strings.Builder
	if note != "" {
		b.WriteString(note)
		b.WriteString("\n\n")
	}
	if e.SourcePath == "" {
		return b.String()
	}
	src, err := os.ReadFile(filepath.Join(root, e.SourcePath))
	if err != nil {
		return b.String()
	}
	cur := strings.TrimRight(string(src), "\n")

	b.WriteString("<details>\n<summary>Show the reference solution (spoiler)</summary>\n\n")
	// Snapshots are cumulative programs, so the whole file would give away
	// every later stage as well. Diffing against the previous stage isolates
	// what this one added. Stage 1 has no baseline and shows in full.
	if e.PrevPath != "" {
		if prev, err := os.ReadFile(filepath.Join(root, e.PrevPath)); err == nil {
			delta, changed := unifiedDiff(strings.TrimRight(string(prev), "\n"), cur)
			if changed {
				b.WriteString("_Changes this stage adds to the previous stage's solution:_\n\n")
				fmt.Fprintf(&b, "```diff title=%q\n%s\n```\n\n", "main.go", delta)
			} else {
				b.WriteString("_No code changes this stage — identical to the previous stage's solution._\n\n")
			}
			fmt.Fprintf(&b, "[↗ The whole program at this stage](%s/%s)\n\n", repoBlob, e.SourcePath)
			b.WriteString("</details>\n")
			return b.String()
		}
	}
	b.WriteString("_The first stage's reference solution, in full:_\n\n")
	fmt.Fprintf(&b, "```go title=%q\n%s\n```\n\n", "main.go", cur)
	b.WriteString("</details>\n")
	return b.String()
}

// writeDetail emits the markdown the /catalog modal fetches for one stage.
func writeDetail(root string, e stageEntry, body string) error {
	var b strings.Builder
	// JSON string syntax is a valid YAML double-quoted scalar, so this
	// survives titles containing a colon or a quote.
	fmt.Fprintf(&b, "---\ntitle: %s\n---\n\n", js(e.Title))
	b.WriteString(body)
	name := e.Course + "-" + e.Slug + ".md"
	return os.WriteFile(filepath.Join(root, detailsDir, name), []byte(b.String()), 0o644)
}

// writeLessonPage publishes one stage's note as its own Starlight page, so
// the course reads as a lesson track and not only as a table of stages.
func writeLessonPage(root string, e stageEntry, body string, total int) error {
	dir := filepath.Join(root, docsLearn, e.Course)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "---\ntitle: %s\ndescription: %s\nsidebar:\n  order: %d\n  label: %s\n---\n\n",
		js(fmt.Sprintf("Stage %d — %s", e.Index, e.Title)),
		js(e.Description),
		e.Index,
		js(fmt.Sprintf("%d. %s", e.Index, e.Title)))
	fmt.Fprintf(&b, "_%s, stage %d of %d. Run it with_ `byok8s run %d`_._\n\n",
		e.CourseName, e.Index, total, e.Index)
	b.WriteString(body)
	return os.WriteFile(filepath.Join(dir, fmt.Sprintf("%02d-%s.md", e.Index, e.Slug)), []byte(b.String()), 0o644)
}

// writePrimer turns a course's learn/index.md into the landing page of its
// lesson track — the index of the same directory the stage notes live in, so
// Starlight's autogenerated sidebar groups the whole course under one entry.
func writePrimer(root, courseDir string, ci courseInfo) error {
	body, ok := learn.Primer(courseDir)
	if !ok {
		return nil
	}
	dir := filepath.Join(root, docsLearn, ci.Slug)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "---\ntitle: %s\ndescription: %s\nsidebar:\n  order: 0\n  label: %s\n---\n\n",
		js(ci.Name+" — primer"),
		js("The background behind "+ci.Name+": what the API actually is, and the Go you reach for."),
		js("Primer"))
	fmt.Fprintf(&b, "_Read this before stage 1. The %d stage notes follow it._\n\n", ci.Total)
	b.WriteString(body)
	b.WriteString("\n")
	return os.WriteFile(filepath.Join(dir, "index.md"), []byte(b.String()), 0o644)
}

// writeConceptIndex groups every stage by the concepts its note declares, so
// the course can be approached by idea — "show me everything about
// discovery" — rather than only in stage order. The tags come from the
// notes' own frontmatter, which nothing else reads.
func writeConceptIndex(root string, stages []stageEntry) error {
	type ref struct {
		course, courseName, slug, title string
		index                           int
	}
	byConcept := map[string][]ref{}
	for _, e := range stages {
		courseDir := filepath.Join(root, "courses", e.Course)
		for _, tag := range learn.Concepts(courseDir, e.StageDir) {
			byConcept[tag] = append(byConcept[tag], ref{e.Course, e.CourseName, e.Slug, e.Title, e.Index})
		}
	}
	if len(byConcept) == 0 {
		return nil
	}

	tags := make([]string, 0, len(byConcept))
	for tag := range byConcept {
		tags = append(tags, tag)
	}
	// Most-covered concepts first: the ones worth reading as a thread.
	sort.Slice(tags, func(i, j int) bool {
		if n, m := len(byConcept[tags[i]]), len(byConcept[tags[j]]); n != m {
			return n > m
		}
		return tags[i] < tags[j]
	})

	var b strings.Builder
	b.WriteString("---\ntitle: \"Concepts\"\ndescription: \"Every stage grouped by the idea it teaches, so the course can be read by concept rather than in order.\"\nsidebar:\n  order: 99\n---\n\n")
	fmt.Fprintf(&b, "%d concepts across %d stages. Each stage's note declares what it teaches; this page inverts that.\n\n", len(tags), len(stages))
	for _, tag := range tags {
		refs := byConcept[tag]
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].course != refs[j].course {
				return refs[i].course < refs[j].course
			}
			return refs[i].index < refs[j].index
		})
		fmt.Fprintf(&b, "## %s\n\n", tag)
		for _, r := range refs {
			fmt.Fprintf(&b, "- **Stage %d** — [%s](/catalog/?stage=%s-%s)\n", r.index, r.title, r.course, r.slug)
		}
		b.WriteString("\n")
	}
	return os.WriteFile(filepath.Join(root, docsLearn, "concepts.md"), []byte(b.String()), 0o644)
}

// writeCatalog emits src/data/catalog.ts: the typed COURSES/CATALOG data the
// /catalog page renders at build time.
func writeCatalog(root string, courses []courseInfo, stages []stageEntry) error {
	var b strings.Builder
	b.WriteString("// AUTO-GENERATED by `go run ./web/gen` from courses.yml — do not hand-edit.\n\n")
	b.WriteString("export type CatalogEntry = {\n")
	b.WriteString("  course: string;\n  index: number;\n  slug: string;\n  title: string;\n")
	b.WriteString("  difficulty: string;\n  description: string;\n  verified: boolean;\n  sourcePath: string;\n  lesson: string;\n};\n\n")

	b.WriteString("export const COURSES: Record<string, { name: string; repo: string; color: string; learn: string; total: number; verified: number }> = {\n")
	for _, c := range courses {
		fmt.Fprintf(&b, "  %s: { name: %s, repo: %s, color: %s, learn: %s, total: %d, verified: %d },\n",
			js(c.Slug), js(c.Name), js(repoURL), js(c.Color), js(c.Learn), c.Total, c.Verified)
	}
	b.WriteString("};\n\n")

	b.WriteString("export const CATALOG: CatalogEntry[] = [\n")
	for _, e := range stages {
		lesson := fmt.Sprintf("/learn/%s/%02d-%s/", e.Course, e.Index, e.Slug)
		fmt.Fprintf(&b, "  { course: %s, index: %d, slug: %s, title: %s, difficulty: %s, description: %s, verified: %s, sourcePath: %s, lesson: %s },\n",
			js(e.Course), e.Index, js(e.Slug), js(e.Title), js(e.Difficulty), js(e.Description), boolLit(e.Verified), js(e.SourcePath), js(lesson))
	}
	b.WriteString("];\n")

	if err := os.MkdirAll(filepath.Join(root, dataDir), 0o755); err != nil {
		return err
	}
	out := filepath.Join(root, dataDir, "catalog.ts")
	if err := os.WriteFile(out, []byte(b.String()), 0o644); err != nil {
		return err
	}

	total, verified := 0, 0
	for _, c := range courses {
		total += c.Total
		verified += c.Verified
	}
	fmt.Fprintf(os.Stderr, "gen: wrote %d course(s), %d/%d stages verified -> %s\n", len(courses), verified, total, out)

	// Teaching content is written by hand, so report coverage rather than
	// assuming it.
	notes := 0
	for _, e := range stages {
		if _, ok := learn.Note(filepath.Join(root, "courses", e.Course), e.StageDir); ok {
			notes++
		}
	}
	fmt.Fprintf(os.Stderr, "gen: learn coverage: %d/%d stage notes\n", notes, total)
	return nil
}

var (
	mdLink    = regexp.MustCompile(`\[([^\]]+)\]\([^)]+\)`)
	mdCode    = regexp.MustCompile("`([^`]+)`")
	mdHeading = regexp.MustCompile(`(?m)^#{1,6}\s+`)
	mdBold    = regexp.MustCompile(`\*\*([^*]+)\*\*`)
	mdItalic  = regexp.MustCompile(`\*([^*]+)\*`)
)

// plainText strips the light markdown a note uses down to prose, for the
// catalog table's one-line description and the course popover.
func plainText(s string) string {
	s = mdHeading.ReplaceAllString(s, "")
	s = mdLink.ReplaceAllString(s, "$1")
	s = mdCode.ReplaceAllString(s, "$1")
	s = mdBold.ReplaceAllString(s, "$1")
	s = mdItalic.ReplaceAllString(s, "$1")
	return strings.TrimSpace(strings.Join(strings.Fields(s), " "))
}

// summaryLen is the longest one-line description the catalog table reads
// comfortably; a note's opening paragraph routinely runs several times that.
const summaryLen = 180

// summarize shortens prose to the last sentence that fits in summaryLen,
// falling back to a word-boundary ellipsis when the first sentence is
// already too long.
func summarize(s string) string {
	if len(s) <= summaryLen {
		return s
	}
	if cut := strings.LastIndexAny(s[:summaryLen], ".?!"); cut > summaryLen/3 {
		return s[:cut+1]
	}
	cut := strings.LastIndex(s[:summaryLen], " ")
	if cut < 0 {
		cut = summaryLen
	}
	return s[:cut] + "…"
}

// firstParagraph returns the first non-empty, non-heading paragraph — the
// one-sentence summary shown in the catalog row.
func firstParagraph(md string) string {
	for _, para := range strings.Split(md, "\n\n") {
		para = strings.TrimSpace(para)
		if para == "" || strings.HasPrefix(para, "#") {
			continue
		}
		return para
	}
	return ""
}

func js(s string) string {
	out, _ := json.Marshal(s)
	return string(out)
}

func boolLit(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
