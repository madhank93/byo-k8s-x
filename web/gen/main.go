// Command gen renders the byok8s website's data from the repo itself:
// courses.yml, each course's course.yml, its learn/ notes, and the verified
// reference snapshots under courses/<slug>/reference/stages.
//
// Everything it writes is generated wholesale on each run and gitignored —
// catalog.ts, the per-stage detail markdown the /catalog modal fetches, and
// the Starlight page for the course primer.
// Run it from the repo root: `go run ./web/gen`, or `mise run gen`.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

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
	"#00a6a6", "#c9184a", "#5a7d9a", "#7f8c00", "#b5179e",
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
	Language    string // the code fence a snapshot renders in
	Entrypoint  string // the one file a snapshot holds
	SourcePath  string // repo-relative path to the snapshotted entrypoint, if verified
	PrevPath    string // repo-relative path to the previous stage's snapshot, if any
	Concepts    []string
}

type courseInfo struct {
	Slug     string
	Name     string
	Color    string
	Learn    string
	Language string
	Total    int
	Verified int
}

// plannedInfo is a course the ladder names but nobody has authored yet. It has
// no directory to load, so it never reaches the stage table — the catalog
// lists it separately, which is how the whole ladder stays visible.
type plannedInfo struct {
	Slug  string
	Name  string
	Tier  string
	Blurb string
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
	reg, err := course.LoadRegistry(root)
	if err != nil {
		return err
	}
	if len(reg.Entries) == 0 {
		return fmt.Errorf("courses.yml lists no courses")
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
	var planned []plannedInfo

	for _, entry := range reg.Entries {
		if !entry.Shipped() {
			planned = append(planned, plannedInfo{Slug: entry.Slug, Name: entry.Name, Tier: entry.Tier, Blurb: entry.Blurb})
			continue
		}
		c, err := course.Load(root, entry.Slug)
		if err != nil {
			return fmt.Errorf("%s: %w", entry.Slug, err)
		}
		courseDir := c.Dir()

		ci := courseInfo{
			Slug:     c.Slug,
			Name:     c.Name,
			Color:    courseColors[len(courses)%len(courseColors)],
			Language: c.Language,
			Total:    len(c.Stages),
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
				Language:   c.Language,
				Entrypoint: c.Entrypoint,
				Concepts:   learn.Concepts(courseDir, stageDir),
			}

			note, hasNote := learn.Note(courseDir, stageDir)
			if hasNote {
				e.Description = summarize(plainText(firstParagraph(note)))
			}

			src := filepath.Join("courses", c.Slug, "reference", "stages", stageDir, c.Entrypoint)
			if _, err := os.Stat(filepath.Join(root, src)); err == nil {
				e.Verified = true
				e.SourcePath = src
				ci.Verified++
				if n > 1 {
					prev := filepath.Join("courses", c.Slug, "reference", "stages", c.StageDir(n-1), c.Entrypoint)
					if _, err := os.Stat(filepath.Join(root, prev)); err == nil {
						e.PrevPath = prev
					}
				}
			}

			if err := writeDetail(root, e, stageBody(root, e, note)); err != nil {
				return fmt.Errorf("%s/%s: %w", c.Slug, s.Slug, err)
			}
			stages = append(stages, e)
		}

		if err := writePrimer(root, courseDir, ci); err != nil {
			return fmt.Errorf("%s: %w", c.Slug, err)
		}
		courses = append(courses, ci)
	}

	return writeCatalog(root, courses, planned, stages)
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
				fmt.Fprintf(&b, "```diff title=%q\n%s\n```\n\n", e.Entrypoint, delta)
			} else {
				b.WriteString("_No code changes this stage — identical to the previous stage's solution._\n\n")
			}
			fmt.Fprintf(&b, "[↗ The whole program at this stage](%s/%s)\n\n", repoBlob, e.SourcePath)
			b.WriteString("</details>\n")
			return b.String()
		}
	}
	b.WriteString("_The first stage's reference solution, in full:_\n\n")
	fmt.Fprintf(&b, "```%s title=%q\n%s\n```\n\n", e.Language, e.Entrypoint, cur)
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

// writePrimer publishes a course's learn/index.md as a page. It is the one
// piece of teaching content that isn't a stage, so unlike the stage notes —
// which the catalog modal shows — it needs a page of its own.
func writePrimer(root, courseDir string, ci courseInfo) error {
	body, ok := learn.Primer(courseDir)
	if !ok {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "---\ntitle: %s\ndescription: %s\nsidebar:\n  order: 0\n  label: %s\n---\n\n",
		js(ci.Name+" — primer"),
		js("The background behind "+ci.Name+": what the API actually is, and the "+languageLabel(ci.Language)+" APIs it needs."),
		js("Primer"))
	fmt.Fprintf(&b, "_Read this before stage 1. Each of the %d stages has its own note — open any row in the [catalog](/catalog/) to read it._\n\n", ci.Total)
	b.WriteString(body)
	b.WriteString("\n")
	return os.WriteFile(filepath.Join(root, docsLearn, ci.Slug+".md"), []byte(b.String()), 0o644)
}

// writeCatalog emits src/data/catalog.ts: the typed COURSES/CATALOG data the
// /catalog page renders at build time.
func writeCatalog(root string, courses []courseInfo, planned []plannedInfo, stages []stageEntry) error {
	var b strings.Builder
	b.WriteString("// AUTO-GENERATED by `go run ./web/gen` from courses.yml — do not hand-edit.\n\n")
	b.WriteString("export type CatalogEntry = {\n")
	b.WriteString("  course: string;\n  index: number;\n  slug: string;\n  title: string;\n")
	b.WriteString("  difficulty: string;\n  description: string;\n  verified: boolean;\n  sourcePath: string;\n  concepts: string[];\n};\n\n")

	b.WriteString("export const COURSES: Record<string, { name: string; repo: string; color: string; learn: string; total: number; verified: number }> = {\n")
	for _, c := range courses {
		fmt.Fprintf(&b, "  %s: { name: %s, repo: %s, color: %s, learn: %s, total: %d, verified: %d },\n",
			js(c.Slug), js(c.Name), js(repoURL), js(c.Color), js(c.Learn), c.Total, c.Verified)
	}
	b.WriteString("};\n\n")

	b.WriteString("export const CATALOG: CatalogEntry[] = [\n")
	for _, e := range stages {
		fmt.Fprintf(&b, "  { course: %s, index: %d, slug: %s, title: %s, difficulty: %s, description: %s, verified: %s, sourcePath: %s, concepts: %s },\n",
			js(e.Course), e.Index, js(e.Slug), js(e.Title), js(e.Difficulty), js(e.Description), boolLit(e.Verified), js(e.SourcePath), jsList(e.Concepts))
	}
	b.WriteString("];\n\n")

	b.WriteString("export const PLANNED: { slug: string; name: string; tier: string; blurb: string }[] = [\n")
	for _, p := range planned {
		fmt.Fprintf(&b, "  { slug: %s, name: %s, tier: %s, blurb: %s },\n", js(p.Slug), js(p.Name), js(p.Tier), js(p.Blurb))
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
	fmt.Fprintf(os.Stderr, "gen: wrote %d course(s) + %d planned, %d/%d stages verified -> %s\n", len(courses), len(planned), verified, total, out)

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

// languageLabel turns a course.yml language into prose. Every course is Go
// today; this exists so the site does not say "Go" for one that is not.
func languageLabel(lang string) string {
	switch lang {
	case "", course.DefaultLanguage:
		return "Go"
	default:
		return strings.ToUpper(lang[:1]) + lang[1:]
	}
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

// jsList renders a tag list as a TS array literal, empty rather than null so
// the page can iterate it without a guard.
func jsList(v []string) string {
	if len(v) == 0 {
		return "[]"
	}
	parts := make([]string, len(v))
	for i, s := range v {
		parts[i] = js(s)
	}
	return "[" + strings.Join(parts, ", ") + "]"
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
