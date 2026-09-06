// Package learn loads the course's teaching layer: the primer and one concept
// note per stage.
//
// The notes explain the idea behind a stage; the stage itself only says
// whether the program behaves. Keeping them apart is deliberate — a note is
// still true with the tester deleted. Coverage may be partial, so a missing
// note is an ordinary outcome rather than an error.
package learn

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Dir is the course-relative root of the teaching content.
const Dir = "learn"

// Primer returns the course primer's body, without frontmatter.
func Primer(courseDir string) (string, bool) {
	return read(filepath.Join(courseDir, Dir, "index.md"))
}

// Note returns a stage's concept note body, without frontmatter. stageDir is
// the shared NN-slug identifier used by the reference snapshots.
func Note(courseDir, stageDir string) (string, bool) {
	return read(filepath.Join(courseDir, Dir, stageDir+".md"))
}

// Concepts returns the tags a note declares in its frontmatter, or nil when
// the note is missing or untagged.
func Concepts(courseDir, stageDir string) []string {
	data, err := os.ReadFile(filepath.Join(courseDir, Dir, stageDir+".md"))
	if err != nil {
		return nil
	}
	return parseConcepts(string(data))
}

// parseConcepts reads the `concepts: [a, b]` line out of the frontmatter. A
// hand-rolled reader rather than a YAML dependency: the field is a flat list
// of short strings and nothing else in the frontmatter is read.
func parseConcepts(doc string) []string {
	front, ok := frontmatter(doc)
	if !ok {
		return nil
	}
	for _, line := range strings.Split(front, "\n") {
		rest, ok := strings.CutPrefix(line, "concepts:")
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		rest = strings.TrimPrefix(rest, "[")
		rest = strings.TrimSuffix(rest, "]")
		var out []string
		for _, tag := range strings.Split(rest, ",") {
			if tag = strings.TrimSpace(tag); tag != "" {
				out = append(out, tag)
			}
		}
		return out
	}
	return nil
}

func read(path string) (string, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	body := strings.TrimSpace(stripFrontmatter(string(data)))
	return body, body != ""
}

func frontmatter(s string) (string, bool) {
	if !strings.HasPrefix(s, "---\n") {
		return "", false
	}
	end := strings.Index(s[4:], "\n---")
	if end < 0 {
		return "", false
	}
	return s[4 : 4+end], true
}

// stripFrontmatter drops the leading YAML block. It carries authoring
// metadata the CLI already knows from course.yml, so it never reaches the
// reader.
func stripFrontmatter(s string) string {
	front, ok := frontmatter(s)
	if !ok {
		return s
	}
	rest := s[4+len(front)+len("\n---"):]
	return strings.TrimPrefix(rest, "\n")
}

var detailsBlock = regexp.MustCompile(`(?s)<details>\s*<summary>(.*?)</summary>(.*?)</details>`)

// SplitHints separates a note's hint ladder from the rest of it. A hint is
// only a hint while it is still hidden, and a terminal has no <details> to
// collapse — so the CLI holds them back until asked and renders them plain.
func SplitHints(note string) (body, hints string) {
	i := strings.Index(note, "\n## Hints\n")
	if i < 0 {
		return note, ""
	}
	rest := note[i+len("\n## Hints\n"):]
	end := strings.Index(rest, "\n## ")
	if end < 0 {
		end = len(rest)
	}
	body = strings.TrimSpace(note[:i]) + "\n\n" + strings.TrimSpace(rest[end:])
	hints = detailsBlock.ReplaceAllString(strings.TrimSpace(rest[:end]), "**$1**\n$2")
	return strings.TrimSpace(body), strings.TrimSpace(hints)
}
