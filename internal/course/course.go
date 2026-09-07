// Package course loads the course registry and a course's stage list.
//
// A course is a directory under courses/ with its own course.yml, the
// learner's app/, and one reference snapshot per stage. The stage list is
// hand-authored here rather than vendored from anywhere, so a stage exists the
// moment someone writes it down.
package course

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Defaults for the fields a Go course never needs to spell out. Every course
// is Go today; the fields exist so the harness reads them from course.yml
// instead of hardcoding them, which is what a non-Go course will need.
const (
	DefaultLanguage   = "go"
	DefaultEntrypoint = "main.go"
)

// Entry is one course in courses.yml. A planned entry has no directory under
// courses/ yet: it exists so the ladder is visible on the site and in the CLI,
// and nothing tries to load it.
type Entry struct {
	Slug   string `yaml:"slug"`
	Name   string `yaml:"name"`
	Status string `yaml:"status"`
	Tier   string `yaml:"tier"`
	Blurb  string `yaml:"blurb"`
}

// Shipped reports whether the course has a directory to load. An entry with no
// status is treated as shipped, so an older courses.yml still works.
func (e Entry) Shipped() bool { return e.Status == "" || e.Status == "shipped" }

// Registry is courses.yml.
type Registry struct {
	Entries []Entry `yaml:"entries"`
}

// LoadRegistry reads courses.yml from the repository root.
func LoadRegistry(repoRoot string) (*Registry, error) {
	data, err := os.ReadFile(filepath.Join(repoRoot, "courses.yml"))
	if err != nil {
		return nil, fmt.Errorf("read courses.yml: %w", err)
	}
	var r Registry
	if err := yaml.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("parse courses.yml: %w", err)
	}
	seen := map[string]bool{}
	for _, e := range r.Entries {
		switch {
		case e.Slug == "":
			return nil, fmt.Errorf("courses.yml has an entry with no slug")
		case seen[e.Slug]:
			return nil, fmt.Errorf("courses.yml: duplicate slug %q", e.Slug)
		}
		seen[e.Slug] = true
	}
	return &r, nil
}

// Shipped is the subset of entries with a course directory to load.
func (r *Registry) Shipped() []Entry {
	var out []Entry
	for _, e := range r.Entries {
		if e.Shipped() {
			out = append(out, e)
		}
	}
	return out
}

// Stage is one step of a course.
type Stage struct {
	Slug       string `yaml:"slug"`
	Name       string `yaml:"name"`
	Difficulty string `yaml:"difficulty"`
}

// Course is one course.yml.
type Course struct {
	Slug   string  `yaml:"slug"`
	Name   string  `yaml:"name"`
	Stages []Stage `yaml:"stages"`

	// Language names the code fence the site renders, and Entrypoint the one
	// file a reference snapshot holds. Both default for a Go course.
	Language   string `yaml:"language"`
	Entrypoint string `yaml:"entrypoint"`

	root string
}

// Load reads courses/<slug>/course.yml.
func Load(repoRoot, slug string) (*Course, error) {
	dir := filepath.Join(repoRoot, "courses", slug)
	data, err := os.ReadFile(filepath.Join(dir, "course.yml"))
	if err != nil {
		return nil, fmt.Errorf("read course.yml: %w", err)
	}
	var c Course
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse course.yml: %w", err)
	}
	if len(c.Stages) == 0 {
		return nil, fmt.Errorf("course %s has no stages", slug)
	}
	// Slugs name directories and appear in test-case JSON; check them once
	// here so no caller has to.
	seen := map[string]bool{}
	for i, s := range c.Stages {
		switch {
		case s.Slug == "":
			return nil, fmt.Errorf("stage %d has no slug", i+1)
		case strings.ContainsAny(s.Slug, "/\\ ."):
			return nil, fmt.Errorf("stage %q: slug must be a bare name", s.Slug)
		case seen[s.Slug]:
			return nil, fmt.Errorf("stage %q: duplicate slug", s.Slug)
		}
		seen[s.Slug] = true
	}
	if c.Language == "" {
		c.Language = DefaultLanguage
	}
	if c.Entrypoint == "" {
		c.Entrypoint = DefaultEntrypoint
	}
	c.root = dir
	return &c, nil
}

// Dir is the course directory.
func (c *Course) Dir() string { return c.root }

// AppDir holds the learner's program — one package that grows across every
// stage, which is what makes the course cumulative.
func (c *Course) AppDir() string { return filepath.Join(c.root, "app") }

// ReferenceDir is the verified snapshot of the program as of stage n (1-based).
func (c *Course) ReferenceDir(n int) string {
	return filepath.Join(c.root, "reference", "stages", c.StageDir(n))
}

// StageDir is the NN-slug identifier shared by reference snapshots and notes.
func (c *Course) StageDir(n int) string {
	return fmt.Sprintf("%02d-%s", n, c.Stages[n-1].Slug)
}
