// Package course loads a course's stage list.
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
