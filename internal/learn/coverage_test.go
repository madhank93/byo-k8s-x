package learn

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/madhank93/byo-k8s-x/internal/course"
)

// hint ladders are scaled to the stage: a hard stage earns the full nudge →
// approach → implementation ladder, an easy one is usually done after the
// nudge, and padding a one-line stage out to three hints just buries the
// useful one.
var hintBudget = map[string][2]int{
	"easy":   {1, 2},
	"medium": {1, 3},
	"hard":   {2, 3},
}

// hintLabels are the summaries SplitHints turns into bold headings. They must
// match the <summary> text notes actually use, or the budget check silently
// undercounts and passes for the wrong reason.
var hintLabels = []string{"**Nudge**", "**Approach**", "**Implementation**"}

// shippedCourses loads every course the registry says has a directory, so a
// new course is covered by these checks the moment it is listed.
func shippedCourses(t *testing.T) []*course.Course {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	reg, err := course.LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	var out []*course.Course
	for _, e := range reg.Shipped() {
		c, err := course.Load(root, e.Slug)
		if err != nil {
			t.Fatalf("course %s: %v", e.Slug, err)
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		t.Fatal("courses.yml lists no shipped course")
	}
	return out
}

func TestEveryStageHasANote(t *testing.T) {
	for _, c := range shippedCourses(t) {
		t.Run(c.Slug, func(t *testing.T) {
			if _, ok := Primer(c.Dir()); !ok {
				t.Error("the course has no primer")
			}
			for i := range c.Stages {
				stage := c.StageDir(i + 1)
				note, ok := Note(c.Dir(), stage)
				if !ok {
					t.Errorf("%s: no note", stage)
					continue
				}
				for _, section := range []string{"## Core concept", "## Further reading"} {
					if !strings.Contains(note, section) {
						t.Errorf("%s: missing %q", stage, section)
					}
				}
				if len(Concepts(c.Dir(), stage)) == 0 {
					t.Errorf("%s: no concepts in frontmatter", stage)
				}
			}
		})
	}
}

func TestHintLaddersMatchDifficulty(t *testing.T) {
	for _, c := range shippedCourses(t) {
		t.Run(c.Slug, func(t *testing.T) {
			for i, s := range c.Stages {
				stage := c.StageDir(i + 1)
				note, ok := Note(c.Dir(), stage)
				if !ok {
					continue
				}
				_, hints := SplitHints(note)
				n := 0
				for _, label := range hintLabels {
					n += strings.Count(hints, label)
				}
				budget, known := hintBudget[s.Difficulty]
				if !known {
					t.Errorf("%s: unknown difficulty %q", stage, s.Difficulty)
					continue
				}
				if n < budget[0] || n > budget[1] {
					t.Errorf("%s (%s): %d hints, want %d-%d", stage, s.Difficulty, n, budget[0], budget[1])
				}
			}
		})
	}
}
