package learn

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/madhank93/byo-k8s-x/internal/course"
)

// hint ladders are scaled to the stage: a hard stage earns the full nudge →
// approach → API ladder, an easy one is usually done after the nudge, and
// padding a one-line stage out to three hints just buries the useful one.
var hintBudget = map[string][2]int{
	"easy":   {1, 2},
	"medium": {1, 3},
	"hard":   {2, 3},
}

func loadKubectl(t *testing.T) *course.Course {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	c, err := course.Load(root, "kubectl")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestEveryStageHasANote(t *testing.T) {
	c := loadKubectl(t)

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
}

func TestHintLaddersMatchDifficulty(t *testing.T) {
	c := loadKubectl(t)

	for i, s := range c.Stages {
		stage := c.StageDir(i + 1)
		note, ok := Note(c.Dir(), stage)
		if !ok {
			continue
		}
		_, hints := SplitHints(note)
		n := strings.Count(hints, "**Nudge**") +
			strings.Count(hints, "**Approach**") +
			strings.Count(hints, "**The API**")
		budget, known := hintBudget[s.Difficulty]
		if !known {
			t.Errorf("%s: unknown difficulty %q", stage, s.Difficulty)
			continue
		}
		if n < budget[0] || n > budget[1] {
			t.Errorf("%s (%s): %d hints, want %d-%d", stage, s.Difficulty, n, budget[0], budget[1])
		}
	}
}
