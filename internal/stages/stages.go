// Package stages defines the shape every course's assertions share.
//
// A course keeps its own assertions in a subpackage (internal/stages/<slug>)
// and exposes a Lookup of this shape; cmd/tester holds the table mapping a
// course slug to that Lookup. The type lives here rather than in any one
// course so the table can be typed without a course importing a sibling.
package stages

import (
	"context"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
)

// Timeout bounds one stage. Generous, because a cold cluster answering its
// first request is slower than any later one.
const Timeout = 90 * time.Second

// Stage is one gradable step: the learner's built binary, a namespace of its
// own, and a verdict.
type Stage struct {
	Slug string
	Run  func(ctx context.Context, env *kube.Env, bin string) error
}

// Lookup resolves a stage slug within one course.
type Lookup func(slug string) (Stage, bool)
