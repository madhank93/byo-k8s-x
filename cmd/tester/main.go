// Command tester grades a learner's program against a course's stages.
//
// It reads its work from the environment, which keeps it usable from a CLI, a
// TUI or CI without any of them knowing how grading works:
//
//	BYOK8S_COURSE           the course slug, which selects its assertions
//	BYOK8S_SUBMISSION_DIR   the course directory holding app/
//	BYOK8S_TEST_CASES_JSON  [{"slug":"server-url","title":"Stage #1: …"}, …]
//
// Cases arrive in order and are run in order, so a run of stages 1..N
// re-verifies every earlier stage. A stage that used to pass and no longer
// does is a regression the learner should see immediately, not at the end.
//
// Exit 0 means every case passed.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
	"github.com/madhank93/byo-k8s-x/internal/stages"
	apiserverstages "github.com/madhank93/byo-k8s-x/internal/stages/apiserver"
	cnistages "github.com/madhank93/byo-k8s-x/internal/stages/cni"
	controllerstages "github.com/madhank93/byo-k8s-x/internal/stages/controller"
	helmstages "github.com/madhank93/byo-k8s-x/internal/stages/helm"
	kubectlstages "github.com/madhank93/byo-k8s-x/internal/stages/kubectl"
	kustomizestages "github.com/madhank93/byo-k8s-x/internal/stages/kustomize"
	runtimestages "github.com/madhank93/byo-k8s-x/internal/stages/runtime"
	safeapplystages "github.com/madhank93/byo-k8s-x/internal/stages/safe-apply"
	schedulerstages "github.com/madhank93/byo-k8s-x/internal/stages/scheduler"
	webhookstages "github.com/madhank93/byo-k8s-x/internal/stages/webhook"
)

// course is how one course is graded: its assertions, whether its stages
// want a namespace on the kind cluster prepared for them, and whether the
// program runs inside a kind node, which means building it for Linux.
type course struct {
	lookup  stages.Lookup
	cluster bool
	node    bool
}

// courses maps a course slug to that — the one line a new course adds to this
// command. The apiserver course builds the thing the others are clients of, so
// its stages talk to the learner's own server over HTTP and there is no
// cluster for the harness to prepare; kustomize renders offline.
var courses = map[string]course{
	"kubectl":    {lookup: kubectlstages.Lookup, cluster: true},
	"controller": {lookup: controllerstages.Lookup, cluster: true},
	"webhook":    {lookup: webhookstages.Lookup, cluster: true},
	"scheduler":  {lookup: schedulerstages.Lookup, cluster: true},
	"apiserver":  {lookup: apiserverstages.Lookup},
	"safe-apply": {lookup: safeapplystages.Lookup, cluster: true},
	"helm":       {lookup: helmstages.Lookup, cluster: true},
	"kustomize":  {lookup: kustomizestages.Lookup},
	"runtime":    {lookup: runtimestages.Lookup, node: true},
	"cni":        {lookup: cnistages.Lookup, cluster: true, node: true},
}

type testCase struct {
	Slug  string `json:"slug"`
	Title string `json:"title"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\n%v\n", err)
		os.Exit(1)
	}
}

func run() error {
	slug := os.Getenv("BYOK8S_COURSE")
	if slug == "" {
		return fmt.Errorf("BYOK8S_COURSE is not set")
	}
	c, ok := courses[slug]
	if !ok {
		return fmt.Errorf("no assertions are registered for course %q", slug)
	}
	dir := os.Getenv("BYOK8S_SUBMISSION_DIR")
	if dir == "" {
		return fmt.Errorf("BYOK8S_SUBMISSION_DIR is not set")
	}
	raw := os.Getenv("BYOK8S_TEST_CASES_JSON")
	if raw == "" {
		return fmt.Errorf("BYOK8S_TEST_CASES_JSON is not set")
	}
	var cases []testCase
	if err := json.Unmarshal([]byte(raw), &cases); err != nil {
		return fmt.Errorf("parse BYOK8S_TEST_CASES_JSON: %w", err)
	}

	ctx := context.Background()

	// Build once. Compiling is the slowest step in the loop and the program
	// does not change between cases.
	fmt.Println("building your program…")
	goos, goarch := "", ""
	if c.node {
		arch, err := runner.NodeArch(ctx)
		if err != nil {
			return err
		}
		goos, goarch = "linux", arch
	}
	bin, err := runner.BuildFor(ctx, filepath.Join(dir, "app"), goos, goarch)
	if err != nil {
		return err
	}
	defer os.RemoveAll(filepath.Dir(bin))

	for i, tc := range cases {
		stage, ok := c.lookup(tc.Slug)
		if !ok {
			return fmt.Errorf("no assertions registered for stage %q", tc.Slug)
		}

		title := tc.Title
		if title == "" {
			title = fmt.Sprintf("Stage #%d: %s", i+1, tc.Slug)
		}
		fmt.Printf("\n[%s] running\n", title)

		// A course graded without a cluster gets no Env: there is no namespace
		// to put anything in, and a stage that asked for one would be reaching
		// for the very thing the learner is building.
		var env *kube.Env
		cancel := func() {}
		if c.cluster {
			env, cancel, err = kube.Begin(ctx, slug, tc.Slug, stages.Timeout+30*time.Second)
			if err != nil {
				return fmt.Errorf("[%s] preparing the cluster: %w", title, err)
			}
		}
		err = stage.Run(ctx, env, bin)
		cancel()

		if err != nil {
			return fmt.Errorf("[%s] FAILED\n%v", title, err)
		}
		fmt.Printf("[%s] passed\n", title)
	}

	fmt.Printf("\nall %d stage(s) passed\n", len(cases))
	return nil
}
