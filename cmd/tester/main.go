// Command tester grades a learner's program against a course's stages.
//
// It reads its work from the environment, which keeps it usable from a CLI, a
// TUI or CI without any of them knowing how grading works:
//
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
	stages "github.com/madhank93/byo-k8s-x/internal/stages/kubectl"
)

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
	bin, err := runner.Build(ctx, filepath.Join(dir, "app"))
	if err != nil {
		return err
	}
	defer os.RemoveAll(filepath.Dir(bin))

	for i, tc := range cases {
		stage, ok := stages.Lookup(tc.Slug)
		if !ok {
			return fmt.Errorf("no assertions registered for stage %q", tc.Slug)
		}

		title := tc.Title
		if title == "" {
			title = fmt.Sprintf("Stage #%d: %s", i+1, tc.Slug)
		}
		fmt.Printf("\n[%s] running\n", title)

		env, cancel, err := kube.Begin(ctx, tc.Slug, stages.StageTimeout+30*time.Second)
		if err != nil {
			return fmt.Errorf("[%s] preparing the cluster: %w", title, err)
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
