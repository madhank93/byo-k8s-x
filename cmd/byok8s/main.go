// Command byok8s drives a course: brings the cluster up, runs stages, resets
// the learner's program to a reference snapshot when they want out of a hole.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"github.com/madhank93/byo-k8s-x/internal/cluster"
	"github.com/madhank93/byo-k8s-x/internal/course"
	learnpkg "github.com/madhank93/byo-k8s-x/internal/learn"
)

const defaultCourse = "kubectl"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	if err := dispatch(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "\n%v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `byok8s — build your own Kubernetes tooling

  byok8s up                 create the kind cluster the courses run against
  byok8s down               delete it
  byok8s doctor             check the environment can run a course
  byok8s list               show the stages and where you are
  byok8s run [N]            verify stages 1..N (default: the next unfinished one)
  byok8s learn [N] [-hints] the course primer, or the concept note for stage N
  byok8s reset --to N       replace your program with the stage N reference
`)
}

func dispatch(cmd string, args []string) error {
	ctx := context.Background()
	say := func(s string) { fmt.Println(s) }

	switch cmd {
	case "up":
		return cluster.Up(ctx, say)
	case "down":
		return cluster.Down(ctx, say)
	case "doctor":
		return doctor(ctx)
	case "list":
		return list()
	case "run":
		return runStages(ctx, args)
	case "learn":
		return learn(args)
	case "reset":
		return reset(args)
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// repoRoot walks up from the working directory looking for courses.yml, so the
// CLI works from anywhere inside the repo.
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

// doctor reports what is missing and how to fix it, cheapest check first. A
// stack trace from deep inside client-go is a bad way to learn that Docker is
// not running.
func doctor(ctx context.Context) error {
	for _, bin := range []string{"docker", "kind", "kubectl", "go"} {
		if _, err := exec.LookPath(bin); err != nil {
			return fmt.Errorf("%s is not on PATH\n  fix: install it (mise install)", bin)
		}
	}
	if err := exec.CommandContext(ctx, "docker", "info").Run(); err != nil {
		return fmt.Errorf("docker is not responding\n  fix: start Docker and try again")
	}
	exists, err := cluster.Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("the %s cluster does not exist\n  fix: byok8s up", cluster.Name)
	}
	out, err := exec.CommandContext(ctx, "kubectl", "--context", cluster.Context,
		"get", "--raw", "/readyz", "--request-timeout=5s").CombinedOutput()
	if err != nil {
		return fmt.Errorf("the cluster is not ready: %s\n  fix: byok8s down && byok8s up", out)
	}
	fmt.Println("everything checks out: cluster " + cluster.Name + " is ready")
	return nil
}

func loadCourse() (*course.Course, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	return course.Load(root, defaultCourse)
}

func list() error {
	c, err := loadCourse()
	if err != nil {
		return err
	}
	fmt.Println(c.Name)
	for i, s := range c.Stages {
		fmt.Printf("  %2d  %-18s %s\n", i+1, s.Slug, s.Name)
	}
	return nil
}

// runStages verifies 1..N. Earlier stages are re-run every time, so a change
// that breaks stage 2 while working on stage 5 surfaces at once.
func runStages(ctx context.Context, args []string) error {
	c, err := loadCourse()
	if err != nil {
		return err
	}
	upto := len(c.Stages)
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil || n < 1 || n > len(c.Stages) {
			return fmt.Errorf("stage must be between 1 and %d", len(c.Stages))
		}
		upto = n
	}

	type testCase struct {
		Slug  string `json:"slug"`
		Title string `json:"title"`
	}
	cases := make([]testCase, 0, upto)
	for i := 0; i < upto; i++ {
		cases = append(cases, testCase{
			Slug:  c.Stages[i].Slug,
			Title: fmt.Sprintf("Stage #%d: %s", i+1, c.Stages[i].Name),
		})
	}
	payload, err := json.Marshal(cases)
	if err != nil {
		return err
	}

	root, err := repoRoot()
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/tester")
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"BYOK8S_SUBMISSION_DIR="+c.Dir(),
		"BYOK8S_TEST_CASES_JSON="+string(payload),
	)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

// reset copies a reference snapshot over the learner's program. Cumulative
// stages mean a wrong stage 3 blocks stage 4; this is the way out, and it is
// deliberately explicit rather than automatic.
func reset(args []string) error {
	if len(args) != 2 || args[0] != "--to" {
		return fmt.Errorf("usage: byok8s reset --to N")
	}
	c, err := loadCourse()
	if err != nil {
		return err
	}
	n, err := strconv.Atoi(args[1])
	if err != nil || n < 1 || n > len(c.Stages) {
		return fmt.Errorf("stage must be between 1 and %d", len(c.Stages))
	}
	src := filepath.Join(c.ReferenceDir(n), "main.go")
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("no reference snapshot for stage %d yet: %w", n, err)
	}
	dst := filepath.Join(c.AppDir(), "main.go")
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		return err
	}
	fmt.Printf("your program is now the stage %d reference (%s)\n", n, c.StageDir(n))
	return nil
}

// learn prints the teaching layer: the primer with no argument, a stage's
// concept note with one. Hints stay behind -hints, because a hint you did not
// ask for is a spoiler.
func learn(args []string) error {
	c, err := loadCourse()
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("learn", flag.ContinueOnError)
	hints := fs.Bool("hints", false, "show the hint ladder as well")

	// flag stops at the first positional, so "learn 13 -hints" would leave the
	// flag unparsed — the same trap stage 5 is about. Take a flag run, take one
	// positional, repeat.
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() == 0 {
			break
		}
		positional = append(positional, fs.Arg(0))
		args = fs.Args()[1:]
	}

	if len(positional) == 0 {
		primer, ok := learnpkg.Primer(c.Dir())
		if !ok {
			return fmt.Errorf("this course has no primer yet")
		}
		fmt.Println(primer)
		return nil
	}

	n, err := strconv.Atoi(positional[0])
	if err != nil || n < 1 || n > len(c.Stages) {
		return fmt.Errorf("stage must be between 1 and %d", len(c.Stages))
	}
	note, ok := learnpkg.Note(c.Dir(), c.StageDir(n))
	if !ok {
		return fmt.Errorf("stage %d has no note yet", n)
	}

	body, ladder := learnpkg.SplitHints(note)
	fmt.Printf("Stage %d: %s\n\n%s\n", n, c.Stages[n-1].Name, body)
	if ladder == "" {
		return nil
	}
	if !*hints {
		fmt.Println("\n(hints available: byok8s learn " + strconv.Itoa(n) + " -hints)")
		return nil
	}
	fmt.Printf("\n## Hints\n\n%s\n", ladder)
	return nil
}
