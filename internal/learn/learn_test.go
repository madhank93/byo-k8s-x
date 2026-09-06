package learn

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const note = `---
title: List the pods
concepts: [clients, listing]
---

## What this stage teaches

The idea.

## Hints

<details><summary>Nudge</summary>

Try the small thing.
</details>

## Going deeper

- [a link](https://example.com)
`

func writeNote(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, Dir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, Dir, "04-list-pods.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestNoteStripsFrontmatter(t *testing.T) {
	dir := writeNote(t, note)

	body, ok := Note(dir, "04-list-pods")
	if !ok {
		t.Fatal("Note reported no note")
	}
	if strings.Contains(body, "title:") {
		t.Errorf("frontmatter reached the reader:\n%s", body)
	}
	if !strings.HasPrefix(body, "## What this stage teaches") {
		t.Errorf("body starts wrong:\n%s", body)
	}
}

func TestMissingNoteIsNotAnError(t *testing.T) {
	dir := writeNote(t, note)

	if _, ok := Note(dir, "99-nope"); ok {
		t.Error("a missing note reported content")
	}
	if _, ok := Primer(dir); ok {
		t.Error("a missing primer reported content")
	}
	if got := Concepts(dir, "99-nope"); got != nil {
		t.Errorf("Concepts(missing) = %v", got)
	}
}

func TestConcepts(t *testing.T) {
	dir := writeNote(t, note)

	got := Concepts(dir, "04-list-pods")
	want := []string{"clients", "listing"}
	if len(got) != len(want) {
		t.Fatalf("Concepts = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Concepts = %v, want %v", got, want)
		}
	}
}

func TestSplitHints(t *testing.T) {
	body, hints := SplitHints(stripFrontmatter(note))

	for _, want := range []string{"## What this stage teaches", "## Going deeper"} {
		if !strings.Contains(body, want) {
			t.Errorf("body lost %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Try the small thing") {
		t.Errorf("body still carries the hint text:\n%s", body)
	}
	if !strings.Contains(hints, "**Nudge**") || !strings.Contains(hints, "Try the small thing") {
		t.Errorf("hints = %q", hints)
	}
	if strings.Contains(hints, "<details>") {
		t.Errorf("hints still carry HTML the terminal cannot collapse: %q", hints)
	}
}

func TestSplitHintsWithoutHints(t *testing.T) {
	in := "## What this stage teaches\n\nJust prose.\n"
	body, hints := SplitHints(in)
	if body != in || hints != "" {
		t.Errorf("SplitHints(no hints) = %q, %q", body, hints)
	}
}
