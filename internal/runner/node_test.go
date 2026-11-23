package runner

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestStale(t *testing.T) {
	dead := exec.Command("true")
	if err := dead.Run(); err != nil {
		t.Fatal(err)
	}
	me, gone, parent := os.Getpid(), dead.Process.Pid, os.Getppid()
	for name, want := range map[string]bool{
		fmt.Sprintf("byok8s-app-%d-3.scope", me):     true,
		fmt.Sprintf("byok8s-app-%d", me):             false,
		fmt.Sprintf("byok8s-app-%d-1.scope", parent): false,
		fmt.Sprintf("byok8s-app-%d", parent):         false,
		fmt.Sprintf("byok8s-app-%d-1.scope", gone):   true,
		fmt.Sprintf("byok8s-app-%d", gone):           true,
		"byok8s-app-x.scope":                         true,
	} {
		if got := stale(name); got != want {
			t.Errorf("stale(%q) = %v, want %v", name, got, want)
		}
	}
}
