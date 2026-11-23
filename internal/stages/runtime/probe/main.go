//go:build linux

// Command probe is the program inside every image the runtime course serves.
// It reports what it can see of its own process — arguments, environment,
// working directory, pid, namespaces, mounts, root — as key=value lines, so a
// stage can judge the container the learner's runtime built from the inside.
//
//	probe [exit=N] [sleep] [escape] [exists=PATH]… [anything else]…
package main

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "breakout" {
		breakout(os.Args[2])
		return
	}
	code, sleep, tryEscape := 0, false, false
	var exists []string
	for _, a := range os.Args[1:] {
		switch {
		case a == "sleep":
			sleep = true
		case a == "escape":
			tryEscape = true
		case strings.HasPrefix(a, "exit="):
			code, _ = strconv.Atoi(strings.TrimPrefix(a, "exit="))
		case strings.HasPrefix(a, "exists="):
			exists = append(exists, strings.TrimPrefix(a, "exists="))
		}
	}

	q := make([]string, len(os.Args)-1)
	for i, a := range os.Args[1:] {
		q[i] = strconv.Quote(a)
	}
	fmt.Println("args=[" + strings.Join(q, ",") + "]")
	env := os.Environ()
	sort.Strings(env)
	for _, e := range env {
		fmt.Println("env=" + e)
	}
	cwd, _ := os.Getwd()
	fmt.Println("cwd=" + cwd)
	fmt.Println("pid=" + strconv.Itoa(os.Getpid()))
	marker, _ := os.ReadFile("/etc/byok8s-image")
	fmt.Println("marker=" + strings.TrimSpace(string(marker)))
	for _, p := range exists {
		_, err := os.Stat(p)
		fmt.Printf("exists %s=%t\n", p, err == nil)
	}
	mnt, _ := os.Readlink("/proc/self/ns/mnt")
	fmt.Println("mntns=" + mnt)
	fmt.Println("procs=" + strconv.Itoa(procs()))
	for _, m := range mountpoints() {
		fmt.Println("mount=" + m)
	}
	if tryEscape {
		fmt.Println("escaped=" + escapeAttempt())
	}
	fmt.Println("end")

	if sleep {
		time.Sleep(time.Hour)
	}
	os.Exit(code)
}

func procs() int {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return -1
	}
	n := 0
	for _, e := range ents {
		if _, err := strconv.Atoi(e.Name()); err == nil {
			n++
		}
	}
	return n
}

func mountpoints() []string {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if f := strings.Fields(l); len(f) > 4 {
			out = append(out, f[4])
		}
	}
	return out
}

// escapeAttempt runs the classic chroot breakout in a child and reports
// whether it reached the kind node's own root. Under a plain chroot it does;
// after pivot_root with the old root detached there is nothing above "/".
func escapeAttempt() string {
	dir, err := os.MkdirTemp("/", ".breakout-")
	if err != nil {
		return "unknown: " + err.Error()
	}
	defer os.Remove(dir)
	out, err := exec.Command("/proc/self/exe", "breakout", dir).Output()
	if err != nil {
		return "unknown: " + err.Error()
	}
	return string(out)
}

func breakout(dir string) {
	if err := syscall.Chroot(dir); err != nil {
		fmt.Print("no")
		return
	}
	for range 64 {
		os.Chdir("..")
	}
	syscall.Chroot(".")
	if _, err := os.Stat("/kind/version"); err == nil {
		fmt.Print("yes")
		return
	}
	fmt.Print("no")
}
