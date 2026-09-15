// Your scheduler.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
//
// Stage 1: find the pods waiting for you. Watch pods in every namespace whose
// spec.schedulerName is "byok8s" and that have no node yet, and print one line
// per pod: "unscheduled <namespace>/<name>".
//
// This program is long-running: it keeps watching until it is stopped. Run it
// with `byok8s --course scheduler run`.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course scheduler list` to see the stages")
}
