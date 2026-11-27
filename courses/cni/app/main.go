// Your CNI plugin.
//
// This one file grows for the whole course: every stage adds to the program
// you already have. It is built for Linux and run as root inside the kind
// workers, so set GOOS=linux in your editor.
//
// A runtime runs it with CNI_COMMAND and friends in the environment and the
// network config on stdin. Stage 1: report a bad invocation as a CNI error.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course cni list` to see the stages")
}
