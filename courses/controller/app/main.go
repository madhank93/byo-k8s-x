// Your controller.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
//
// Stage 1: teach the cluster a new kind. Create the CustomResourceDefinition
// for websites.byok8s.dev, wait until the API server reports it Established,
// and print that it is ready.
//
// From stage 3 on this program is long-running: it watches, reconciles, and
// only stops when it is asked to. Run it with `byok8s --course controller run`.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course controller list` to see the stages")
}
