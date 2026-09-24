// Your API server.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
//
// Stage 1: serve HTTP on the address given by the -addr flag. Answer /healthz,
// /livez and /readyz with "ok", and answer anything else with a 404 carrying a
// Status object.
//
// This program is long-running: it serves until it is stopped. Run it with
// `byok8s --course apiserver run`.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course apiserver list` to see the stages")
}
