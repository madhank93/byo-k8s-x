// Your admission webhook.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
//
// Stage 1: serve HTTPS. Generate a self-signed certificate, listen on the
// address given by --addr, answer /healthz, and print the address you are
// serving on.
//
// This program is long-running from the first stage: the API server calls it,
// so it has to be up before anything can be admitted. Run it with
// `byok8s --course webhook run`.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course webhook list` to see the stages")
}
