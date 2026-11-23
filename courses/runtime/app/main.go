// Your container runtime.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one. It is built for Linux and
// run as root inside a kind node, so set GOOS=linux in your editor.
//
// Stage 1: `auth REF` follows the registry's 401 to its token service and
// prints a pull token for REF's repository.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course runtime list` to see the stages")
}
