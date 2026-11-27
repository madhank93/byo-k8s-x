// Your kubelet.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one. It is built for Linux and
// run as root inside a kind node, so set GOOS=linux in your editor.
//
// Stage 1: register a Node. Run as
//
//	app --kubeconfig PATH --hostname-override NAME --register-with-taints KEY=VALUE:EFFECT
//
// and create Node NAME, then keep running until you are stopped.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course kubelet list` to see the stages")
}
