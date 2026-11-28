// Your metrics server, and then your autoscaler.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
//
// Stage 1: `app summary <node>` asks that node's kubelet, through the API
// server, what every pod on it is using, and prints one line per pod:
//
//	<namespace>/<pod> <cpu>m <memory-bytes>
//
// From stage 4 the program also runs as a server (`app serve`), and from
// stage 8 as a controller (`app autoscale`).
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course hpa list` to see the stages")
}
