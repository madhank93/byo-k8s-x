// Your CSI driver.
//
// This one file grows for the whole course: every stage adds to the driver
// you already have, rather than starting a new one. It is built for Linux and
// run as root inside a kind worker, so set GOOS=linux in your editor.
//
// Stage 1: serve the CSI Identity service on --endpoint unix://PATH, named
// by --name, and exit 0 on SIGTERM leaving no socket behind.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course csi list` to see the stages")
}
