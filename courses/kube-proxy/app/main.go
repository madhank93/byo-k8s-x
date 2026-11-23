// Your kube-proxy.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
//
// Stage 1: find the Services that are yours. Watch Services in every
// namespace labelled service.kubernetes.io/service-proxy-name=byok8s and
// print one line for each that has a cluster IP: "service <namespace>/<name>
// <clusterIP>".
//
// The program is built for Linux and runs as root inside a kind worker node,
// with KUBECONFIG pointing at the cluster. It is long-running: it keeps
// watching until it is stopped. Run it with `byok8s --course kube-proxy run`.
package main

import "fmt"

func main() {
	fmt.Println("nothing here yet — run `byok8s --course kube-proxy list` to see the stages")
}
