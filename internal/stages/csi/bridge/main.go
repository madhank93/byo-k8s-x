// Command bridge joins its stdin and stdout to a unix socket. The harness runs
// it in a kind node with `docker exec -i`, which is how a gRPC client on the
// host reaches a driver's socket inside the node.
package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	c, err := net.Dial("unix", os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	go func() {
		io.Copy(c, os.Stdin)
		c.(*net.UnixConn).CloseWrite()
	}()
	io.Copy(os.Stdout, c)
}
