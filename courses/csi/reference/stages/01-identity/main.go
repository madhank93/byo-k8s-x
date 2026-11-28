// Your CSI driver.
//
// This one file grows for the whole course. It is built for Linux and run as
// root inside a kind worker, so set GOOS=linux in your editor.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

const version = "0.1.0"

type driver struct {
	csi.UnimplementedIdentityServer

	name   string
	nodeID string
	root   string
}

func main() {
	endpoint := flag.String("endpoint", "", "the socket to serve CSI on, unix:///path")
	d := &driver{}
	flag.StringVar(&d.name, "name", "", "the driver name")
	flag.StringVar(&d.nodeID, "node-id", "", "the node this driver runs on")
	flag.StringVar(&d.root, "root", "", "where the driver keeps its volumes")
	flag.Parse()
	if err := d.serve(*endpoint); err != nil {
		log.Fatal(err)
	}
}

// serve listens on the endpoint until SIGTERM, then stops and removes the
// socket.
func (d *driver) serve(endpoint string) error {
	path, ok := strings.CutPrefix(endpoint, "unix://")
	if !ok {
		return fmt.Errorf("--endpoint %q is not unix://", endpoint)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, d)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	log.Printf("%s serving on %s", d.name, path)
	return srv.Serve(lis)
}

func (d *driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: d.name, VendorVersion: version}, nil
}

func (d *driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}},
	}}, nil
}

func (d *driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
