// Your CSI driver.
//
// This one file grows for the whole course. It is built for Linux and run as
// root inside a kind worker, so set GOOS=linux in your editor.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const version = "0.1.0"

// topologyKey names the one place a volume can be used from: the node whose
// disk holds it.
const topologyKey = "topology.byok8s.dev/node"

type driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

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
	csi.RegisterControllerServer(srv, d)
	csi.RegisterNodeServer(srv, d)

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

// controllerCaps is what this driver's controller service implements, and so
// all it advertises.
var controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
}

func (d *driver) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	var caps []*csi.ControllerServiceCapability
	for _, c := range controllerCaps {
		caps = append(caps, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: c}}})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

// defaultCapacity is a volume's size when the request names none.
const defaultCapacity = 1 << 30

// volume is what the driver remembers about one volume, kept as JSON under
// --root so it outlives the process.
type volume struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Capacity int64  `json:"capacity"`
}

func (d *driver) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_capabilities are required")
	}
	for _, c := range req.VolumeCapabilities {
		if c.GetMount() == nil {
			return nil, status.Error(codes.InvalidArgument, "only filesystem volumes are supported")
		}
	}
	size, err := capacity(req.CapacityRange)
	if err != nil {
		return nil, err
	}

	id := volumeID(req.Name)
	if v, err := d.load(id); err == nil {
		if v.Capacity < size || (req.CapacityRange.GetLimitBytes() > 0 && v.Capacity > req.CapacityRange.GetLimitBytes()) {
			return nil, status.Errorf(codes.AlreadyExists, "volume %s exists with capacity %d", req.Name, v.Capacity)
		}
		return &csi.CreateVolumeResponse{Volume: v.csi()}, nil
	} else if !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	v := &volume{ID: id, Name: req.Name, Capacity: size}
	if err := os.MkdirAll(d.dataPath(id), 0o755); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := d.save(v); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.CreateVolumeResponse{Volume: v.csi()}, nil
}

// capacity picks a volume's size from the range a CO asked for.
func capacity(r *csi.CapacityRange) (int64, error) {
	req, limit := r.GetRequiredBytes(), r.GetLimitBytes()
	if req < 0 || limit < 0 || (limit > 0 && req > limit) {
		return 0, status.Errorf(codes.OutOfRange, "capacity range %d..%d is not satisfiable", req, limit)
	}
	if req > 0 {
		return req, nil
	}
	if limit > 0 && limit < defaultCapacity {
		return limit, nil
	}
	return defaultCapacity, nil
}

// volumeID derives the id from the name, so a retried CreateVolume finds the
// volume it already made.
func volumeID(name string) string {
	sum := sha256.Sum256([]byte(name))
	return "vol-" + hex.EncodeToString(sum[:8])
}

func (v *volume) csi() *csi.Volume {
	return &csi.Volume{VolumeId: v.ID, CapacityBytes: v.Capacity}
}

func (d *driver) metaPath(id string) string { return filepath.Join(d.root, "volumes", id+".json") }
func (d *driver) dataPath(id string) string { return filepath.Join(d.root, "data", id) }

func (d *driver) load(id string) (*volume, error) {
	b, err := os.ReadFile(d.metaPath(id))
	if err != nil {
		return nil, err
	}
	v := &volume{}
	return v, json.Unmarshal(b, v)
}

// save writes a record whole or not at all: a crash mid-write must not leave
// a half-written file behind.
func (d *driver) save(v *volume) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	path := d.metaPath(v.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (d *driver) DeleteVolume(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := os.RemoveAll(d.dataPath(req.VolumeId)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := os.Remove(d.metaPath(req.VolumeId)); err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ListVolumes answers every volume in one page.
func (d *driver) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	vols, err := d.all()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &csi.ListVolumesResponse{}
	for _, v := range vols {
		resp.Entries = append(resp.Entries, &csi.ListVolumesResponse_Entry{Volume: v.csi()})
	}
	return resp, nil
}

func (d *driver) all() ([]*volume, error) {
	files, err := filepath.Glob(filepath.Join(d.root, "volumes", "*.json"))
	if err != nil {
		return nil, err
	}
	var vols []*volume
	for _, f := range files {
		v, err := d.load(strings.TrimSuffix(filepath.Base(f), ".json"))
		if err != nil {
			return nil, err
		}
		vols = append(vols, v)
	}
	return vols, nil
}

// nodeCaps is what this driver's node service implements.
var nodeCaps = []csi.NodeServiceCapability_RPC_Type{}

func (d *driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	var caps []*csi.NodeServiceCapability
	for _, c := range nodeCaps {
		caps = append(caps, &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: c}}})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:             d.nodeID,
		AccessibleTopology: &csi.Topology{Segments: map[string]string{topologyKey: d.nodeID}},
	}, nil
}
