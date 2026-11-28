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
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	registration "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

const version = "0.1.0"

// topologyKey names the one place a volume can be used from: the node whose
// disk holds it.
const topologyKey = "topology.byok8s.dev/node"

type driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	name     string
	nodeID   string
	root     string
	endpoint string // the CSI socket's path
	capacity int64  // the bytes all volumes together may take

	// mu serialises every call that changes a volume, a snapshot or a mount:
	// a CO sends the same call again while the first is still running.
	// ponytail: one lock for the driver; per-volume locks if calls get slow.
	mu sync.Mutex
}

func main() {
	endpoint := flag.String("endpoint", "", "the socket to serve CSI on, unix:///path")
	d := &driver{}
	flag.StringVar(&d.name, "name", "", "the driver name")
	flag.StringVar(&d.nodeID, "node-id", "", "the node this driver runs on")
	flag.StringVar(&d.root, "root", "", "where the driver keeps its volumes")
	flag.Int64Var(&d.capacity, "capacity", 100<<30, "the bytes all volumes together may take")
	regSocket := flag.String("registration-socket", "", "where to serve the kubelet's plugin registration API, if anywhere")
	flag.Parse()
	if err := d.serve(*endpoint, *regSocket); err != nil {
		log.Fatal(err)
	}
}

// serve listens on the endpoint, and on the registration socket if there is
// one, until SIGTERM; then it stops and removes both sockets.
func (d *driver) serve(endpoint, regSocket string) error {
	path, ok := strings.CutPrefix(endpoint, "unix://")
	if !ok {
		return fmt.Errorf("--endpoint %q is not unix://", endpoint)
	}
	d.endpoint = path
	lis, err := listen(path)
	if err != nil {
		return err
	}
	defer os.Remove(path)
	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, d)
	csi.RegisterControllerServer(srv, d)
	csi.RegisterNodeServer(srv, d)
	servers := []*grpc.Server{srv}

	// The kubelet watches its plugins_registry directory: a socket appearing
	// there is a plugin asking to be registered, and its going is the plugin
	// leaving.
	if regSocket != "" {
		rl, err := listen(regSocket)
		if err != nil {
			return err
		}
		defer os.Remove(regSocket)
		reg := grpc.NewServer()
		registration.RegisterRegistrationServer(reg, &registrar{d: d})
		servers = append(servers, reg)
		go reg.Serve(rl)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		<-ctx.Done()
		for _, s := range servers {
			s.GracefulStop()
		}
	}()
	log.Printf("%s serving on %s", d.name, path)
	return srv.Serve(lis)
}

// listen opens a unix socket at path, replacing whatever a crashed run left
// there.
func listen(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return net.Listen("unix", path)
}

// registrar answers the kubelet's plugin watcher: which kind of plugin this
// is, its name, and where its CSI endpoint is.
type registrar struct {
	registration.UnimplementedRegistrationServer
	d *driver
}

func (r *registrar) GetInfo(context.Context, *registration.InfoRequest) (*registration.PluginInfo, error) {
	return &registration.PluginInfo{
		Type:              registration.CSIPlugin,
		Name:              r.d.name,
		Endpoint:          r.d.endpoint,
		SupportedVersions: []string{"1.0.0"},
	}, nil
}

func (r *registrar) NotifyRegistrationStatus(_ context.Context, s *registration.RegistrationStatus) (*registration.RegistrationStatusResponse, error) {
	if !s.PluginRegistered {
		log.Printf("the kubelet refused the registration: %s", s.Error)
	}
	return &registration.RegistrationStatusResponse{}, nil
}

func (d *driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: d.name, VendorVersion: version}, nil
}

func (d *driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}},
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS}}},
		{Type: &csi.PluginCapability_VolumeExpansion_{VolumeExpansion: &csi.PluginCapability_VolumeExpansion{Type: csi.PluginCapability_VolumeExpansion_ONLINE}}},
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
	csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
	csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
	csi.ControllerServiceCapability_RPC_GET_CAPACITY,
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
	// FsType is "" for a directory volume, or the filesystem made on an
	// image file that is attached as a loop device.
	FsType string `json:"fsType,omitempty"`
	// Nodes are the nodes the volume is published to.
	Nodes []string `json:"nodes,omitempty"`
	// Ephemeral is a pod's inline volume, made on publish and gone with
	// the unpublish.
	Ephemeral bool `json:"ephemeral,omitempty"`
	// FromSnapshot is the snapshot the volume was restored from.
	FromSnapshot string `json:"fromSnapshot,omitempty"`
	// FromVolume is the volume this one is a clone of.
	FromVolume string `json:"fromVolume,omitempty"`
}

func (d *driver) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_capabilities are required")
	}
	fsType := ""
	for _, c := range req.VolumeCapabilities {
		if c.GetMount() == nil {
			return nil, status.Error(codes.InvalidArgument, "only filesystem volumes are supported")
		}
		switch t := c.GetMount().FsType; t {
		case "":
		case "ext4":
			fsType = t
		default:
			return nil, status.Errorf(codes.InvalidArgument, "fs_type %q is not supported; ext4 is", t)
		}
	}
	size, err := capacity(req.CapacityRange)
	if err != nil {
		return nil, err
	}
	if !d.reachable(req.AccessibilityRequirements) {
		return nil, status.Errorf(codes.ResourceExhausted, "volumes live on node %s, which no requisite topology includes", d.nodeID)
	}

	id := volumeID(req.Name)
	if v, err := d.load(id); err == nil {
		if v.Capacity < size || (req.CapacityRange.GetLimitBytes() > 0 && v.Capacity > req.CapacityRange.GetLimitBytes()) {
			return nil, status.Errorf(codes.AlreadyExists, "volume %s exists with capacity %d", req.Name, v.Capacity)
		}
		return &csi.CreateVolumeResponse{Volume: d.csiVolume(v)}, nil
	} else if !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	free, err := d.available()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if size > free {
		return nil, status.Errorf(codes.ResourceExhausted, "%d bytes asked for, %d available", size, free)
	}

	v := &volume{ID: id, Name: req.Name, Capacity: size, FsType: fsType}
	if src := req.VolumeContentSource; src != nil {
		if err := d.fill(v, src); err != nil {
			return nil, err
		}
	} else if err := d.makeStorage(v); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := d.save(v); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.CreateVolumeResponse{Volume: d.csiVolume(v)}, nil
}

// reachable is whether a volume made on this node satisfies the CO's
// requisite topologies; with none, any node will do.
func (d *driver) reachable(req *csi.TopologyRequirement) bool {
	if len(req.GetRequisite()) == 0 {
		return true
	}
	for _, t := range req.Requisite {
		if t.Segments[topologyKey] == d.nodeID {
			return true
		}
	}
	return false
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

// csiVolume is a volume as the CO sees it: it is reachable only from the node
// whose disk holds it.
func (d *driver) csiVolume(v *volume) *csi.Volume {
	cv := &csi.Volume{
		VolumeId:           v.ID,
		CapacityBytes:      v.Capacity,
		AccessibleTopology: []*csi.Topology{{Segments: map[string]string{topologyKey: d.nodeID}}},
	}
	if v.FromSnapshot != "" {
		cv.ContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: v.FromSnapshot}}}
	}
	if v.FromVolume != "" {
		cv.ContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: v.FromVolume}}}
	}
	return cv
}

func (d *driver) metaPath(id string) string  { return filepath.Join(d.root, "volumes", id+".json") }
func (d *driver) dataPath(id string) string  { return filepath.Join(d.root, "data", id) }
func (d *driver) imagePath(id string) string { return filepath.Join(d.root, "data", id+".img") }

// makeStorage makes what holds a volume's data: a directory, or a sparse
// image file of the volume's size.
func (d *driver) makeStorage(v *volume) error {
	if v.FsType == "" {
		return os.MkdirAll(d.dataPath(v.ID), 0o755)
	}
	if err := os.MkdirAll(filepath.Dir(d.imagePath(v.ID)), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(d.imagePath(v.ID), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(v.Capacity)
}

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
	return writeAtomic(d.metaPath(v.ID), b)
}

func writeAtomic(path string, b []byte) error {
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
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if v, err := d.load(req.VolumeId); err == nil && len(v.Nodes) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is still published to %v", v.ID, v.Nodes)
	}
	if err := os.RemoveAll(d.dataPath(req.VolumeId)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := os.Remove(d.imagePath(req.VolumeId)); err != nil && !os.IsNotExist(err) {
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
		resp.Entries = append(resp.Entries, &csi.ListVolumesResponse_Entry{Volume: d.csiVolume(v)})
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
var nodeCaps = []csi.NodeServiceCapability_RPC_Type{
	csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
	csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
}

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

// NodeStageVolume mounts the volume's storage at the staging path, once per
// node; every pod that uses the volume is then a bind mount of that.
func (d *driver) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" || req.StagingTargetPath == "" || req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume_id, staging_target_path and volume_capability are required")
	}
	v, err := d.load(req.VolumeId)
	if os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "no volume %s", req.VolumeId)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if v.FsType == "" {
		err = bindOnce(d.dataPath(v.ID), req.StagingTargetPath, false)
	} else {
		err = d.mountImage(v, req.StagingTargetPath)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// mountImage attaches the volume's image as a loop device and mounts its
// filesystem at path, making the filesystem first if the image has none.
// mount's loop option sets autoclear, so the last unmount detaches the device.
func (d *driver) mountImage(v *volume, path string) error {
	if ok, err := mounted(path); err != nil || ok {
		return err
	}
	img := d.imagePath(v.ID)
	// blkid exits 2 when it finds no filesystem; any other failure must not
	// lead to formatting over data.
	var exit *exec.ExitError
	if err := exec.Command("blkid", "-p", img).Run(); errors.As(err, &exit) && exit.ExitCode() == 2 {
		if out, err := exec.Command("mkfs."+v.FsType, "-q", "-F", img).CombinedOutput(); err != nil {
			return fmt.Errorf("mkfs.%s: %w: %s", v.FsType, err, out)
		}
	} else if err != nil {
		return fmt.Errorf("blkid %s: %w", img, err)
	}
	if out, err := exec.Command("mount", "-t", v.FsType, "-o", "loop", img, path).CombinedOutput(); err != nil {
		return fmt.Errorf("mount %s: %w: %s", img, err, out)
	}
	return nil
}

// bindOnce bind-mounts src at dst unless something is mounted there already,
// so a retried call does not stack a second mount.
func bindOnce(src, dst string, readonly bool) error {
	if ok, err := mounted(dst); err != nil || ok {
		return err
	}
	if err := syscall.Mount(src, dst, "", syscall.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind %s at %s: %w", src, dst, err)
	}
	if readonly {
		// A bind mount takes its source's flags; read-only needs a remount.
		if err := syscall.Mount("", dst, "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_RDONLY, ""); err != nil {
			syscall.Unmount(dst, 0)
			return fmt.Errorf("remount %s read-only: %w", dst, err)
		}
	}
	return nil
}

// mounted is whether anything is mounted exactly on path.
func mounted(path string) (bool, error) {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	for _, l := range strings.Split(string(b), "\n") {
		// Field 5 is the mount point, with spaces and the like octal-escaped.
		if f := strings.Fields(l); len(f) > 4 && f[4] == filepath.Clean(path) {
			return true, nil
		}
	}
	return false, nil
}

// NodePublishVolume makes the staged volume appear at a pod's target path,
// read-only if the pod asked.
func (d *driver) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" || req.TargetPath == "" || req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume_id, target_path and volume_capability are required")
	}
	if req.VolumeContext["csi.storage.k8s.io/ephemeral"] == "true" {
		return d.publishEphemeral(req)
	}
	if _, err := d.load(req.VolumeId); os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "no volume %s", req.VolumeId)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.FailedPrecondition, "staging_target_path is required; the volume must be staged first")
	}
	// The CO makes the target's parent; the target itself is the driver's.
	if err := os.MkdirAll(req.TargetPath, 0o755); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := bindOnce(req.StagingTargetPath, req.TargetPath, req.Readonly); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the target and removes it; a target that is
// already gone is success.
func (d *driver) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" || req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and target_path are required")
	}
	if err := unmount(req.TargetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := os.Remove(req.TargetPath); err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if v, err := d.load(req.VolumeId); err == nil && v.Ephemeral {
		if err := os.RemoveAll(d.dataPath(v.ID)); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if err := os.Remove(d.metaPath(v.ID)); err != nil && !os.IsNotExist(err) {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeUnstageVolume unmounts the staging path. The directory is the CO's, so
// it stays.
func (d *driver) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" || req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and staging_target_path are required")
	}
	if err := unmount(req.StagingTargetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// unmount unmounts path if anything is mounted there.
func unmount(path string) error {
	ok, err := mounted(path)
	if err != nil || !ok {
		return err
	}
	if err := syscall.Unmount(path, 0); err != nil {
		return fmt.Errorf("unmount %s: %w", path, err)
	}
	return nil
}

// ControllerExpandVolume grows the volume's storage. A filesystem volume's
// image grows here, but the filesystem on it can only grow on the node.
func (d *driver) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	v, err := d.load(req.VolumeId)
	if os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "no volume %s", req.VolumeId)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	size, err := capacity(req.CapacityRange)
	if err != nil {
		return nil, err
	}
	if size < v.Capacity {
		return nil, status.Errorf(codes.OutOfRange, "volume %s is %d bytes; it cannot shrink to %d", v.ID, v.Capacity, size)
	}
	if size > v.Capacity {
		if v.FsType != "" {
			if err := os.Truncate(d.imagePath(v.ID), size); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
		v.Capacity = size
		if err := d.save(v); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &csi.ControllerExpandVolumeResponse{CapacityBytes: v.Capacity, NodeExpansionRequired: v.FsType != ""}, nil
}

// NodeExpandVolume tells the loop device its image grew, then grows the
// filesystem on it while it stays mounted.
func (d *driver) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" || req.VolumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and volume_path are required")
	}
	v, err := d.load(req.VolumeId)
	if os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "no volume %s", req.VolumeId)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if v.FsType == "" {
		return &csi.NodeExpandVolumeResponse{CapacityBytes: v.Capacity}, nil
	}
	out, err := exec.Command("losetup", "-j", d.imagePath(v.ID), "-n", "-O", "NAME").Output()
	dev := strings.TrimSpace(string(out))
	if err != nil || dev == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is not attached to a loop device; stage it first", v.ID)
	}
	for _, cmd := range [][]string{{"losetup", "-c", dev}, {"resize2fs", dev}} {
		if out, err := exec.Command(cmd[0], cmd[1:]...).CombinedOutput(); err != nil {
			return nil, status.Errorf(codes.Internal, "%s: %v: %s", strings.Join(cmd, " "), err, out)
		}
	}
	return &csi.NodeExpandVolumeResponse{CapacityBytes: v.Capacity}, nil
}

// ControllerPublishVolume makes a volume usable from a node: for a network
// disk, the attach. This driver's volumes are on its own node already, so
// publishing only records which nodes hold the volume.
func (d *driver) ControllerPublishVolume(_ context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" || req.NodeId == "" || req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume_id, node_id and volume_capability are required")
	}
	v, err := d.load(req.VolumeId)
	if os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "no volume %s", req.VolumeId)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if req.NodeId != d.nodeID {
		return nil, status.Errorf(codes.NotFound, "no node %s; volumes live on %s", req.NodeId, d.nodeID)
	}
	if !slices.Contains(v.Nodes, req.NodeId) {
		v.Nodes = append(v.Nodes, req.NodeId)
		if err := d.save(v); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume forgets that a node holds the volume; a volume or
// node it never heard of is already unpublished.
func (d *driver) ControllerUnpublishVolume(_ context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	v, err := d.load(req.VolumeId)
	if os.IsNotExist(err) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// An empty node_id means every node.
	kept := slices.DeleteFunc(v.Nodes, func(n string) bool { return req.NodeId == "" || n == req.NodeId })
	if len(kept) != len(v.Nodes) || req.NodeId == "" {
		v.Nodes = kept
		if err := d.save(v); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// publishEphemeral serves an inline volume. The kubelet names it and never
// creates, stages or deletes it: it lives from this publish to its unpublish.
func (d *driver) publishEphemeral(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	v, err := d.load(req.VolumeId)
	if os.IsNotExist(err) {
		v = &volume{ID: req.VolumeId, Name: req.VolumeId, Ephemeral: true}
		if err := d.makeStorage(v); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if err := d.save(v); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := os.MkdirAll(req.TargetPath, 0o755); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := bindOnce(d.dataPath(v.ID), req.TargetPath, req.Readonly); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// snapshot is what the driver remembers about one snapshot: a full copy of
// its source volume's storage, taken when it was created.
type snapshot struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Source  string `json:"source"`
	Size    int64  `json:"size"`
	FsType  string `json:"fsType,omitempty"`
	Created int64  `json:"created"` // unix nanoseconds
}

func (d *driver) snapMetaPath(id string) string {
	return filepath.Join(d.root, "snapshots", id+".json")
}
func (d *driver) snapDataPath(id string) string { return filepath.Join(d.root, "snapshots", id) }

// storagePath is where a volume's data is: its directory or its image.
func (d *driver) storagePath(v *volume) string {
	if v.FsType == "" {
		return d.dataPath(v.ID)
	}
	return d.imagePath(v.ID)
}

// copyData copies a directory tree or an image file to dst, which must not
// exist yet.
func copyData(src, dst string) error {
	if out, err := exec.Command("cp", "-a", "--sparse=always", src, dst).CombinedOutput(); err != nil {
		return fmt.Errorf("copy %s to %s: %w: %s", src, dst, err, out)
	}
	return nil
}

func (s *snapshot) csi() *csi.Snapshot {
	return &csi.Snapshot{
		SnapshotId:     s.ID,
		SourceVolumeId: s.Source,
		SizeBytes:      s.Size,
		CreationTime:   timestamppb.New(time.Unix(0, s.Created)),
		ReadyToUse:     true,
	}
}

func (d *driver) loadSnapshot(id string) (*snapshot, error) {
	b, err := os.ReadFile(d.snapMetaPath(id))
	if err != nil {
		return nil, err
	}
	s := &snapshot{}
	return s, json.Unmarshal(b, s)
}

func (d *driver) CreateSnapshot(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.Name == "" || req.SourceVolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "name and source_volume_id are required")
	}
	id := "snap-" + strings.TrimPrefix(volumeID(req.Name), "vol-")
	if s, err := d.loadSnapshot(id); err == nil {
		if s.Source != req.SourceVolumeId {
			return nil, status.Errorf(codes.AlreadyExists, "snapshot %s exists of volume %s", req.Name, s.Source)
		}
		return &csi.CreateSnapshotResponse{Snapshot: s.csi()}, nil
	} else if !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	v, err := d.load(req.SourceVolumeId)
	if os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "no volume %s", req.SourceVolumeId)
	} else if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	s := &snapshot{ID: id, Name: req.Name, Source: v.ID, Size: v.Capacity, FsType: v.FsType, Created: time.Now().UnixNano()}
	dst := d.snapDataPath(id)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// A copy a crash cut short is not a snapshot; start it over.
	os.RemoveAll(dst)
	if err := copyData(d.storagePath(v), dst); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := writeAtomic(d.snapMetaPath(id), b); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.CreateSnapshotResponse{Snapshot: s.csi()}, nil
}

func (d *driver) DeleteSnapshot(_ context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.SnapshotId == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}
	if err := os.RemoveAll(d.snapDataPath(req.SnapshotId)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := os.Remove(d.snapMetaPath(req.SnapshotId)); err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots answers every snapshot in one page, or the ones that match
// the request's snapshot_id or source_volume_id.
func (d *driver) ListSnapshots(_ context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	files, err := filepath.Glob(filepath.Join(d.root, "snapshots", "*.json"))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &csi.ListSnapshotsResponse{}
	for _, f := range files {
		s, err := d.loadSnapshot(strings.TrimSuffix(filepath.Base(f), ".json"))
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if (req.SnapshotId != "" && s.ID != req.SnapshotId) || (req.SourceVolumeId != "" && s.Source != req.SourceVolumeId) {
			continue
		}
		resp.Entries = append(resp.Entries, &csi.ListSnapshotsResponse_Entry{Snapshot: s.csi()})
	}
	return resp, nil
}

// fill makes a new volume's storage as a copy of the content source. The
// volume takes the source's filesystem, and may be larger but not smaller.
func (d *driver) fill(v *volume, src *csi.VolumeContentSource) error {
	if from := src.GetVolume(); from != nil {
		sv, err := d.load(from.VolumeId)
		if os.IsNotExist(err) {
			return status.Errorf(codes.NotFound, "no volume %s", from.VolumeId)
		} else if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if v.Capacity < sv.Capacity {
			return status.Errorf(codes.OutOfRange, "volume %s is %d bytes; a clone of %d cannot hold it", sv.ID, sv.Capacity, v.Capacity)
		}
		v.FsType, v.FromVolume = sv.FsType, sv.ID
		return d.copyInto(v, d.storagePath(sv), sv.Capacity)
	}
	snap := src.GetSnapshot()
	if snap == nil {
		return status.Error(codes.InvalidArgument, "a content source is a snapshot or a volume")
	}
	s, err := d.loadSnapshot(snap.SnapshotId)
	if os.IsNotExist(err) {
		return status.Errorf(codes.NotFound, "no snapshot %s", snap.SnapshotId)
	} else if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if v.Capacity < s.Size {
		return status.Errorf(codes.OutOfRange, "snapshot %s is %d bytes; a volume of %d cannot hold it", s.ID, s.Size, v.Capacity)
	}
	v.FsType, v.FromSnapshot = s.FsType, s.ID
	return d.copyInto(v, d.snapDataPath(s.ID), s.Size)
}

// copyInto copies size bytes' worth of storage at from into the volume's own,
// growing an image to the volume's capacity.
func (d *driver) copyInto(v *volume, from string, size int64) error {
	dst := d.storagePath(v)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	os.RemoveAll(dst)
	if err := copyData(from, dst); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if v.FsType != "" && v.Capacity > size {
		if err := os.Truncate(dst, v.Capacity); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
	return nil
}

// GetCapacity answers what is left of --capacity. Every volume is on this
// node and of one kind, so the request's topology and capabilities do not
// change the answer.
func (d *driver) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	free, err := d.available()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.GetCapacityResponse{AvailableCapacity: free}, nil
}

// available is --capacity less the capacity of every volume.
func (d *driver) available() (int64, error) {
	vols, err := d.all()
	if err != nil {
		return 0, err
	}
	free := d.capacity
	for _, v := range vols {
		free -= v.Capacity
	}
	return max(free, 0), nil
}
