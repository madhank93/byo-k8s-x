//go:build linux

// A CNI plugin: given a container's network namespace, give it a network.
//
// A runtime runs it with the request in the environment and the network
// config on stdin:
//
//	CNI_COMMAND=ADD|DEL|CHECK|VERSION
//	CNI_CONTAINERID, CNI_NETNS, CNI_IFNAME, CNI_ARGS, CNI_PATH
//
// It prints a result, or an error, as JSON on stdout.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
)

// Well-known error codes from the CNI spec.
const (
	errVersion   = 1
	errBadEnv    = 4
	errDecode    = 6
	errBadConfig = 7
	errInternal  = 100 // codes from 100 up are the plugin's own
)

// cniError is how a plugin fails: printed on stdout, with a non-zero exit.
type cniError struct {
	CNIVersion string `json:"cniVersion"`
	Code       int    `json:"code"`
	Msg        string `json:"msg"`
}

func (e *cniError) Error() string { return e.Msg }

func fail(code int, format string, a ...any) error {
	return &cniError{Code: code, Msg: fmt.Sprintf(format, a...)}
}

// supported is every spec version this plugin reads configs in and writes
// results for.
var supported = []string{"1.0.0", "1.1.0"}

func main() {
	if err := run(); err != nil {
		var ce *cniError
		if !errors.As(err, &ce) {
			ce = &cniError{Code: errInternal, Msg: err.Error()}
		}
		if ce.CNIVersion == "" {
			ce.CNIVersion = "1.0.0"
		}
		json.NewEncoder(os.Stdout).Encode(ce)
		os.Exit(1)
	}
}

// request is one invocation: the environment, and the config on stdin.
type request struct {
	command, containerID, netns, ifname, args, path string
	raw                                             []byte
	conf                                            netConf
}

// netConf is the network config. Fields are added as the plugin learns them.
type netConf struct {
	CNIVersion string `json:"cniVersion"`
	Name       string `json:"name"`
	Type       string `json:"type"`
}

func run() error {
	req := request{
		command:     os.Getenv("CNI_COMMAND"),
		containerID: os.Getenv("CNI_CONTAINERID"),
		netns:       os.Getenv("CNI_NETNS"),
		ifname:      os.Getenv("CNI_IFNAME"),
		args:        os.Getenv("CNI_ARGS"),
		path:        os.Getenv("CNI_PATH"),
	}
	switch req.command {
	case "VERSION":
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"cniVersion": "1.1.0", "supportedVersions": supported})
	case "ADD", "DEL", "CHECK":
	case "":
		return fail(errBadEnv, "CNI_COMMAND is not set")
	default:
		return fail(errBadEnv, "unknown CNI_COMMAND %q", req.command)
	}
	if req.containerID == "" {
		return fail(errBadEnv, "CNI_CONTAINERID is not set")
	}
	if req.ifname == "" {
		return fail(errBadEnv, "CNI_IFNAME is not set")
	}
	if req.netns == "" && req.command != "DEL" {
		return fail(errBadEnv, "CNI_NETNS is not set")
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fail(errDecode, "read config: %v", err)
	}
	req.raw = raw
	if err := json.Unmarshal(raw, &req.conf); err != nil {
		return fail(errDecode, "decode config: %v", err)
	}
	if !slices.Contains(supported, req.conf.CNIVersion) {
		return fail(errVersion, "cniVersion %q is not supported; this plugin speaks %v", req.conf.CNIVersion, supported)
	}
	switch req.command {
	case "ADD":
		return cmdAdd(&req)
	}
	return nil
}

// result is what ADD reports: the interfaces it made, their addresses, the
// routes, and DNS, in the config's spec version.
type result struct {
	CNIVersion string  `json:"cniVersion"`
	Interfaces []iface `json:"interfaces,omitempty"`
}

type iface struct {
	Name    string `json:"name"`
	Mac     string `json:"mac,omitempty"`
	Sandbox string `json:"sandbox,omitempty"`
}

func printResult(r *result) error {
	return json.NewEncoder(os.Stdout).Encode(r)
}

// openNetns opens the container's network namespace and a netlink handle
// working inside it, so nothing here has to move a thread between
// namespaces.
func openNetns(path string) (netns.NsHandle, *netlink.Handle, error) {
	ns, err := netns.GetFromPath(path)
	if err != nil {
		return 0, nil, fail(errInternal, "open netns %s: %v", path, err)
	}
	h, err := netlink.NewHandleAt(ns)
	if err != nil {
		ns.Close()
		return 0, nil, fail(errInternal, "netlink in %s: %v", path, err)
	}
	return ns, h, nil
}

func cmdAdd(req *request) error {
	ns, h, err := openNetns(req.netns)
	if err != nil {
		return err
	}
	defer ns.Close()
	defer h.Close()
	lo, err := h.LinkByName("lo")
	if err != nil {
		return err
	}
	if err := h.LinkSetUp(lo); err != nil {
		return fmt.Errorf("bring lo up: %w", err)
	}
	host, ctr, err := addVeth(req, ns, h)
	if err != nil {
		return err
	}
	r := &result{CNIVersion: req.conf.CNIVersion, Interfaces: []iface{
		{Name: host.Attrs().Name, Mac: host.Attrs().HardwareAddr.String()},
		{Name: ctr.Attrs().Name, Mac: ctr.Attrs().HardwareAddr.String(), Sandbox: req.netns},
	}}
	return printResult(r)
}

// hostVeth names the node's end of a container's pair. It is derived from
// the container and interface, so DEL can find it again, and fits the
// kernel's 15-character limit.
func hostVeth(req *request) string {
	sum := sha256.Sum256([]byte(req.containerID + "/" + req.ifname))
	return "veth" + hex.EncodeToString(sum[:])[:11]
}

// addVeth makes the pair with its peer created straight inside the
// container, so CNI_IFNAME never has to be free on the node, and brings both
// ends up.
func addVeth(req *request, ns netns.NsHandle, h *netlink.Handle) (netlink.Link, netlink.Link, error) {
	veth := &netlink.Veth{
		LinkAttrs:     netlink.LinkAttrs{Name: hostVeth(req)},
		PeerName:      req.ifname,
		PeerNamespace: netlink.NsFd(ns),
	}
	if err := netlink.LinkAdd(veth); err != nil {
		return nil, nil, fmt.Errorf("create veth %s: %w", veth.Name, err)
	}
	host, err := netlink.LinkByName(veth.Name)
	if err != nil {
		return nil, nil, err
	}
	ctr, err := h.LinkByName(req.ifname)
	if err != nil {
		return nil, nil, err
	}
	if err := netlink.LinkSetUp(host); err != nil {
		return nil, nil, err
	}
	if err := h.LinkSetUp(ctr); err != nil {
		return nil, nil, err
	}
	return host, ctr, nil
}
