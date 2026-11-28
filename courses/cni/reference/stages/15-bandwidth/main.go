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
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"

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
	if len(os.Args) > 1 && os.Args[1] == "conflist" {
		os.Exit(conflist(os.Args[2:]))
	}
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
	CNIVersion string            `json:"cniVersion"`
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Bridge     string            `json:"bridge"`
	MTU        int               `json:"mtu"`
	DNS        json.RawMessage   `json:"dns"`
	Sysctl     map[string]string `json:"sysctl"`
	PrevResult *result           `json:"prevResult"`

	RuntimeConfig struct {
		PortMappings []portMapping `json:"portMappings"`
		Bandwidth    *bandwidth    `json:"bandwidth"`
	} `json:"runtimeConfig"`
	IPAM *ipamConf `json:"ipam"`
}

// ipamConf says where the container's addresses come from: listed in the
// config ("static"), or handed out from a subnet ("pool").
type ipamConf struct {
	Type      string `json:"type"`
	Addresses []struct {
		Address string `json:"address"`
		Gateway string `json:"gateway"`
	} `json:"addresses"`
	Subnet  string  `json:"subnet"`
	Gateway string  `json:"gateway"`
	DataDir string  `json:"dataDir"`
	Routes  []route `json:"routes"`
}

// route is one route for the container; with no gw it goes through the
// address's gateway.
type route struct {
	Dst string `json:"dst"`
	GW  string `json:"gw,omitempty"`
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
	switch req.conf.Type {
	case "byok8s-portmap":
		return portmap(&req)
	case "byok8s-bandwidth":
		return shape(&req)
	}
	switch req.command {
	case "ADD":
		return cmdAdd(&req)
	case "DEL":
		return cmdDel(&req)
	case "CHECK":
		return cmdCheck(&req)
	}
	return nil
}

// result is what ADD reports: the interfaces it made, their addresses, the
// routes, and DNS, in the config's spec version.
type result struct {
	CNIVersion string          `json:"cniVersion"`
	Interfaces []iface         `json:"interfaces,omitempty"`
	IPs        []ipConfig      `json:"ips,omitempty"`
	Routes     []route         `json:"routes,omitempty"`
	DNS        json.RawMessage `json:"dns,omitempty"`
}

type ipConfig struct {
	Address   string `json:"address"`
	Gateway   string `json:"gateway,omitempty"`
	Interface *int   `json:"interface,omitempty"`
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
	for k := range req.conf.Sysctl {
		if !strings.HasPrefix(k, "net.") || strings.ContainsAny(k, "/") {
			return fail(errBadConfig, "sysctl %q: only net.* sysctls are per network namespace", k)
		}
	}
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
	ips, err := allocate(req)
	if err != nil {
		return err
	}
	host, ctr, err := addVeth(req, ns, h)
	if err != nil {
		return err
	}
	if err := setSysctls(ns, req.conf.Sysctl); err != nil {
		return err
	}
	if req.conf.Bridge != "" {
		if err := joinBridge(req.conf.Bridge, host, ips); err != nil {
			return err
		}
	}
	r := &result{CNIVersion: req.conf.CNIVersion, Interfaces: []iface{
		{Name: host.Attrs().Name, Mac: host.Attrs().HardwareAddr.String()},
		{Name: ctr.Attrs().Name, Mac: ctr.Attrs().HardwareAddr.String(), Sandbox: req.netns},
	}}
	for _, ip := range ips {
		addr, err := netlink.ParseAddr(ip.Address)
		if err != nil {
			return err
		}
		if err := h.AddrAdd(ctr, addr); err != nil {
			return fmt.Errorf("add %s to %s: %w", ip.Address, req.ifname, err)
		}
		ip.Interface = &[]int{1}[0]
		r.IPs = append(r.IPs, ip)
	}
	if err := addRoutes(req, h, ctr, r.IPs); err != nil {
		return err
	}
	if req.conf.IPAM != nil {
		r.Routes = req.conf.IPAM.Routes
	}
	r.DNS = req.conf.DNS
	return printResult(r)
}

// cmdDel undoes ADD and succeeds however much of it is already gone: the
// netns, the interface, or the lease. Runtimes retry DEL until it does.
func cmdDel(req *request) error {
	if req.netns != "" {
		if ns, h, err := openNetns(req.netns); err == nil {
			if l, err := h.LinkByName(req.ifname); err == nil {
				h.LinkDel(l)
			}
			h.Close()
			ns.Close()
		}
	}
	if l, err := netlink.LinkByName(hostVeth(req)); err == nil {
		netlink.LinkDel(l)
	}
	if ipam := req.conf.IPAM; ipam != nil && ipam.Type == "pool" {
		return poolRelease(req, ipam)
	}
	return nil
}

// cmdCheck confirms the container still has what prevResult, the result of
// its ADD, says it was given: the interface, its addresses, and its routes.
func cmdCheck(req *request) error {
	prev := req.conf.PrevResult
	if prev == nil {
		return fail(errBadConfig, "CHECK needs a prevResult")
	}
	ns, h, err := openNetns(req.netns)
	if err != nil {
		return err
	}
	defer ns.Close()
	defer h.Close()
	l, err := h.LinkByName(req.ifname)
	if err != nil {
		return fail(errInternal, "%s is gone from %s: %v", req.ifname, req.netns, err)
	}
	have, err := h.AddrList(l, netlink.FAMILY_V4)
	if err != nil {
		return err
	}
	for _, ip := range prev.IPs {
		if !slices.ContainsFunc(have, func(a netlink.Addr) bool { return a.IPNet.String() == ip.Address }) {
			return fail(errInternal, "%s no longer holds %s", req.ifname, ip.Address)
		}
	}
	routes, err := h.RouteList(l, netlink.FAMILY_V4)
	if err != nil {
		return err
	}
	for _, rt := range prev.Routes {
		found := slices.ContainsFunc(routes, func(r netlink.Route) bool {
			dst := "0.0.0.0/0"
			if r.Dst != nil {
				dst = r.Dst.String()
			}
			return dst == rt.Dst
		})
		if !found {
			return fail(errInternal, "route to %s is gone from %s", rt.Dst, req.netns)
		}
	}
	return nil
}

// addRoutes installs the ipam routes in the container. A route with no gw
// goes through the first address's gateway.
func addRoutes(req *request, h *netlink.Handle, ctr netlink.Link, ips []ipConfig) error {
	if req.conf.IPAM == nil {
		return nil
	}
	for _, rt := range req.conf.IPAM.Routes {
		_, dst, err := net.ParseCIDR(rt.Dst)
		if err != nil {
			return fail(errBadConfig, "route dst %q: %v", rt.Dst, err)
		}
		gw := rt.GW
		if gw == "" && len(ips) > 0 {
			gw = ips[0].Gateway
		}
		nr := &netlink.Route{LinkIndex: ctr.Attrs().Index, Dst: dst, Gw: net.ParseIP(gw)}
		if err := h.RouteAdd(nr); err != nil {
			return fmt.Errorf("add route %s via %s: %w", rt.Dst, gw, err)
		}
	}
	return nil
}

// allocate decides the container's addresses, before anything is created,
// so a bad config leaves nothing behind.
func allocate(req *request) ([]ipConfig, error) {
	ipam := req.conf.IPAM
	if ipam == nil {
		return nil, nil
	}
	switch ipam.Type {
	case "static":
		var ips []ipConfig
		for _, a := range ipam.Addresses {
			if _, _, err := net.ParseCIDR(a.Address); err != nil {
				return nil, fail(errBadConfig, "ipam address %q: %v", a.Address, err)
			}
			if a.Gateway != "" && net.ParseIP(a.Gateway) == nil {
				return nil, fail(errBadConfig, "ipam gateway %q is not an address", a.Gateway)
			}
			ips = append(ips, ipConfig{Address: a.Address, Gateway: a.Gateway})
		}
		return ips, nil
	case "pool":
		ip, err := poolAllocate(req, ipam)
		if err != nil {
			return nil, err
		}
		return []ipConfig{ip}, nil
	}
	return nil, fail(errBadConfig, "unknown ipam type %q", ipam.Type)
}

// pool is a subnet, its gateway, and the directory holding its leases: one
// file per address in use, named for the address, holding its owner.
type pool struct {
	subnet  *net.IPNet
	gateway net.IP
	dir     string
}

func openPool(ipam *ipamConf) (*pool, error) {
	_, subnet, err := net.ParseCIDR(ipam.Subnet)
	if err != nil || subnet.IP.To4() == nil {
		return nil, fail(errBadConfig, "ipam subnet %q is not an IPv4 CIDR", ipam.Subnet)
	}
	if ipam.DataDir == "" {
		return nil, fail(errBadConfig, "ipam dataDir is not set")
	}
	gw := nextIP(subnet.IP)
	if ipam.Gateway != "" {
		if gw = net.ParseIP(ipam.Gateway).To4(); gw == nil {
			return nil, fail(errBadConfig, "ipam gateway %q is not an address", ipam.Gateway)
		}
	}
	return &pool{subnet: subnet, gateway: gw, dir: ipam.DataDir}, nil
}

// lock holds an exclusive flock on the pool until the returned func runs.
// Runtimes start plugins in parallel, and only the lock keeps two from
// taking one address.
func (p *pool) lock() (func(), error) {
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(p.dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() { f.Close() }, nil
}

func owner(req *request) string { return req.containerID + "/" + req.ifname }

// poolAllocate leases the lowest free address to the container, or the one
// it already holds, skipping the network, gateway and broadcast addresses.
func poolAllocate(req *request, ipam *ipamConf) (ipConfig, error) {
	p, err := openPool(ipam)
	if err != nil {
		return ipConfig{}, err
	}
	unlock, err := p.lock()
	if err != nil {
		return ipConfig{}, err
	}
	defer unlock()
	ones, _ := p.subnet.Mask.Size()
	lease := func(ip net.IP) ipConfig {
		return ipConfig{Address: fmt.Sprintf("%s/%d", ip, ones), Gateway: p.gateway.String()}
	}
	bcast := broadcast(p.subnet)
	var free net.IP
	for ip := nextIP(p.subnet.IP); p.subnet.Contains(ip) && !ip.Equal(bcast); ip = nextIP(ip) {
		if ip.Equal(p.gateway) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(p.dir, ip.String()))
		if err == nil && strings.TrimSpace(string(b)) == owner(req) {
			return lease(ip), nil
		}
		if errors.Is(err, os.ErrNotExist) && free == nil {
			free = ip
		}
	}
	if free == nil {
		return ipConfig{}, fail(errInternal+1, "no free address in %s", p.subnet)
	}
	if err := os.WriteFile(filepath.Join(p.dir, free.String()), []byte(owner(req)), 0o644); err != nil {
		return ipConfig{}, err
	}
	return lease(free), nil
}

// poolRelease drops every lease the container holds.
func poolRelease(req *request, ipam *ipamConf) error {
	p, err := openPool(ipam)
	if err != nil {
		return err
	}
	unlock, err := p.lock()
	if err != nil {
		return err
	}
	defer unlock()
	files, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		path := filepath.Join(p.dir, f.Name())
		if b, err := os.ReadFile(path); err == nil && f.Name() != "lock" && strings.TrimSpace(string(b)) == owner(req) {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	return nil
}

func nextIP(ip net.IP) net.IP {
	n := slices.Clone(ip.To4())
	for i := len(n) - 1; i >= 0; i-- {
		if n[i]++; n[i] != 0 {
			break
		}
	}
	return n
}

func broadcast(n *net.IPNet) net.IP {
	b := slices.Clone(n.IP.To4())
	for i := range b {
		b[i] |= ^n.Mask[i]
	}
	return b
}

// setSysctls writes sysctls inside the container's netns. /proc/sys/net
// shows the netns of the thread that opens it, so the writes happen on a
// thread moved into ns and back.
func setSysctls(ns netns.NsHandle, sysctls map[string]string) error {
	if len(sysctls) == 0 {
		return nil
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	orig, err := netns.Get()
	if err != nil {
		return err
	}
	defer orig.Close()
	if err := netns.Set(ns); err != nil {
		return err
	}
	defer netns.Set(orig)
	for k, v := range sysctls {
		path := "/proc/sys/" + strings.ReplaceAll(k, ".", "/")
		if err := os.WriteFile(path, []byte(v), 0o644); err != nil {
			return fail(errBadConfig, "sysctl %s=%q: %v", k, v, err)
		}
	}
	return nil
}

// joinBridge attaches the host end to the bridge, creating the bridge on
// first use and giving it each address's gateway, so it routes for the
// containers behind it.
func joinBridge(name string, host netlink.Link, ips []ipConfig) error {
	br := &netlink.Bridge{LinkAttrs: netlink.LinkAttrs{Name: name}}
	if err := netlink.LinkAdd(br); err != nil && !errors.Is(err, syscall.EEXIST) {
		return fmt.Errorf("create bridge %s: %w", name, err)
	}
	l, err := netlink.LinkByName(name)
	if err != nil {
		return err
	}
	if _, ok := l.(*netlink.Bridge); !ok {
		return fail(errBadConfig, "%s exists and is not a bridge", name)
	}
	for _, ip := range ips {
		if ip.Gateway == "" {
			continue
		}
		_, subnet, _ := net.ParseCIDR(ip.Address)
		ones, _ := subnet.Mask.Size()
		gw, err := netlink.ParseAddr(fmt.Sprintf("%s/%d", ip.Gateway, ones))
		if err != nil {
			return err
		}
		if err := netlink.AddrAdd(l, gw); err != nil && !errors.Is(err, syscall.EEXIST) {
			return fmt.Errorf("add %s to %s: %w", gw, name, err)
		}
	}
	if err := netlink.LinkSetUp(l); err != nil {
		return err
	}
	return netlink.LinkSetMaster(host, l)
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
		LinkAttrs:     netlink.LinkAttrs{Name: hostVeth(req), MTU: req.conf.MTU},
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

// conflist plays the runtime for a plugin list:
//
//	conflist --cache DIR ADD|CHECK|DEL FILE
//
// with the container's CNI_* variables in the environment. It runs the
// list's plugins, found on CNI_PATH, in order (DEL in reverse), and stops at
// the first failure, passing its error on. ADD's final result is cached, as
// every later CHECK and DEL gets it as prevResult.
func conflist(args []string) int {
	if len(args) != 4 || args[0] != "--cache" {
		fmt.Fprintln(os.Stderr, "usage: conflist --cache DIR ADD|CHECK|DEL FILE")
		return 2
	}
	cacheDir, command, file := args[1], args[2], args[3]
	report := func(err error) int {
		json.NewEncoder(os.Stdout).Encode(cniError{CNIVersion: "1.0.0", Code: errInternal, Msg: err.Error()})
		return 1
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return report(err)
	}
	var list struct {
		CNIVersion string            `json:"cniVersion"`
		Name       string            `json:"name"`
		Plugins    []json.RawMessage `json:"plugins"`
	}
	if err := json.Unmarshal(b, &list); err != nil {
		return report(fmt.Errorf("decode %s: %w", file, err))
	}
	cache := filepath.Join(cacheDir, list.Name+"-"+os.Getenv("CNI_CONTAINERID")+"-"+os.Getenv("CNI_IFNAME"))
	var prev json.RawMessage
	if command != "ADD" {
		prev, _ = os.ReadFile(cache)
	}
	plugins := slices.Clone(list.Plugins)
	if command == "DEL" {
		slices.Reverse(plugins)
	}
	for _, raw := range plugins {
		var conf map[string]any
		if err := json.Unmarshal(raw, &conf); err != nil {
			return report(err)
		}
		conf["name"], conf["cniVersion"] = list.Name, list.CNIVersion
		if len(prev) > 0 {
			conf["prevResult"] = prev
		}
		typ, _ := conf["type"].(string)
		bin, err := findPlugin(typ)
		if err != nil {
			return report(err)
		}
		stdin, _ := json.Marshal(conf)
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(), "CNI_COMMAND="+command)
		cmd.Stdin = strings.NewReader(string(stdin))
		cmd.Stderr = os.Stderr
		out, err := cmd.Output()
		if err != nil {
			var ee *exec.ExitError
			if !errors.As(err, &ee) {
				return report(fmt.Errorf("run %s: %w", typ, err))
			}
			os.Stdout.Write(out)
			return ee.ExitCode()
		}
		if command == "ADD" {
			prev = json.RawMessage(strings.TrimSpace(string(out)))
		}
	}
	switch command {
	case "ADD":
		if err := os.MkdirAll(cacheDir, 0o755); err != nil {
			return report(err)
		}
		if err := os.WriteFile(cache, prev, 0o644); err != nil {
			return report(err)
		}
		fmt.Println(string(prev))
	case "DEL":
		os.Remove(cache)
	}
	return 0
}

// findPlugin is the first executable named typ in a CNI_PATH directory.
func findPlugin(typ string) (string, error) {
	if typ == "" || strings.Contains(typ, "/") {
		return "", fmt.Errorf("plugin type %q is not a plugin name", typ)
	}
	for _, dir := range filepath.SplitList(os.Getenv("CNI_PATH")) {
		p := filepath.Join(dir, typ)
		if fi, err := os.Stat(p); err == nil && fi.Mode().IsRegular() && fi.Mode()&0o111 != 0 {
			return p, nil
		}
	}
	return "", fmt.Errorf("no plugin %q on CNI_PATH %q", typ, os.Getenv("CNI_PATH"))
}

type portMapping struct {
	HostPort      int    `json:"hostPort"`
	ContainerPort int    `json:"containerPort"`
	Protocol      string `json:"protocol"`
}

// portmapTable holds every container's forwarding, one DNAT rule per
// mapping, commented with the container's ID so DEL finds its own.
const portmapTable = "byok8s-cni-portmap"

// nft runs an nft script.
func nft(script string) error {
	cmd := exec.Command("nft", "-f", "-")
	cmd.Stdin = strings.NewReader(script)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nft: %v: %s", err, out)
	}
	return nil
}

// portmap is a chained plugin: on ADD it forwards each hostPort on the node
// to the container's address from prevResult, for traffic arriving from
// outside (prerouting) and from the node itself (output).
func portmap(req *request) error {
	switch req.command {
	case "ADD":
		prev := req.conf.PrevResult
		if prev == nil || len(prev.IPs) == 0 {
			return fail(errBadConfig, "portmap needs a prevResult with the container's address")
		}
		ip, _, _ := strings.Cut(prev.IPs[0].Address, "/")
		var b strings.Builder
		fmt.Fprintf(&b, "table ip %[1]s {\n chain pre { type nat hook prerouting priority -100; }\n chain out { type nat hook output priority -100; }\n}\n", portmapTable)
		for _, m := range req.conf.RuntimeConfig.PortMappings {
			proto := strings.ToLower(m.Protocol)
			if proto == "" {
				proto = "tcp"
			}
			if proto != "tcp" && proto != "udp" {
				return fail(errBadConfig, "portmap protocol %q", m.Protocol)
			}
			for _, chain := range []string{"pre", "out"} {
				fmt.Fprintf(&b, "add rule ip %s %s fib daddr type local %s dport %d dnat to %s:%d comment %q\n",
					portmapTable, chain, proto, m.HostPort, ip, m.ContainerPort, req.containerID)
			}
		}
		if err := nft(b.String()); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(prev)
	}
	return nil
}

// bandwidth is in bits per second, and bits of burst.
type bandwidth struct {
	IngressRate  int64 `json:"ingressRate"`
	IngressBurst int64 `json:"ingressBurst"`
}

// shape is a chained plugin that limits what the container receives with a
// token bucket on the host end of its veth: what that end sends is what the
// container gets.
func shape(req *request) error {
	if req.command != "ADD" {
		return nil
	}
	prev := req.conf.PrevResult
	if prev == nil {
		return fail(errBadConfig, "bandwidth needs a prevResult to find the container's host interface")
	}
	bw := req.conf.RuntimeConfig.Bandwidth
	if bw != nil && bw.IngressRate > 0 {
		var host string
		for _, i := range prev.Interfaces {
			if i.Sandbox == "" {
				host = i.Name
			}
		}
		if host == "" {
			return fail(errBadConfig, "prevResult lists no host interface")
		}
		burst := max(bw.IngressBurst/8, 1600)
		out, err := exec.Command("tc", "qdisc", "replace", "dev", host, "root", "tbf",
			"rate", fmt.Sprintf("%dbit", bw.IngressRate), "burst", fmt.Sprint(burst), "latency", "25ms").CombinedOutput()
		if err != nil {
			return fmt.Errorf("tc on %s: %v: %s", host, err, out)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(prev)
}
