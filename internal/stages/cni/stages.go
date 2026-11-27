package cni

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/madhank93/byo-k8s-x/internal/runner"
)

func init() {
	register(1, "plugin-contract", stagePluginContract)
	register(2, "version", stageVersion)
	register(3, "netns", stageNetns)
	register(4, "veth", stageVeth)
	register(5, "ipam-static", stageIPAMStatic)
	register(6, "ipam-persist", stageIPAMPersist)
	register(7, "routes", stageRoutes)
	register(8, "bridge", stageBridge)
	register(9, "result", stageResult)
	register(10, "sysctls", stageSysctls)
	register(11, "del", stageDel)
	register(12, "check", stageCheck)
	register(13, "conflist", stageConflist)
	register(14, "portmap", stagePortmap)
	register(15, "bandwidth", stageBandwidth)
	register(16, "hostport-cleanup", stageHostportCleanup)
	register(17, "node-routes", stageNodeRoutes)
	register(18, "vxlan", stageVXLAN)
	register(19, "netpol-lite", stageNetpolLite)
	register(20, "kubelet-drives-it", stageKubeletDrivesIt)
}

// stagePluginContract — the plugin reads its command from CNI_COMMAND and
// its config from stdin, and reports a failure as a CNI error on stdout.
func stagePluginContract(w *work) error {
	_, path, err := w.netns(node1, "c")
	if err != nil {
		return err
	}
	good := w.conf(nil)
	full := call{command: "ADD", id: w.id("c"), netns: path, ifname: "eth0"}
	cases := []struct {
		c    call
		conf any
		code int
		why  string
	}{
		{call{id: w.id("c"), netns: path, ifname: "eth0"}, good, 4, "CNI_COMMAND is not set"},
		{call{command: "FROB", id: w.id("c"), netns: path, ifname: "eth0"}, good, 4, "FROB is not a CNI command"},
		{call{command: "ADD", netns: path, ifname: "eth0"}, good, 4, "ADD without CNI_CONTAINERID"},
		{call{command: "ADD", id: w.id("c"), ifname: "eth0"}, good, 4, "ADD without CNI_NETNS"},
		{call{command: "ADD", id: w.id("c"), netns: path}, good, 4, "ADD without CNI_IFNAME"},
		{full, "{not json", 6, "the config on stdin is not JSON"},
	}
	for _, tc := range cases {
		res, err := w.invoke(tc.c, tc.conf)
		if err != nil {
			return err
		}
		if err := wantCNIError(res, tc.code, tc.why); err != nil {
			return err
		}
	}
	return nil
}

// stageVersion — VERSION reports the spec versions the plugin speaks, and a
// config in any other version is refused before anything is done.
func stageVersion(w *work) error {
	res, err := w.invoke(call{command: "VERSION"}, `{"cniVersion":"1.0.0"}`)
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("VERSION: %w", err)
	}
	var v struct {
		CNIVersion        string   `json:"cniVersion"`
		SupportedVersions []string `json:"supportedVersions"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &v); err != nil || v.CNIVersion == "" {
		return fmt.Errorf("VERSION must print {\"cniVersion\":…,\"supportedVersions\":[…]}, got %q", res.Stdout)
	}
	if !slices.Contains(v.SupportedVersions, "1.0.0") {
		return fmt.Errorf("VERSION lists %v; the plugin must speak 1.0.0", v.SupportedVersions)
	}
	_, path, err := w.netns(node1, "c")
	if err != nil {
		return err
	}
	for _, bad := range []string{"0.2.0", "2.0.0"} {
		res, err := w.invoke(call{command: "ADD", id: w.id("c"), netns: path, ifname: "eth0"}, w.conf(map[string]any{"cniVersion": bad}))
		if err != nil {
			return err
		}
		if err := wantCNIError(res, 1, "the config asks for cniVersion "+bad+", which the plugin does not support"); err != nil {
			return err
		}
	}
	return nil
}

// stageNetns — ADD enters the container's network namespace and brings its
// loopback up there, leaving the node's alone.
func stageNetns(w *work) error {
	r, ns, err := w.add(node1, "c", w.conf(nil))
	if err != nil {
		return err
	}
	if r.CNIVersion != "1.0.0" {
		return fmt.Errorf("the result's cniVersion is %q; it must be the config's, 1.0.0", r.CNIVersion)
	}
	ls, err := w.links(node1, ns)
	if err != nil {
		return err
	}
	if !ls["lo"].up() {
		return fmt.Errorf("lo in the container's netns is still down after ADD")
	}
	res, err := w.invoke(call{command: "ADD", id: w.id("gone"), netns: "/run/netns/" + prefix + "-does-not-exist", ifname: "eth0"}, w.conf(nil))
	if err != nil {
		return err
	}
	return wantCNIError(res, 0, "CNI_NETNS names no namespace")
}

// stageVeth — ADD creates a veth pair: CNI_IFNAME inside the container, its
// peer on the node, both up, and both reported in the result.
func stageVeth(w *work) error {
	hosts := map[string]bool{}
	for _, tc := range []struct{ name, ifname string }{{"a", "eth0"}, {"b", "net1"}} {
		ns, path, err := w.netns(node1, tc.name)
		if err != nil {
			return err
		}
		r, err := w.ok(call{command: "ADD", id: w.id(tc.name), netns: path, ifname: tc.ifname}, w.conf(nil), "ADD of "+tc.name)
		if err != nil {
			return err
		}
		inside, err := w.links(node1, ns)
		if err != nil {
			return err
		}
		l, ok := inside[tc.ifname]
		if !ok || l.Info.Kind != "veth" || !l.up() {
			return fmt.Errorf("container %s: expected an up veth named %s (CNI_IFNAME) in its netns, got %+v", tc.name, tc.ifname, l)
		}
		var sandboxed bool
		for _, i := range r.Interfaces {
			sandboxed = sandboxed || (i.Name == tc.ifname && i.Sandbox == path)
		}
		if !sandboxed {
			return fmt.Errorf("container %s: the result must list %s with \"sandbox\": %q, got %s", tc.name, tc.ifname, path, r.raw)
		}
		h := r.host()
		if h == "" {
			return fmt.Errorf("container %s: the result must list the node's end of the pair, an interface with no sandbox; got %s", tc.name, r.raw)
		}
		if hosts[h] {
			return fmt.Errorf("two containers got the same host interface name %s", h)
		}
		hosts[h] = true
		outside, err := w.links(node1, "")
		if err != nil {
			return err
		}
		if hl, ok := outside[h]; !ok || hl.Info.Kind != "veth" || !hl.up() {
			return fmt.Errorf("container %s: the result names %s as the host end, but the node has no up veth by that name", tc.name, h)
		}
	}
	return nil
}

// stageIPAMStatic — a static address from the config lands on the
// container's interface and in the result.
func stageIPAMStatic(w *work) error {
	addr := w.ip(10) + "/24"
	conf := w.conf(map[string]any{"ipam": map[string]any{
		"type":      "static",
		"addresses": []any{map[string]any{"address": addr, "gateway": w.ip(1)}},
	}})
	r, ns, err := w.add(node1, "c", conf)
	if err != nil {
		return err
	}
	got, err := w.addrs(node1, ns, "eth0")
	if err != nil {
		return err
	}
	if !slices.Equal(got, []string{addr}) {
		return fmt.Errorf("eth0 in the container holds %v, expected [%s]", got, addr)
	}
	if err := wantIP(r, addr, w.ip(1)); err != nil {
		return err
	}
	bad := w.conf(map[string]any{"ipam": map[string]any{
		"type": "static", "addresses": []any{map[string]any{"address": w.ip(300) + "/24"}},
	}})
	_, path, err := w.netns(node1, "bad")
	if err != nil {
		return err
	}
	res, err := w.invoke(call{command: "ADD", id: w.id("bad"), netns: path, ifname: "eth0"}, bad)
	if err != nil {
		return err
	}
	return wantCNIError(res, 7, w.ip(300)+" is not an address")
}

// wantIP checks the result's first address, its gateway, and that its
// interface index points at the container's interface.
func wantIP(r *result, addr, gw string) error {
	if len(r.IPs) == 0 || r.IPs[0].Address != addr || r.IPs[0].Gateway != gw {
		return fmt.Errorf("the result's ips must be [{\"address\":%q,\"gateway\":%q,…}], got %s", addr, gw, r.raw)
	}
	i := r.IPs[0].Interface
	if i == nil || *i < 0 || *i >= len(r.Interfaces) || r.Interfaces[*i].Sandbox == "" {
		return fmt.Errorf("the result's ips[0].interface must be the index, in interfaces, of the container's interface; got %s", r.raw)
	}
	return nil
}

// stageIPAMPersist — a pool hands out each address once, across processes
// running at the same time, and refuses when it is empty.
func stageIPAMPersist(w *work) error {
	cidr := fmt.Sprintf("10.217.%d.0/29", w.n)
	conf := w.conf(map[string]any{"ipam": w.pool(cidr)})
	type out struct {
		r   *result
		ns  string
		err error
	}
	outs := make([]out, 5)
	var wg sync.WaitGroup
	for i := range outs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, ns, err := w.add(node1, fmt.Sprintf("c%d", i), conf)
			outs[i] = out{r, ns, err}
		}()
	}
	wg.Wait()
	seen := map[string]string{}
	for i, o := range outs {
		if o.err != nil {
			return fmt.Errorf("five containers ADDed at once to a /29 (five free addresses): %w", o.err)
		}
		a := o.r.addr()
		if !slices.Contains([]string{w.ip(2), w.ip(3), w.ip(4), w.ip(5), w.ip(6)}, a) {
			return fmt.Errorf("container c%d got %q from %s; the usable addresses are .2-.6 (.0 is the network, .1 the gateway, .7 broadcast)", i, o.r.raw, cidr)
		}
		if prev, dup := seen[a]; dup {
			return fmt.Errorf("%s was handed to both %s and c%d", a, prev, i)
		}
		seen[a] = fmt.Sprintf("c%d", i)
		if err := wantIP(o.r, a+"/29", w.ip(1)); err != nil {
			return err
		}
		got, err := w.addrs(node1, o.ns, "eth0")
		if err != nil {
			return err
		}
		if !slices.Equal(got, []string{a + "/29"}) {
			return fmt.Errorf("c%d's eth0 holds %v, expected [%s/29]", i, got, a)
		}
	}
	_, path, err := w.netns(node1, "full")
	if err != nil {
		return err
	}
	res, err := w.invoke(call{command: "ADD", id: w.id("full"), netns: path, ifname: "eth0"}, conf)
	if err != nil {
		return err
	}
	if err := wantCNIError(res, 0, "every address in "+cidr+" is taken"); err != nil {
		return err
	}
	return w.waitsForLock()
}

// waitsForLock holds the pool's lock, an flock on dataDir/lock, and checks an
// ADD waits for it. Racing processes collide too rarely to catch a missing
// lock any other way.
func (w *work) waitsForLock() error {
	const hold = 3 * time.Second
	lock := w.ipamDir() + "/lock"
	conf := w.conf(map[string]any{"ipam": w.pool(fmt.Sprintf("10.217.%d.0/24", w.n))})
	done := make(chan error, 1)
	go func() {
		_, err := w.sh(node1, fmt.Sprintf("mkdir -p %s && flock %s sleep %d", w.ipamDir(), lock, int(hold.Seconds())))
		done <- err
	}()
	time.Sleep(time.Second)
	start := time.Now()
	_, _, err := w.add(node1, "waiter", conf)
	took := time.Since(start)
	if lerr := <-done; lerr != nil {
		return lerr
	}
	if err != nil {
		return err
	}
	if took < hold-1500*time.Millisecond {
		return fmt.Errorf("with %s held by another process, ADD returned after %s instead of waiting for it; without the lock, two plugins running at once can lease the same address", lock, took.Round(time.Millisecond))
	}
	return nil
}

// stageRoutes — ipam routes become routes in the container, through the
// gateway unless a route names its own.
func stageRoutes(w *work) error {
	ipam := w.pool(w.subnet())
	ipam["routes"] = []any{
		map[string]any{"dst": "0.0.0.0/0"},
		map[string]any{"dst": "10.99.0.0/16", "gw": w.ip(254)},
	}
	r, ns, err := w.add(node1, "c", w.conf(map[string]any{"ipam": ipam}))
	if err != nil {
		return err
	}
	table, err := w.routes(node1, ns)
	if err != nil {
		return err
	}
	for _, want := range []string{"default via " + w.ip(1) + " dev eth0", "10.99.0.0/16 via " + w.ip(254) + " dev eth0"} {
		if !strings.Contains(table, want) {
			return fmt.Errorf("the container's routing table has no %q:\n%s", want, tail(table))
		}
	}
	var def, extra bool
	for _, rt := range r.Routes {
		def = def || rt.Dst == "0.0.0.0/0"
		extra = extra || (rt.Dst == "10.99.0.0/16" && rt.GW == w.ip(254))
	}
	if !def || !extra {
		return fmt.Errorf("the result's routes must list both routes from the config; got %s", r.raw)
	}
	return nil
}

// stageBridge — the host ends join a bridge that holds the gateway address,
// so containers reach each other and the node reaches them.
func stageBridge(w *work) error {
	conf := w.conf(map[string]any{"bridge": w.bridge(), "ipam": w.pool(w.subnet())})
	var rs []*result
	var nss []string
	for _, name := range []string{"a", "b", "c"} {
		r, ns, err := w.add(node1, name, conf)
		if err != nil {
			return err
		}
		rs, nss = append(rs, r), append(nss, ns)
	}
	ls, err := w.links(node1, "")
	if err != nil {
		return err
	}
	br, ok := ls[w.bridge()]
	if !ok || br.Info.Kind != "bridge" || !br.up() {
		return fmt.Errorf("expected an up bridge %s on the node", w.bridge())
	}
	got, err := w.addrs(node1, "", w.bridge())
	if err != nil {
		return err
	}
	if !slices.Contains(got, w.ip(1)+"/24") {
		return fmt.Errorf("%s holds %v; it is the gateway, so it must hold %s/24", w.bridge(), got, w.ip(1))
	}
	for i, r := range rs {
		if m := ls[r.host()].Master; m != w.bridge() {
			return fmt.Errorf("container %d's host end %s is attached to %q, not %s", i, r.host(), m, w.bridge())
		}
	}
	a, b := end{node1, nss[0]}, end{node1, nss[1]}
	src, err := w.dial(a, b, rs[1].addr(), port())
	if err != nil {
		return err
	}
	if src != rs[0].addr() {
		return fmt.Errorf("container b saw a connection from a arrive from %s, not a's address %s", src, rs[0].addr())
	}
	if _, err := w.dial(end{node1, ""}, a, rs[0].addr(), port()); err != nil {
		return err
	}
	_, err = w.dial(b, end{node1, ""}, w.ip(1), port())
	return err
}

// stageResult — the result describes what was built: MTU applied to both
// ends, real MACs, and the config's DNS passed back.
func stageResult(w *work) error {
	dns := map[string]any{"nameservers": []any{"10.96.0.10"}, "search": []any{"svc.cluster.local"}}
	r, ns, err := w.add(node1, "c", w.conf(map[string]any{
		"bridge": w.bridge(), "mtu": 1400, "dns": dns, "ipam": w.pool(w.subnet()),
	}))
	if err != nil {
		return err
	}
	inside, err := w.links(node1, ns)
	if err != nil {
		return err
	}
	outside, err := w.links(node1, "")
	if err != nil {
		return err
	}
	for _, i := range r.Interfaces {
		l, ok := outside[i.Name]
		if i.Sandbox != "" {
			l, ok = inside[i.Name]
		}
		if !ok {
			return fmt.Errorf("the result lists %s, which does not exist", i.Name)
		}
		if l.Mtu != 1400 {
			return fmt.Errorf("%s has MTU %d; the config asks for 1400", i.Name, l.Mtu)
		}
		if !strings.EqualFold(l.Mac, i.Mac) {
			return fmt.Errorf("the result says %s has MAC %q; it has %s", i.Name, i.Mac, l.Mac)
		}
	}
	if len(r.Interfaces) != 2 {
		return fmt.Errorf("the result must list both ends of the pair, got %s", r.raw)
	}
	if err := wantIP(r, r.addr()+"/24", w.ip(1)); err != nil {
		return err
	}
	if !reflect.DeepEqual(r.DNS, dns) {
		return fmt.Errorf("the result's dns must be the config's %v, got %v", dns, r.DNS)
	}
	return nil
}

// stageSysctls — sysctls from the config are set inside the container's
// netns, never the node's, and only net.* ones are accepted.
func stageSysctls(w *work) error {
	before, err := w.sh(node1, "sysctl -n net.ipv4.ip_unprivileged_port_start kernel.domainname")
	if err != nil {
		return err
	}
	_, ns, err := w.add(node1, "c", w.conf(map[string]any{
		"bridge": w.bridge(), "ipam": w.pool(w.subnet()),
		"sysctl": map[string]any{"net.ipv4.ip_unprivileged_port_start": "80", "net.ipv4.ping_group_range": "0 1000"},
	}))
	if err != nil {
		return err
	}
	got, err := w.in(node1, ns, "cat /proc/sys/net/ipv4/ip_unprivileged_port_start; cat /proc/sys/net/ipv4/ping_group_range")
	if err != nil {
		return err
	}
	if f := strings.Fields(got); !slices.Equal(f, []string{"80", "0", "1000"}) {
		return fmt.Errorf("inside the container, ip_unprivileged_port_start and ping_group_range read %q; expected 80 and \"0 1000\"", got)
	}
	_, path, err := w.netns(node1, "bad")
	if err != nil {
		return err
	}
	res, err := w.invoke(call{command: "ADD", id: w.id("bad"), netns: path, ifname: "eth0"}, w.conf(map[string]any{
		"ipam": w.pool(w.subnet()), "sysctl": map[string]any{"kernel.domainname": "pwned"},
	}))
	if err != nil {
		return err
	}
	if err := wantCNIError(res, 0, "kernel.domainname is not a network sysctl"); err != nil {
		return err
	}
	after, err := w.sh(node1, "sysctl -n net.ipv4.ip_unprivileged_port_start kernel.domainname")
	if err != nil {
		return err
	}
	if after != before {
		return fmt.Errorf("the node's own sysctls changed from %q to %q; only the container's netns may change", before, after)
	}
	return nil
}

// stageDel — DEL takes the interface down with it and returns the address to
// the pool, and succeeds however little is left to delete.
func stageDel(w *work) error {
	conf := w.conf(map[string]any{"bridge": w.bridge(), "ipam": w.pool(fmt.Sprintf("10.217.%d.0/29", w.n))})
	rs := map[string]*result{}
	nss := map[string]string{}
	for _, name := range []string{"c1", "c2", "c3", "c4", "c5"} {
		r, ns, err := w.add(node1, name, conf)
		if err != nil {
			return err
		}
		rs[name], nss[name] = r, ns
	}
	del := func(name, netns string) error {
		_, err := w.ok(call{command: "DEL", id: w.id(name), netns: netns, ifname: "eth0"}, conf, "DEL of "+name)
		return err
	}
	if err := del("c3", "/run/netns/"+nss["c3"]); err != nil {
		return err
	}
	inside, err := w.links(node1, nss["c3"])
	if err != nil {
		return err
	}
	if _, ok := inside["eth0"]; ok {
		return fmt.Errorf("after DEL, eth0 is still in c3's netns")
	}
	outside, err := w.links(node1, "")
	if err != nil {
		return err
	}
	if _, ok := outside[rs["c3"].host()]; ok {
		return fmt.Errorf("after DEL, c3's host end %s is still on the node", rs["c3"].host())
	}
	r, _, err := w.add(node1, "c6", conf)
	if err != nil {
		return fmt.Errorf("the pool was full, c3 was deleted, so its address should be free: %w", err)
	}
	if r.addr() != rs["c3"].addr() {
		return fmt.Errorf("c6 got %s; the only free address is c3's, %s", r.addr(), rs["c3"].addr())
	}
	if err := del("c3", "/run/netns/"+nss["c3"]); err != nil {
		return fmt.Errorf("a second DEL of the same container must succeed too: %w", err)
	}
	if err := del("never-added", "/run/netns/"+prefix+"-does-not-exist"); err != nil {
		return fmt.Errorf("DEL of a container the plugin never saw must succeed: %w", err)
	}
	if _, err := w.sh(node1, "ip netns del "+nss["c4"]); err != nil {
		return err
	}
	if err := del("c4", ""); err != nil {
		return fmt.Errorf("DEL with no CNI_NETNS (the netns is already gone): %w", err)
	}
	r, _, err = w.add(node1, "c7", conf)
	if err != nil {
		return fmt.Errorf("c4 was deleted with its netns already gone; its address should still be freed: %w", err)
	}
	if r.addr() != rs["c4"].addr() {
		return fmt.Errorf("c7 got %s; the only free address is c4's, %s", r.addr(), rs["c4"].addr())
	}
	return nil
}

// stageCheck — CHECK compares the container with the result ADD returned and
// fails when any part of it has gone.
func stageCheck(w *work) error {
	base := w.conf(map[string]any{"bridge": w.bridge(), "ipam": w.pool(w.subnet())})
	check := func(name, ns string, prev *result) (*runner.Result, error) {
		conf := w.conf(map[string]any{"bridge": w.bridge(), "ipam": w.pool(w.subnet())})
		if prev != nil {
			conf["prevResult"] = json.RawMessage(prev.raw)
		}
		return w.invoke(call{command: "CHECK", id: w.id(name), netns: "/run/netns/" + ns, ifname: "eth0"}, conf)
	}
	for _, tc := range []struct{ name, breakIt, what string }{
		{"addr", "ip addr flush dev eth0", "its address was removed"},
		{"route", "ip route del default", "its default route was removed"},
		{"link", "ip link del eth0", "its interface was deleted"},
	} {
		r, ns, err := w.add(node1, tc.name, base)
		if err != nil {
			return err
		}
		res, err := check(tc.name, ns, r)
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return fmt.Errorf("CHECK of a container just added: %w", err)
		}
		if _, err := w.in(node1, ns, tc.breakIt); err != nil {
			return err
		}
		res, err = check(tc.name, ns, r)
		if err != nil {
			return err
		}
		if err := wantCNIError(res, 0, "CHECK of container "+tc.name+": "+tc.what); err != nil {
			return err
		}
	}
	_, ns, err := w.add(node1, "noprev", base)
	if err != nil {
		return err
	}
	res, err := check("noprev", ns, nil)
	if err != nil {
		return err
	}
	return wantCNIError(res, 0, "CHECK was given no prevResult")
}

// recorder is a stand-in plugin for the conflist stage. It logs each call
// beside itself, adds its name to the result's dns.search so the chain is
// visible in the result, and fails when its config says so.
const recorder = `#!/usr/bin/env python3
import json, os, sys
conf = json.load(sys.stdin)
me = os.path.basename(sys.argv[0])
with open(os.path.join(os.path.dirname(sys.argv[0]), "log"), "a") as f:
    env = {k: v for k, v in os.environ.items() if k.startswith("CNI_")}
    f.write(json.dumps({"plugin": me, "env": env, "conf": conf}) + "\n")
if conf.get("fail"):
    print(json.dumps({"cniVersion": conf["cniVersion"], "code": 999, "msg": me + " failed"}))
    sys.exit(1)
if os.environ.get("CNI_COMMAND") == "ADD":
    r = conf.get("prevResult") or {"cniVersion": conf["cniVersion"]}
    r.setdefault("dns", {}).setdefault("search", []).append(me)
    print(json.dumps(r))
`

type logged struct {
	Plugin string            `json:"plugin"`
	Env    map[string]string `json:"env"`
	Conf   map[string]any    `json:"conf"`
}

// stageConflist — `conflist` plays the runtime's part: it runs a list's
// plugins in order on ADD and in reverse on DEL, threading each result into
// the next plugin as prevResult.
func stageConflist(w *work) error {
	bin := w.dir + "/bin"
	for _, name := range []string{"rec-a", "rec-b", "rec-fail"} {
		if err := node1.WriteFile(w.ctx, bin+"/"+name, []byte(recorder), 0o755); err != nil {
			return err
		}
	}
	list := map[string]any{"cniVersion": "1.0.0", "name": "chain", "plugins": []any{
		map[string]any{"type": "rec-a", "marker": w.token},
		map[string]any{"type": "rec-b"},
	}}
	b, _ := json.Marshal(list)
	if err := node1.WriteFile(w.ctx, w.dir+"/chain.conflist", b, 0o644); err != nil {
		return err
	}
	_, path, err := w.netns(node1, "c")
	if err != nil {
		return err
	}
	c := call{id: w.id("c"), netns: path, ifname: "eth0", path: "/nonexistent:" + bin}
	run := func(cmd, file string) (*runner.Result, []logged, error) {
		if _, err := w.sh(node1, "rm -f "+bin+"/log"); err != nil {
			return nil, nil, err
		}
		res, err := w.invoke(c, "", "conflist", "--cache", w.dir+"/cache", cmd, file)
		if err != nil {
			return nil, nil, err
		}
		out, _ := node1.ReadFile(w.ctx, bin+"/log")
		var log []logged
		for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
			var e logged
			if json.Unmarshal([]byte(l), &e) == nil {
				log = append(log, e)
			}
		}
		return res, log, nil
	}
	order := func(log []logged) []string {
		var o []string
		for _, e := range log {
			o = append(o, e.Plugin+" "+e.Env["CNI_COMMAND"])
		}
		return o
	}
	search := func(v any) []any {
		m, _ := v.(map[string]any)
		d, _ := m["dns"].(map[string]any)
		s, _ := d["search"].([]any)
		return s
	}

	res, log, err := run("ADD", w.dir+"/chain.conflist")
	if err != nil {
		return err
	}
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("conflist ADD: %w", err)
	}
	if got := order(log); !slices.Equal(got, []string{"rec-a ADD", "rec-b ADD"}) {
		return fmt.Errorf("conflist ADD must run rec-a then rec-b (found on CNI_PATH), each with CNI_COMMAND=ADD; they ran as %v", got)
	}
	a, bb := log[0], log[1]
	for _, e := range log {
		if e.Conf["name"] != "chain" || e.Conf["cniVersion"] != "1.0.0" {
			return fmt.Errorf("%s's config must carry the list's name and cniVersion; got %v", e.Plugin, e.Conf)
		}
		for k, want := range map[string]string{"CNI_CONTAINERID": c.id, "CNI_NETNS": c.netns, "CNI_IFNAME": c.ifname} {
			if e.Env[k] != want {
				return fmt.Errorf("%s ran with %s=%q; the runtime passed %q", e.Plugin, k, e.Env[k], want)
			}
		}
	}
	if a.Conf["marker"] != w.token || a.Conf["prevResult"] != nil {
		return fmt.Errorf("rec-a, the first plugin, must get its own entry from the list and no prevResult; got %v", a.Conf)
	}
	if s := search(bb.Conf["prevResult"]); !reflect.DeepEqual(s, []any{"rec-a"}) {
		return fmt.Errorf("rec-b's prevResult must be what rec-a printed; got %v", bb.Conf["prevResult"])
	}
	var final map[string]any
	if json.Unmarshal([]byte(res.Stdout), &final) != nil || !reflect.DeepEqual(search(final), []any{"rec-a", "rec-b"}) {
		return fmt.Errorf("conflist ADD must print the last plugin's result; got %q", res.Stdout)
	}

	for _, cmd := range []string{"CHECK", "DEL"} {
		want := []string{"rec-a " + cmd, "rec-b " + cmd}
		if cmd == "DEL" {
			want = []string{"rec-b DEL", "rec-a DEL"}
		}
		res, log, err := run(cmd, w.dir+"/chain.conflist")
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return fmt.Errorf("conflist %s: %w", cmd, err)
		}
		if got := order(log); !slices.Equal(got, want) {
			return fmt.Errorf("conflist %s must run %v; ran %v", cmd, want, got)
		}
		for _, e := range log {
			if s := search(e.Conf["prevResult"]); !reflect.DeepEqual(s, []any{"rec-a", "rec-b"}) {
				return fmt.Errorf("on %s every plugin gets the cached result of ADD as prevResult; %s got %v", cmd, e.Plugin, e.Conf["prevResult"])
			}
		}
	}

	list["plugins"] = []any{
		map[string]any{"type": "rec-a"},
		map[string]any{"type": "rec-fail", "fail": true},
		map[string]any{"type": "rec-b"},
	}
	b, _ = json.Marshal(list)
	if err := node1.WriteFile(w.ctx, w.dir+"/fail.conflist", b, 0o644); err != nil {
		return err
	}
	res, log, err = run("ADD", w.dir+"/fail.conflist")
	if err != nil {
		return err
	}
	if err := wantCNIError(res, 999, "rec-fail failed with code 999, and the runtime passes a plugin's error on"); err != nil {
		return err
	}
	if got := order(log); slices.Contains(got, "rec-b ADD") {
		return fmt.Errorf("rec-b ran after rec-fail failed: %v", got)
	}
	return nil
}

// withPrev is a chained plugin's config: its own fields plus the result of
// the plugin before it.
func (w *work) withPrev(typ string, prev *result, fields map[string]any) map[string]any {
	c := w.conf(fields)
	c["type"] = typ
	if prev != nil {
		c["prevResult"] = json.RawMessage(prev.raw)
	}
	return c
}

// wantPassThrough checks a chained plugin printed the result it was given.
func wantPassThrough(res *runner.Result, prev *result, who string) error {
	if err := wantExit(res, 0); err != nil {
		return fmt.Errorf("%s: %w", who, err)
	}
	got, err := parseResult(res.Stdout, who)
	if err != nil {
		return err
	}
	if got.addr() != prev.addr() || len(got.Interfaces) != len(prev.Interfaces) {
		return fmt.Errorf("%s must print the prevResult it was given; got %s", who, got.raw)
	}
	return nil
}

func portmapConf(w *work, prev *result, hostPort int) map[string]any {
	return w.withPrev("byok8s-portmap", prev, map[string]any{"runtimeConfig": map[string]any{
		"portMappings": []any{map[string]any{"hostPort": hostPort, "containerPort": 8080, "protocol": "tcp"}},
	}})
}

// stagePortmap — a chained plugin forwards a port on the node to the
// container, for traffic from the node itself and from other nodes.
func stagePortmap(w *work) error {
	r, ns, err := w.add(node1, "c", w.conf(map[string]any{"bridge": w.bridge(), "ipam": w.pool(w.subnet())}))
	if err != nil {
		return err
	}
	hp := port()
	prev := r
	res, err := w.invoke(call{command: "ADD", id: w.id("c"), netns: "/run/netns/" + ns, ifname: "eth0"}, portmapConf(w, prev, hp))
	if err != nil {
		return err
	}
	if err := wantPassThrough(res, prev, "byok8s-portmap ADD"); err != nil {
		return err
	}
	ip, err := w.nodeIP(node1)
	if err != nil {
		return err
	}
	c := end{node1, ns}
	if _, err := w.dial(end{node1, ""}, c, ip, hp); err != nil {
		return fmt.Errorf("hostPort %d → containerPort 8080, from the node itself: %w", hp, err)
	}
	if _, err := w.dial(end{node2, ""}, c, ip, hp); err != nil {
		return fmt.Errorf("hostPort %d → containerPort 8080, from another node: %w", hp, err)
	}
	return nil
}

// stageBandwidth — a chained plugin shapes traffic into the container with a
// token bucket on the host end of its veth, found through prevResult.
func stageBandwidth(w *work) error {
	r, ns, err := w.add(node1, "c", w.conf(map[string]any{"bridge": w.bridge(), "ipam": w.pool(w.subnet())}))
	if err != nil {
		return err
	}
	c := call{command: "ADD", id: w.id("c"), netns: "/run/netns/" + ns, ifname: "eth0"}
	conf := w.withPrev("byok8s-bandwidth", r, map[string]any{"runtimeConfig": map[string]any{
		"bandwidth": map[string]any{"ingressRate": 8_000_000, "ingressBurst": 800_000},
	}})
	res, err := w.invoke(c, conf)
	if err != nil {
		return err
	}
	if err := wantPassThrough(res, r, "byok8s-bandwidth ADD"); err != nil {
		return err
	}
	out, err := w.sh(node1, "tc -j qdisc show dev "+r.host())
	if err != nil {
		return err
	}
	var qs []struct {
		Kind    string         `json:"kind"`
		Root    bool           `json:"root"`
		Options map[string]any `json:"options"`
	}
	json.Unmarshal([]byte(out), &qs)
	var tbf bool
	for _, q := range qs {
		rate, _ := q.Options["rate"].(float64)
		tbf = tbf || (q.Kind == "tbf" && rate == 1_000_000)
	}
	if !tbf {
		return fmt.Errorf("%s (the host end, which sends what the container receives) needs a tbf qdisc at 1000000 bytes/s for ingressRate 8000000 bits/s; tc shows %s", r.host(), out)
	}
	delete(conf, "prevResult")
	res, err = w.invoke(c, conf)
	if err != nil {
		return err
	}
	return wantCNIError(res, 0, "byok8s-bandwidth was given no prevResult, so it cannot know which interface to shape")
}

// stageHostportCleanup — DEL of the portmap plugin removes that container's
// forwarding and nobody else's, with or without a prevResult.
func stageHostportCleanup(w *work) error {
	conf := w.conf(map[string]any{"bridge": w.bridge(), "ipam": w.pool(w.subnet())})
	ip, err := w.nodeIP(node1)
	if err != nil {
		return err
	}
	type ctr struct {
		r  *result
		ns string
		hp int
		c  call
	}
	var cs []ctr
	for _, name := range []string{"x", "y"} {
		r, ns, err := w.add(node1, name, conf)
		if err != nil {
			return err
		}
		c := call{command: "ADD", id: w.id(name), netns: "/run/netns/" + ns, ifname: "eth0"}
		hp := port()
		res, err := w.invoke(c, portmapConf(w, r, hp))
		if err != nil {
			return err
		}
		if err := wantPassThrough(res, r, "byok8s-portmap ADD"); err != nil {
			return err
		}
		cs = append(cs, ctr{r, ns, hp, c})
	}
	x, y := cs[0], cs[1]
	x.c.command = "DEL"
	for i := range 2 {
		res, err := w.invoke(x.c, portmapConf(w, nil, x.hp))
		if err != nil {
			return err
		}
		if err := wantExit(res, 0); err != nil {
			return fmt.Errorf("byok8s-portmap DEL of x (no prevResult, attempt %d): %w", i+1, err)
		}
	}
	if err := w.blocked(end{node1, ""}, end{node1, x.ns}, ip, x.hp, fmt.Sprintf("x's mapping of hostPort %d was deleted", x.hp)); err != nil {
		return err
	}
	if _, err := w.dial(end{node1, ""}, end{node1, y.ns}, ip, y.hp); err != nil {
		return fmt.Errorf("deleting x's mapping must leave y's: %w", err)
	}
	return nil
}

// twoNodes ADDs one container on each worker, each node with half of the
// stage's /24, and extra config from fields(i) for node i. It returns the
// containers' results and netns names.
func (w *work) twoNodes(fields func(i int, ips [2]string) map[string]any) ([2]*result, [2]string, error) {
	var rs [2]*result
	var nss [2]string
	var ips [2]string
	for i, n := range []runner.Node{node1, node2} {
		ip, err := w.nodeIP(n)
		if err != nil {
			return rs, nss, err
		}
		ips[i] = ip
	}
	for i, n := range []runner.Node{node1, node2} {
		ipam := w.pool(fmt.Sprintf("10.217.%d.%d/25", w.n, 128*i))
		ipam["gateway"] = w.ip(128*i + 1)
		conf := w.conf(map[string]any{"bridge": w.bridge(), "ipam": ipam})
		for k, v := range fields(i, ips) {
			conf[k] = v
		}
		r, ns, err := w.add(n, fmt.Sprintf("n%d", i+1), conf)
		if err != nil {
			return rs, nss, fmt.Errorf("on %s: %w", n, err)
		}
		rs[i], nss[i] = r, ns
	}
	return rs, nss, nil
}

// crossTalk checks both containers reach each other and see each other's
// own address.
func (w *work) crossTalk(rs [2]*result, nss [2]string) error {
	a, b := end{node1, nss[0]}, end{node2, nss[1]}
	src, err := w.dial(a, b, rs[1].addr(), port())
	if err != nil {
		return err
	}
	if src != rs[0].addr() {
		return fmt.Errorf("the container on %s saw the one on %s connect from %s, not its own address %s", node2, node1, src, rs[0].addr())
	}
	_, err = w.dial(b, a, rs[0].addr(), port())
	return err
}

// stageNodeRoutes — each node routes the other's pod subnet to it, so pods
// on different nodes talk with their own addresses.
func stageNodeRoutes(w *work) error {
	rs, nss, err := w.twoNodes(func(i int, ips [2]string) map[string]any {
		o := 1 - i
		return map[string]any{"peers": []any{map[string]any{
			"subnet": fmt.Sprintf("10.217.%d.%d/25", w.n, 128*o), "via": ips[o],
		}}}
	})
	if err != nil {
		return err
	}
	ip2, err := w.nodeIP(node2)
	if err != nil {
		return err
	}
	rt, err := w.sh(node1, fmt.Sprintf("ip route show 10.217.%d.128/25", w.n))
	if err != nil {
		return err
	}
	if !strings.Contains(rt, "via "+ip2) {
		return fmt.Errorf("%s has no route to the peer's subnet 10.217.%d.128/25 via %s: %q", node1, w.n, ip2, rt)
	}
	return w.crossTalk(rs, nss)
}

// stageVXLAN — the same peers, reached through a VXLAN overlay instead of
// direct routes.
func stageVXLAN(w *work) error {
	vx := fmt.Sprintf("%svx%d", prefix, w.n)
	vni := 4200 + w.n
	vtep := func(i int) string { return fmt.Sprintf("10.217.%d.%d", 200+w.n, i+1) }
	rs, nss, err := w.twoNodes(func(i int, ips [2]string) map[string]any {
		o := 1 - i
		return map[string]any{
			"backend": map[string]any{"type": "vxlan", "vni": vni, "port": 4799, "device": vx, "address": vtep(i) + "/24"},
			"peers": []any{map[string]any{
				"subnet": fmt.Sprintf("10.217.%d.%d/25", w.n, 128*o), "via": ips[o], "vtep": vtep(o),
			}},
		}
	})
	if err != nil {
		return err
	}
	ls, err := w.links(node1, "")
	if err != nil {
		return err
	}
	l, ok := ls[vx]
	if !ok || l.Info.Kind != "vxlan" || l.Info.Data["id"] != float64(vni) || !l.up() {
		return fmt.Errorf("expected an up vxlan device %s with VNI %d on %s, got %+v", vx, vni, node1, l)
	}
	via, err := w.sh(node1, "ip -o route get "+rs[1].addr())
	if err != nil {
		return err
	}
	if !strings.Contains(via, "dev "+vx) {
		return fmt.Errorf("%s routes %s out of %q; it must go through %s", node1, rs[1].addr(), strings.TrimSpace(via), vx)
	}
	if err := w.crossTalk(rs, nss); err != nil {
		return err
	}
	if ls, err = w.links(node1, ""); err != nil {
		return err
	}
	if ls[vx].Stats.TX.Packets == 0 {
		return fmt.Errorf("the containers talked, but nothing was sent through %s", vx)
	}
	return nil
}

// stageNetpolLite — with "policy": "namespace", a container accepts traffic
// from containers in its own Kubernetes namespace (from CNI_ARGS) and from
// the node, and nothing else.
func stageNetpolLite(w *work) error {
	conf := w.conf(map[string]any{"bridge": w.bridge(), "policy": "namespace", "ipam": w.pool(fmt.Sprintf("10.217.%d.0/29", w.n))})
	type ctr struct {
		r  *result
		ns string
	}
	cs := map[string]ctr{}
	add := func(name, kns string) error {
		ns, path, err := w.netns(node1, name)
		if err != nil {
			return err
		}
		args := fmt.Sprintf("IgnoreUnknown=1;K8S_POD_NAMESPACE=%s;K8S_POD_NAME=%s", kns, name)
		r, err := w.ok(call{command: "ADD", id: w.id(name), netns: path, ifname: "eth0", args: args}, conf, "ADD of "+name)
		cs[name] = ctr{r, ns}
		return err
	}
	for _, p := range [][2]string{{"a", "blue"}, {"b", "blue"}, {"c", "red"}, {"x", "red"}, {"y", "red"}} {
		if err := add(p[0], p[1]); err != nil {
			return err
		}
	}
	at := func(n string) end { return end{node1, cs[n].ns} }
	ip := func(n string) string { return cs[n].r.addr() }
	if _, err := w.dial(at("a"), at("b"), ip("b"), port()); err != nil {
		return fmt.Errorf("a and b are both in namespace blue: %w", err)
	}
	if err := w.blocked(at("c"), at("b"), ip("b"), port(), "c is in namespace red and b in blue"); err != nil {
		return err
	}
	if err := w.blocked(at("c"), at("a"), ip("a"), port(), "c is in namespace red and a in blue"); err != nil {
		return err
	}
	if _, err := w.dial(end{node1, ""}, at("c"), ip("c"), port()); err != nil {
		return fmt.Errorf("the node itself must still reach every container: %w", err)
	}
	if _, err := w.ok(call{command: "DEL", id: w.id("b"), netns: "/run/netns/" + cs["b"].ns, ifname: "eth0", args: "K8S_POD_NAMESPACE=blue"}, conf, "DEL of b"); err != nil {
		return err
	}
	if err := add("d", "red"); err != nil {
		return err
	}
	if ip("d") != cs["b"].r.addr() {
		return fmt.Errorf("d got %s; the only free address was b's", ip("d"))
	}
	if err := w.blocked(at("a"), at("d"), ip("d"), port(), "d is in red and took the address b had in blue; DEL must take b's address out of blue"); err != nil {
		return err
	}
	if _, err := w.dial(at("c"), at("d"), ip("d"), port()); err != nil {
		return fmt.Errorf("c and d are both in namespace red: %w", err)
	}
	return nil
}
