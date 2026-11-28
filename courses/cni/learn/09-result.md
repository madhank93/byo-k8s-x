---
title: Report what you built
concepts: [CNI result, MTU, MAC address, DNS]
---

## Core concept

The result is the runtime's only view of what the plugin did, and the
kubelet's: the pod IP in `kubectl get pod -o wide` comes from it. It must
describe what is actually there.

- **MTU**: `mtu` in the config applies to both ends of the pair. An overlay
  network needs it smaller than the node's, or large packets vanish.
- **MAC**: report each interface's real hardware address.
- **DNS**: the plugin does not configure DNS — the runtime writes
  `resolv.conf` — but it passes the config's `dns` back in the result for the
  runtime to use.

```json
{"cniVersion": "1.0.0",
 "interfaces": [{…}, {…, "sandbox": "…"}],
 "ips": [{"address": "…/24", "gateway": "…", "interface": 1}],
 "routes": [{"dst": "0.0.0.0/0"}],
 "dns": {"nameservers": ["10.96.0.10"], "search": ["svc.cluster.local"]}}
```

## Go APIs

- `netlink.LinkAttrs{MTU: n}`: on a veth it sets both ends.
- `link.Attrs().HardwareAddr.String()`.
- `json.RawMessage` passes `dns` through untouched.

## Further reading

- [CNI spec: ADD success](https://www.cni.dev/docs/spec/#add-success)
