---
title: The port a pod calls by name
concepts: [targetPort, named port, endpointslice ports, multi-port service]
---

## Core concept

A Service's `targetPort` can be a name instead of a number:

```yaml
ports:
  - name: http
    port: 80
    targetPort: web
```

The name means "whatever port each pod calls `web`" — and each pod decides.
That is what lets a Service span two versions of an app during a migration,
one listening on 8080 and the next on 8081, both calling it `web`. So there is
no single number for the Service, and `TargetPort.IntValue()` on a name is
simply 0.

The EndpointSlice controller does the resolving. Every slice carries a list
of **ports**, each with the *Service* port's name and the number the pods in
that slice give it. Pods that resolve the name to different numbers go into
different slices. So the right port for an endpoint is the one on the slice it
came from, looked up by the service port's name — never the Service's own
`targetPort`. That is also true for numeric target ports, which is why
kube-proxy always reads the slice.

A Service can have several ports, and each is a separate rule: its own map
element, its own chain, its own endpoints with their own numbers. Port names
are required on a multi-port Service precisely so slices can refer to them.

The tester gives two pods the names `web` and `admin` with the numbers
swapped, and a Service with port 80 targeting `web` and port 81 targeting
`admin`. Every backend reports the port a request arrived on.

## Go APIs

- `slice.Ports[i].Name` and `.Port` are both pointers; an unnamed service port
  is the empty name.
- Keep endpoints per port name: `map[string][]endpoint`, where an endpoint is
  an address and a port.

## Hints

<details><summary>Nudge</summary>

Where in the API is the number that a pod gives the name `web`? Not on the
Service.
</details>

<details><summary>Approach</summary>

When joining slices, walk each slice's ports, and for each one add every
ready endpoint, with that port's number, under that port's name. A service
port then takes the list under its own name and `dnat`s to each endpoint's own
port.
</details>

## Further reading

- [Service: port definitions](https://kubernetes.io/docs/concepts/services-networking/service/#field-spec-ports)
- [EndpointSlice ports](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/#endpointslice-ports)
