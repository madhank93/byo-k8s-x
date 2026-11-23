---
title: Join the API server as metrics.k8s.io
concepts: [aggregation layer, APIService, caBundle, ExternalName service, discovery]
---

## Core concept

The API server can serve APIs it knows nothing about. An **APIService**
object says "requests for this group and version go to that Service", and the
API server's aggregator then proxies every such request — authentication,
authorization and all already done — to whatever answers there:

```yaml
apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata:
  name: v1beta1.metrics.k8s.io        # <version>.<group>, always
spec:
  group: metrics.k8s.io
  version: v1beta1
  service: {namespace: <ns>, name: <svc>, port: <port>}
  caBundle: <base64 PEM>              # the CA that signed your serving cert
  groupPriorityMinimum: 100
  versionPriority: 100
```

Three things have to line up before the aggregator marks it **Available**:

1. **It can reach you.** It dials the Service. Your program runs outside the
   cluster, so the Service is `type: ExternalName` naming the machine you run
   on — `--external-host` — with your port.
2. **It trusts you.** It speaks TLS and checks your certificate against
   `caBundle`, using the Service's DNS name, `<svc>.<ns>.svc`. A self-signed
   certificate carrying that name is its own CA. `insecureSkipTLSVerify`
   also works, and means anyone who can answer on that address can serve
   metrics to the whole cluster.
3. **You answer discovery.** It asks `GET /apis/metrics.k8s.io/v1beta1` and
   expects an `APIResourceList`: `pods` (namespaced) and `nodes` (not), each
   with the verbs `get` and `list`.

The registration is cluster-wide, and so is the damage when it outlives you.
An APIService whose backend is gone makes discovery fail for every client:
`kubectl` prints `unable to retrieve the complete list of server APIs`, and
the namespace controller refuses to finish deleting any namespace, because it
cannot be sure it found every resource. So a server that registers itself
unregisters on the way out — on SIGTERM, before it stops answering.

## Go APIs

- `k8s.io/client-go/util/cert`: `GenerateSelfSignedCertKey(host, nil, []string{dnsName})`
  returns a certificate and key PEM; `tls.X509KeyPair` turns them into a
  `tls.Certificate`, and the certificate PEM is your `caBundle`.
- APIService has no typed client in client-go: `dynamic.NewForConfig` and
  the resource `apiregistration.k8s.io/v1, apiservices`.
- `signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)`.
- `http.Server.ServeTLS(listener, "", "")` with the certificate in `TLSConfig`.

## Hints

<details><summary>Nudge</summary>

Listen first, register second, and on SIGTERM unregister first, close
second. The order is the whole of "never registered without a server".
</details>

<details><summary>Approach</summary>

Generate the certificate for `<svc>.<ns>.svc`, start the TLS server on
`0.0.0.0:<port>`, create the ExternalName Service and then the APIService
with the certificate as `caBundle`. Serve one route: the APIResourceList.
Block on the signal context, then delete the APIService and the Service.
</details>

<details><summary>Implementation</summary>

A previous run may have left the APIService behind; on `AlreadyExists`, read
it and `Update` with its `resourceVersion` rather than failing. If `kubectl
get apiservice v1beta1.metrics.k8s.io` says `FailedDiscoveryCheck`, its
message names the step that failed: a dial error is the Service, an x509
error the certificate, a status code your handler.
</details>

## Further reading

- [The aggregation layer](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/apiserver-aggregation/)
- [Configure the aggregation layer](https://kubernetes.io/docs/tasks/extend-kubernetes/configure-aggregation-layer/)
- [APIService reference](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/api-service-v1/)
