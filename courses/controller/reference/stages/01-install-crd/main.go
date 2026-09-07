// Your controller.
//
// This one file grows for the whole course: every stage adds to the program
// you already have, rather than starting a new one.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// The resource this controller owns. A CustomResourceDefinition is itself an
// object, so the same dynamic client that will read Websites can create the
// definition that makes Websites exist.
const (
	group   = "byok8s.dev"
	version = "v1alpha1"
	kind    = "Website"
	plural  = "websites"
	crdName = plural + "." + group
)

var crdGVR = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, _, err := clientConfig()
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	ctx := context.Background()
	if err := installCRD(ctx, dyn); err != nil {
		return err
	}
	fmt.Printf("crd %s established\n", crdName)
	return nil
}

// clientConfig loads the kubeconfig the way every Kubernetes tool does, and
// returns the namespace its context selects — which is the namespace this
// controller works in.
func clientConfig() (*rest.Config, string, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("load kubeconfig: %w", err)
	}
	ns, _, err := cc.Namespace()
	if err != nil {
		return nil, "", fmt.Errorf("read namespace: %w", err)
	}
	return cfg, ns, nil
}

// installCRD creates the definition if it is missing and updates it if it is
// not, then waits for the API server to serve it. Creating it at startup is
// what makes the controller self-contained: nothing has to be applied by hand
// before it can run.
func installCRD(ctx context.Context, dyn dynamic.Interface) error {
	want := crdObject()
	client := dyn.Resource(crdGVR)

	existing, err := client.Get(ctx, crdName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := client.Create(ctx, want, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create crd: %w", err)
		}
	case err != nil:
		return fmt.Errorf("get crd: %w", err)
	default:
		// An update must carry the resourceVersion it is based on, or the API
		// server has no way to tell a change from a stale overwrite.
		want.SetResourceVersion(existing.GetResourceVersion())
		if _, err := client.Update(ctx, want, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update crd: %w", err)
		}
	}
	return waitEstablished(ctx, client)
}

// waitEstablished blocks until the CRD reports Established. Until then the
// API server may still answer 404 for the new kind, so a controller that
// starts watching immediately races its own definition.
func waitEstablished(ctx context.Context, client dynamic.ResourceInterface) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	return wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		got, err := client.Get(ctx, crdName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		conds, _, _ := unstructured.NestedSlice(got.Object, "status", "conditions")
		for _, c := range conds {
			cond, ok := c.(map[string]any)
			if !ok {
				continue
			}
			if cond["type"] == "Established" && cond["status"] == "True" {
				return true, nil
			}
		}
		return false, nil
	})
}

// crdObject is the definition itself. Stage 1 keeps the schema open — the API
// server only needs a structural schema to store the object, and giving the
// fields real types is the next stage.
func crdObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": crdName},
		"spec": map[string]any{
			"group": group,
			"names": map[string]any{
				"kind":     kind,
				"listKind": kind + "List",
				"plural":   plural,
				"singular": "website",
			},
			"scope": "Namespaced",
			"versions": []any{map[string]any{
				"name":    version,
				"served":  true,
				"storage": true,
				"schema": map[string]any{
					"openAPIV3Schema": map[string]any{
						"type":                                 "object",
						"x-kubernetes-preserve-unknown-fields": true,
					},
				},
			}},
		},
	}}
}
