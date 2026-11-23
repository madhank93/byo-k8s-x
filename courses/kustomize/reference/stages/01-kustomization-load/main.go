// A kustomize: `build DIR` reads DIR's kustomization and prints the resources
// it produces as one YAML stream.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"sigs.k8s.io/yaml"
)

// Kustomization is the kustomization file. Decoding is strict, so a field
// missing here is a field kustomize users would see rejected.
type Kustomization struct {
	APIVersion string   `json:"apiVersion,omitempty"`
	Kind       string   `json:"kind,omitempty"`
	Resources  []string `json:"resources,omitempty"`
}

// obj is one resource, as decoded YAML.
type obj = map[string]any

func main() {
	if len(os.Args) != 3 || os.Args[1] != "build" {
		fmt.Fprintln(os.Stderr, "usage: app build DIR")
		os.Exit(2)
	}
	objs, err := build(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	out, err := render(objs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	fmt.Print(out)
}

func render(objs []obj) (string, error) {
	var docs []string
	for _, o := range objs {
		b, err := yaml.Marshal(o)
		if err != nil {
			return "", err
		}
		docs = append(docs, string(b))
	}
	return strings.Join(docs, "---\n"), nil
}

// kustomizationNames are the file names kustomize looks for, in its order.
var kustomizationNames = []string{"kustomization.yaml", "kustomization.yml", "Kustomization"}

func load(dir string) (*Kustomization, error) {
	var found []string
	for _, n := range kustomizationNames {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			found = append(found, n)
		}
	}
	switch len(found) {
	case 0:
		return nil, fmt.Errorf("unable to find one of %s in directory %q", strings.Join(kustomizationNames, ", "), dir)
	case 1:
	default:
		return nil, fmt.Errorf("found multiple kustomization files under %s", dir)
	}
	data, err := os.ReadFile(filepath.Join(dir, found[0]))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(data)) == "" {
		return nil, fmt.Errorf("%s is empty", found[0])
	}
	var k Kustomization
	if err := yaml.Unmarshal(data, &k, func(d *json.Decoder) *json.Decoder {
		d.DisallowUnknownFields()
		return d
	}); err != nil {
		return nil, fmt.Errorf("invalid Kustomization: %w", err)
	}
	if k.Kind != "" && k.Kind != "Kustomization" {
		return nil, errors.New("kind should be Kustomization")
	}
	if k.APIVersion != "" && k.APIVersion != "kustomize.config.k8s.io/v1beta1" {
		return nil, errors.New("apiVersion for Kustomization should be kustomize.config.k8s.io/v1beta1")
	}
	return &k, nil
}

func build(dir string) ([]obj, error) {
	k, err := load(dir)
	if err != nil {
		return nil, err
	}
	var out []obj
	for _, r := range k.Resources {
		objs, err := readResources(filepath.Join(dir, r))
		if err != nil {
			return nil, fmt.Errorf("accumulating resources from %q: %w", r, err)
		}
		out = append(out, objs...)
	}
	return out, nil
}

var docSep = regexp.MustCompile(`(?m)^---[ \t]*$`)

// readResources decodes every document of a YAML file.
func readResources(path string) ([]obj, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []obj
	for _, doc := range docSep.Split(string(data), -1) {
		var o obj
		if err := yaml.Unmarshal([]byte(doc), &o); err != nil {
			return nil, err
		}
		if o == nil {
			continue
		}
		if str(o, "kind") == "" {
			return nil, errors.New("missing kind in object")
		}
		if str(o, "metadata", "name") == "" {
			return nil, errors.New("missing metadata.name in object")
		}
		out = append(out, o)
	}
	return out, nil
}

// str reads the string at a path of map keys, or "".
func str(o obj, path ...string) string {
	var v any = o
	for _, p := range path {
		m, ok := v.(obj)
		if !ok {
			return ""
		}
		v = m[p]
	}
	s, _ := v.(string)
	return s
}
