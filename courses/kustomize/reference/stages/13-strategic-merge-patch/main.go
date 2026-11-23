// A kustomize: `build DIR` reads DIR's kustomization and prints the resources
// it produces as one YAML stream.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"sigs.k8s.io/yaml"
)

// Kustomization is the kustomization file. Decoding is strict, so a field
// missing here is a field kustomize users would see rejected.
type Kustomization struct {
	APIVersion string   `json:"apiVersion,omitempty"`
	Kind       string   `json:"kind,omitempty"`
	Resources  []string `json:"resources,omitempty"`
	Namespace  string   `json:"namespace,omitempty"`
	NamePrefix string   `json:"namePrefix,omitempty"`
	NameSuffix string   `json:"nameSuffix,omitempty"`

	CommonLabels map[string]string `json:"commonLabels,omitempty"`
	Labels       []Label           `json:"labels,omitempty"`

	CommonAnnotations map[string]string `json:"commonAnnotations,omitempty"`

	Images   []Image   `json:"images,omitempty"`
	Replicas []Replica `json:"replicas,omitempty"`

	PatchesStrategicMerge []string `json:"patchesStrategicMerge,omitempty"`
	Patches               []Patch  `json:"patches,omitempty"`

	ConfigMapGenerator []Generator       `json:"configMapGenerator,omitempty"`
	SecretGenerator    []SecretGenerator `json:"secretGenerator,omitempty"`
	GeneratorOptions   *GeneratorOptions `json:"generatorOptions,omitempty"`
}

// Patch is one entry of patches: a patch in a file, or inline.
type Patch struct {
	Path  string `json:"path,omitempty"`
	Patch string `json:"patch,omitempty"`
}

// Generator is one entry of configMapGenerator.
type Generator struct {
	Name      string            `json:"name,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Literals  []string          `json:"literals,omitempty"`
	Files     []string          `json:"files,omitempty"`
	Envs      []string          `json:"envs,omitempty"`
	Options   *GeneratorOptions `json:"options,omitempty"`
}

// SecretGenerator is one entry of secretGenerator: a Generator with a type.
type SecretGenerator struct {
	Generator
	Type string `json:"type,omitempty"`
}

// GeneratorOptions apply to every generator, or to one, which wins.
type GeneratorOptions struct {
	Labels                map[string]string `json:"labels,omitempty"`
	Annotations           map[string]string `json:"annotations,omitempty"`
	DisableNameSuffixHash bool              `json:"disableNameSuffixHash,omitempty"`
	Immutable             bool              `json:"immutable,omitempty"`
}

// Replica is one entry of replicas.
type Replica struct {
	Name  string `json:"name,omitempty"`
	Count int64  `json:"count"`
}

// Image is one entry of images: which image, and what to change in it.
type Image struct {
	Name    string `json:"name,omitempty"`
	NewName string `json:"newName,omitempty"`
	NewTag  string `json:"newTag,omitempty"`
	Digest  string `json:"digest,omitempty"`
}

// Label is one entry of labels: pairs, and how far into the object they go.
type Label struct {
	Pairs            map[string]string `json:"pairs,omitempty"`
	IncludeSelectors bool              `json:"includeSelectors,omitempty"`
	IncludeTemplates bool              `json:"includeTemplates,omitempty"`
}

// obj is one resource, as decoded YAML.
type obj = map[string]any

// resource is an object and every name and namespace it has had, the
// current ones last, so a reference to an older name can still find it.
type resource struct {
	o    obj
	ids  [][2]string
	hash bool // a generated name, suffixed with a content hash at the end
}

func newResource(o obj) *resource {
	r := &resource{o: o}
	r.remember()
	return r
}

func (r *resource) remember() {
	r.ids = append(r.ids, [2]string{str(r.o, "metadata", "name"), str(r.o, "metadata", "namespace")})
}

func (r *resource) rename(name string) {
	meta(r.o)["name"] = name
	r.remember()
}

// hasName is whether the resource is, or once was, called name.
func (r *resource) hasName(name string) bool {
	for _, id := range r.ids {
		if id[0] == name {
			return true
		}
	}
	return false
}

// was is whether the resource is, or once was, name in namespace ns.
func (r *resource) was(name, ns string) bool {
	for _, id := range r.ids {
		if id == [2]string{name, ns} {
			return true
		}
	}
	return false
}

func objects(rs []*resource) []obj {
	var out []obj
	for _, r := range rs {
		out = append(out, r.o)
	}
	return out
}

func main() {
	if len(os.Args) != 3 || os.Args[1] != "build" {
		fmt.Fprintln(os.Stderr, "usage: app build DIR")
		os.Exit(2)
	}
	rs, err := build(os.Args[2], map[string]bool{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	for _, r := range rs {
		if r.hash {
			h, err := contentHash(r.o)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error:", err)
				os.Exit(1)
			}
			r.rename(str(r.o, "metadata", "name") + "-" + h)
		}
	}
	fixReferences(rs)
	objs := objects(rs)
	sortLegacy(objs)
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
	// YAMLToJSON first: yaml.Unmarshal into a struct would quietly turn a
	// number into the string a field wants, which kustomize refuses.
	js, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, err
	}
	var k Kustomization
	dec := json.NewDecoder(bytes.NewReader(js))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&k); err != nil {
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

// build produces a kustomization directory's resources. visiting holds the
// directories being built further up, so a cycle is caught, not recursed.
func build(dir string, visiting map[string]bool) ([]*resource, error) {
	abs, err := filepath.Abs(dir)
	if err == nil {
		abs, err = filepath.EvalSymlinks(abs)
	}
	if err != nil {
		return nil, err
	}
	if visiting[abs] {
		return nil, fmt.Errorf("cycle detected: %s includes itself", abs)
	}
	visiting[abs] = true
	defer delete(visiting, abs)

	k, err := load(abs)
	if err != nil {
		return nil, err
	}
	var rs []*resource
	for _, r := range k.Resources {
		more, err := accumulate(abs, r, visiting)
		if err != nil {
			return nil, fmt.Errorf("accumulating resources from %q: %w", r, err)
		}
		rs = append(rs, more...)
		if err := unique(objects(rs)); err != nil {
			return nil, fmt.Errorf("may not add resource with an already registered id: %w", err)
		}
	}
	var gens []SecretGenerator
	for _, g := range k.ConfigMapGenerator {
		gens = append(gens, SecretGenerator{Generator: g})
	}
	for _, g := range k.SecretGenerator {
		if g.Type == "" {
			g.Type = "Opaque"
		}
		gens = append(gens, g)
	}
	for _, g := range gens {
		o, err := generate(abs, g, k.GeneratorOptions)
		if err != nil {
			return nil, err
		}
		hash := !mergeOptions(k.GeneratorOptions, g.Options).DisableNameSuffixHash
		r := newResource(o)
		r.hash = hash
		rs = append(rs, r)
	}
	if err := unique(objects(rs)); err != nil {
		return nil, fmt.Errorf("may not add resource with an already registered id: %w", err)
	}
	if rs, err = patchAll(abs, k, rs); err != nil {
		return nil, err
	}
	if k.Namespace != "" {
		setNamespace(rs, k.Namespace)
		if err := unique(objects(rs)); err != nil {
			return nil, fmt.Errorf("namespace transformation produces ID conflict: %w", err)
		}
	}
	for _, r := range rs {
		if !fixedName[str(r.o, "kind")] && k.NamePrefix+k.NameSuffix != "" {
			r.rename(k.NamePrefix + str(r.o, "metadata", "name") + k.NameSuffix)
		}
	}
	out := objects(rs)
	if len(k.CommonLabels) > 0 {
		apply(out, labelSpecs, setPairs(k.CommonLabels))
	}
	for _, l := range k.Labels {
		specs := labelSpecs[:1]
		switch {
		case l.IncludeSelectors:
			specs = labelSpecs
		case l.IncludeTemplates:
			specs = nil
			for _, fs := range labelSpecs {
				if strings.HasSuffix(fs.path, "metadata/labels") {
					specs = append(specs, fs)
				}
			}
		}
		apply(out, specs, setPairs(l.Pairs))
	}
	if len(k.CommonAnnotations) > 0 {
		apply(out, annotationSpecs, setPairs(k.CommonAnnotations))
	}
	for _, img := range k.Images {
		for _, o := range out {
			setImages(o, img)
		}
	}
	for _, rep := range k.Replicas {
		found := false
		for _, r := range rs {
			if scalable[str(r.o, "kind")] && r.hasName(rep.Name) {
				spec, ok := r.o["spec"].(obj)
				if !ok {
					spec = obj{}
					r.o["spec"] = spec
				}
				spec["replicas"] = rep.Count
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("resource with name %s does not match a config with a scalable kind", rep.Name)
		}
	}
	return rs, nil
}

// scalable are the kinds replicas may set, in any API group.
var scalable = map[string]bool{"Deployment": true, "ReplicaSet": true, "StatefulSet": true, "ReplicationController": true}

// generate builds the ConfigMap, or with a type the Secret, one generator
// describes.
func generate(root string, g SecretGenerator, global *GeneratorOptions) (obj, error) {
	kind := "ConfigMap"
	if g.Type != "" {
		kind = "Secret"
	}
	pairs, err := keyValues(root, g.Generator)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", kind, g.Name, err)
	}
	data, binary := obj{}, obj{}
	for _, kv := range pairs {
		_, dup := data[kv[0]]
		if _, dupBinary := binary[kv[0]]; dup || dupBinary {
			return nil, fmt.Errorf("%s %s illegally repeats the key `%s`", kind, g.Name, kv[0])
		}
		if kind == "Secret" {
			data[kv[0]] = base64.StdEncoding.EncodeToString([]byte(kv[1]))
		} else if utf8.ValidString(kv[1]) {
			data[kv[0]] = kv[1]
		} else {
			binary[kv[0]] = base64.StdEncoding.EncodeToString([]byte(kv[1]))
		}
	}
	md := obj{"name": g.Name}
	if g.Namespace != "" {
		md["namespace"] = g.Namespace
	}
	o := obj{"apiVersion": "v1", "kind": kind, "metadata": md}
	if len(data) > 0 || kind == "Secret" {
		o["data"] = data
	}
	if kind == "Secret" {
		o["type"] = g.Type
	}
	if len(binary) > 0 {
		o["binaryData"] = binary
	}
	opts := mergeOptions(global, g.Options)
	if len(opts.Labels) > 0 {
		setPairs(opts.Labels)(md, "labels")
	}
	if len(opts.Annotations) > 0 {
		setPairs(opts.Annotations)(md, "annotations")
	}
	if opts.Immutable {
		o["immutable"] = true
	}
	return o, nil
}

// ref is a field naming another object of kind target, in referrers of a
// group and kind ("" matches any). An empty target means the kind field
// beside the name says what it names.
type ref struct {
	group, kind, path, target string
}

// podSpecAt is where each workload kind keeps its pod spec.
var podSpecAt = map[string]string{
	"Pod": "spec", "Deployment": "spec/template/spec", "ReplicaSet": "spec/template/spec",
	"DaemonSet": "spec/template/spec", "StatefulSet": "spec/template/spec", "Job": "spec/template/spec",
	"ReplicationController": "spec/template/spec", "CronJob": "spec/jobTemplate/spec/template/spec",
}

// refs is every reference kustomize keeps up to date here.
var refs = func() []ref {
	inPod := map[string]string{
		"volumes/configMap/name":                   "ConfigMap",
		"volumes/secret/secretName":                "Secret",
		"volumes/persistentVolumeClaim/claimName":  "PersistentVolumeClaim",
		"volumes/projected/sources/configMap/name": "ConfigMap",
		"volumes/projected/sources/secret/name":    "Secret",
		"imagePullSecrets/name":                    "Secret",
		"serviceAccountName":                       "ServiceAccount",
	}
	for _, c := range []string{"containers", "initContainers"} {
		inPod[c+"/env/valueFrom/configMapKeyRef/name"] = "ConfigMap"
		inPod[c+"/env/valueFrom/secretKeyRef/name"] = "Secret"
		inPod[c+"/envFrom/configMapRef/name"] = "ConfigMap"
		inPod[c+"/envFrom/secretRef/name"] = "Secret"
	}
	out := []ref{
		{"networking.k8s.io", "Ingress", "spec/rules/http/paths/backend/service/name", "Service"},
		{"networking.k8s.io", "Ingress", "spec/defaultBackend/service/name", "Service"},
		{"networking.k8s.io", "Ingress", "spec/tls/secretName", "Secret"},
		{"apps", "StatefulSet", "spec/serviceName", "Service"},
		{"rbac.authorization.k8s.io", "RoleBinding", "roleRef/name", ""},
		{"rbac.authorization.k8s.io", "ClusterRoleBinding", "roleRef/name", ""},
		{"autoscaling", "HorizontalPodAutoscaler", "spec/scaleTargetRef/name", ""},
	}
	for kind, at := range podSpecAt {
		for path, target := range inPod {
			out = append(out, ref{"", kind, at + "/" + path, target})
		}
	}
	return out
}()

// fixReferences points every reference to a name a resource used to have at
// the name it has now. Only a resource in the referrer's own namespace, or a
// cluster-scoped one, can answer.
func fixReferences(rs []*resource) {
	for _, r := range rs {
		for _, rf := range refs {
			if (rf.group != "" && rf.group != group(r.o)) || rf.kind != str(r.o, "kind") {
				continue
			}
			walk(r.o, strings.Split(rf.path, "/"), false, func(parent obj, key string) {
				name, _ := parent[key].(string)
				target := rf.target
				if target == "" {
					target, _ = parent["kind"].(string)
				}
				for _, c := range rs {
					if str(c.o, "kind") == target && c.hasName(name) &&
						(clusterScoped(c.o) || str(c.o, "metadata", "namespace") == str(r.o, "metadata", "namespace")) {
						parent[key] = str(c.o, "metadata", "name")
						return
					}
				}
			})
		}
	}
}

// contentHash is kustomize's name suffix: the SHA-256 of the object's kind,
// data and type as JSON, cut to ten characters and respelled so it cannot
// read as a word or a number. kustomize's encoding carries an empty name.
func contentHash(o obj) (string, error) {
	m := map[string]any{"kind": o["kind"], "name": "", "data": o["data"]}
	if m["data"] == nil {
		m["data"] = ""
	}
	for _, k := range []string{"binaryData", "type"} {
		if v, ok := o[k]; ok {
			m[k] = v
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	sum := fmt.Sprintf("%x", sha256.Sum256(b))
	return strings.NewReplacer("0", "g", "1", "h", "3", "k", "a", "m", "e", "t").Replace(sum[:10]), nil
}

// mergeOptions lays a generator's own options over the global ones.
func mergeOptions(global, own *GeneratorOptions) GeneratorOptions {
	var out GeneratorOptions
	for _, o := range []*GeneratorOptions{global, own} {
		if o == nil {
			continue
		}
		for k, v := range o.Labels {
			if out.Labels == nil {
				out.Labels = map[string]string{}
			}
			out.Labels[k] = v
		}
		for k, v := range o.Annotations {
			if out.Annotations == nil {
				out.Annotations = map[string]string{}
			}
			out.Annotations[k] = v
		}
		out.DisableNameSuffixHash = out.DisableNameSuffixHash || o.DisableNameSuffixHash
		out.Immutable = out.Immutable || o.Immutable
	}
	return out
}

// keyValues reads a generator's sources in kustomize's order: literals,
// then files, then env files.
func keyValues(root string, g Generator) ([][2]string, error) {
	var out [][2]string
	for _, l := range g.Literals {
		k, v, ok := strings.Cut(l, "=")
		if !ok {
			return nil, fmt.Errorf("invalid literal source %s, expected key=value", l)
		}
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		out = append(out, [2]string{k, v})
	}
	for _, f := range g.Files {
		k, path, ok := strings.Cut(f, "=")
		if !ok {
			k, path = filepath.Base(f), f
		}
		b, err := readInRoot(root, path)
		if err != nil {
			return nil, err
		}
		out = append(out, [2]string{k, string(b)})
	}
	for _, e := range g.Envs {
		b, err := readInRoot(root, e)
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(string(b), "\n") {
			line = strings.TrimLeft(line, " \t\r")
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				if v, ok = os.LookupEnv(k); !ok {
					continue
				}
			}
			out = append(out, [2]string{k, v})
		}
	}
	return out, nil
}

// readInRoot reads a file that must sit inside root.
func readInRoot(root, path string) ([]byte, error) {
	full := filepath.Join(root, path)
	if rel, err := filepath.Rel(root, full); err != nil || strings.HasPrefix(rel, "..") {
		return nil, fmt.Errorf("security; file %q is not in or below %q", full, root)
	}
	return os.ReadFile(full)
}

// setImages rewrites every container image matching img, wherever the
// containers and initContainers lists sit in the object.
func setImages(v any, img Image) {
	switch v := v.(type) {
	case []any:
		for _, item := range v {
			setImages(item, img)
		}
	case obj:
		for key, val := range v {
			if key == "containers" || key == "initContainers" {
				list, _ := val.([]any)
				for _, c := range list {
					if c, ok := c.(obj); ok {
						if ref, ok := c["image"].(string); ok {
							c["image"] = retag(ref, img)
						}
					}
				}
				continue
			}
			setImages(val, img)
		}
	}
}

// retag applies img to one image reference, name[:tag][@digest].
func retag(ref string, img Image) string {
	name, digest, _ := strings.Cut(ref, "@")
	tag := ""
	if i := strings.LastIndex(name, ":"); i > strings.LastIndex(name, "/") {
		name, tag = name[:i], name[i+1:]
	}
	if name != img.Name {
		return ref
	}
	if img.NewName != "" {
		name = img.NewName
	}
	switch {
	case img.Digest != "":
		tag, digest = img.NewTag, img.Digest
	case img.NewTag != "":
		tag, digest = img.NewTag, ""
	}
	if tag != "" {
		name += ":" + tag
	}
	if digest != "" {
		name += "@" + digest
	}
	return name
}

// fieldSpec is a field kustomize knows to change: a slash path, in objects of
// a group and kind ("" matches any), created when missing only if create.
// A path segment that meets a list applies to every item.
type fieldSpec struct {
	group, kind, path string
	create            bool
}

// apply runs set on the field each matching spec names.
func apply(objs []obj, specs []fieldSpec, set func(parent obj, key string)) {
	for _, o := range objs {
		for _, fs := range specs {
			if (fs.group == "" || fs.group == group(o)) && (fs.kind == "" || fs.kind == str(o, "kind")) {
				walk(o, strings.Split(fs.path, "/"), fs.create, set)
			}
		}
	}
}

func walk(v any, path []string, create bool, set func(parent obj, key string)) {
	switch v := v.(type) {
	case []any:
		for _, item := range v {
			walk(item, path, create, set)
		}
	case obj:
		key := strings.TrimSuffix(path[0], "[]")
		if len(path) == 1 {
			if _, ok := v[key]; ok || create {
				set(v, key)
			}
			return
		}
		next, ok := v[key]
		if !ok || next == nil {
			if !create {
				return
			}
			next = obj{}
			v[key] = next
		}
		walk(next, path[1:], create, set)
	}
}

// setPairs merges pairs into the map at parent[key], creating it.
func setPairs(pairs map[string]string) func(obj, string) {
	return func(parent obj, key string) {
		m, ok := parent[key].(obj)
		if !ok {
			m = obj{}
			parent[key] = m
		}
		for k, v := range pairs {
			m[k] = v
		}
	}
}

// selectorTerms are the optional label selectors inside a pod template.
var selectorTerms = []string{
	"spec/template/spec/affinity/podAffinity/preferredDuringSchedulingIgnoredDuringExecution/podAffinityTerm/labelSelector/matchLabels",
	"spec/template/spec/affinity/podAffinity/requiredDuringSchedulingIgnoredDuringExecution/labelSelector/matchLabels",
	"spec/template/spec/affinity/podAntiAffinity/preferredDuringSchedulingIgnoredDuringExecution/podAffinityTerm/labelSelector/matchLabels",
	"spec/template/spec/affinity/podAntiAffinity/requiredDuringSchedulingIgnoredDuringExecution/labelSelector/matchLabels",
	"spec/template/spec/topologySpreadConstraints/labelSelector/matchLabels",
}

// labelSpecs is where commonLabels goes; metadata/labels comes first.
var labelSpecs = func() []fieldSpec {
	specs := []fieldSpec{
		{"", "", "metadata/labels", true},
		{"", "ReplicationController", "spec/selector", true},
		{"", "ReplicationController", "spec/template/metadata/labels", true},
		{"", "Service", "spec/selector", true},
		{"", "Deployment", "spec/selector/matchLabels", true},
		{"", "Deployment", "spec/template/metadata/labels", true},
		{"", "ReplicaSet", "spec/selector/matchLabels", true},
		{"", "ReplicaSet", "spec/template/metadata/labels", true},
		{"", "DaemonSet", "spec/selector/matchLabels", true},
		{"", "DaemonSet", "spec/template/metadata/labels", true},
		{"apps", "StatefulSet", "spec/selector/matchLabels", true},
		{"apps", "StatefulSet", "spec/template/metadata/labels", true},
		{"apps", "StatefulSet", "spec/volumeClaimTemplates[]/metadata/labels", true},
		{"batch", "Job", "spec/selector/matchLabels", false},
		{"batch", "Job", "spec/template/metadata/labels", true},
		{"batch", "CronJob", "spec/jobTemplate/spec/selector/matchLabels", false},
		{"batch", "CronJob", "spec/jobTemplate/metadata/labels", true},
		{"batch", "CronJob", "spec/jobTemplate/spec/template/metadata/labels", true},
		{"policy", "PodDisruptionBudget", "spec/selector/matchLabels", false},
		{"networking.k8s.io", "NetworkPolicy", "spec/podSelector/matchLabels", false},
		{"networking.k8s.io", "NetworkPolicy", "spec/ingress/from/podSelector/matchLabels", false},
		{"networking.k8s.io", "NetworkPolicy", "spec/egress/to/podSelector/matchLabels", false},
	}
	for _, kind := range []string{"Deployment", "StatefulSet"} {
		for _, p := range selectorTerms {
			specs = append(specs, fieldSpec{"apps", kind, p, false})
		}
	}
	return specs
}()

// annotationSpecs is where commonAnnotations goes: metadata and pod templates.
var annotationSpecs = []fieldSpec{
	{"", "", "metadata/annotations", true},
	{"", "ReplicationController", "spec/template/metadata/annotations", true},
	{"", "Deployment", "spec/template/metadata/annotations", true},
	{"", "ReplicaSet", "spec/template/metadata/annotations", true},
	{"", "DaemonSet", "spec/template/metadata/annotations", true},
	{"apps", "StatefulSet", "spec/template/metadata/annotations", true},
	{"batch", "Job", "spec/template/metadata/annotations", true},
	{"batch", "CronJob", "spec/jobTemplate/metadata/annotations", true},
	{"batch", "CronJob", "spec/jobTemplate/spec/template/metadata/annotations", true},
}

// fixedName are the kinds whose names the API server derives from their
// content, so a prefix would break them.
var fixedName = map[string]bool{"Namespace": true, "CustomResourceDefinition": true, "APIService": true}

// unique refuses two resources with one identity.
func unique(objs []obj) error {
	seen := map[string]bool{}
	for _, o := range objs {
		if seen[id(o)] {
			return errors.New(id(o))
		}
		seen[id(o)] = true
	}
	return nil
}

// clusterKinds are the built-in kinds with no namespace, by group/kind.
var clusterKinds = map[string]bool{
	"/Namespace": true, "/Node": true, "/PersistentVolume": true, "/ComponentStatus": true,
	"rbac.authorization.k8s.io/ClusterRole": true, "rbac.authorization.k8s.io/ClusterRoleBinding": true,
	"apiextensions.k8s.io/CustomResourceDefinition": true, "apiregistration.k8s.io/APIService": true,
	"storage.k8s.io/StorageClass": true, "storage.k8s.io/CSIDriver": true, "storage.k8s.io/CSINode": true,
	"storage.k8s.io/VolumeAttachment": true, "scheduling.k8s.io/PriorityClass": true,
	"node.k8s.io/RuntimeClass": true, "networking.k8s.io/IngressClass": true,
	"admissionregistration.k8s.io/MutatingWebhookConfiguration":     true,
	"admissionregistration.k8s.io/ValidatingWebhookConfiguration":   true,
	"admissionregistration.k8s.io/ValidatingAdmissionPolicy":        true,
	"admissionregistration.k8s.io/ValidatingAdmissionPolicyBinding": true,
	"certificates.k8s.io/CertificateSigningRequest":                 true,
	"flowcontrol.apiserver.k8s.io/FlowSchema":                       true,
	"flowcontrol.apiserver.k8s.io/PriorityLevelConfiguration":       true,
}

func group(o obj) string {
	g, _, ok := strings.Cut(str(o, "apiVersion"), "/")
	if !ok {
		return ""
	}
	return g
}

func clusterScoped(o obj) bool { return clusterKinds[group(o)+"/"+str(o, "kind")] }

// meta is the object's metadata, created if it is missing.
func meta(o obj) obj {
	m, ok := o["metadata"].(obj)
	if !ok {
		m = obj{}
		o["metadata"] = m
	}
	return m
}

// setNamespace moves every namespaced resource into ns. A Namespace object is
// renamed to match, and a binding's subjects named default follow it too.
func setNamespace(rs []*resource, ns string) {
	for _, r := range rs {
		o := r.o
		if group(o) == "rbac.authorization.k8s.io" && strings.HasSuffix(str(o, "kind"), "RoleBinding") {
			subjects, _ := o["subjects"].([]any)
			for _, sub := range subjects {
				if m, ok := sub.(obj); ok && m["name"] == "default" {
					m["namespace"] = ns
				}
			}
		}
		switch {
		case group(o) == "" && str(o, "kind") == "Namespace":
			r.rename(ns)
		case !clusterScoped(o):
			meta(o)["namespace"] = ns
			r.remember()
		}
	}
}

// accumulate reads one entry of resources: a directory is another
// kustomization, and a file must sit inside the kustomization's own root.
func accumulate(root, entry string, visiting map[string]bool) ([]*resource, error) {
	path := filepath.Join(root, entry)
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return build(path, visiting)
	}
	if rel, err := filepath.Rel(root, path); err != nil || strings.HasPrefix(rel, "..") {
		return nil, fmt.Errorf("security; file %q is not in or below %q", path, root)
	}
	return readResources(path)
}

// id is what makes a resource unique: group, version, kind, namespace, name.
func id(o obj) string {
	return str(o, "apiVersion") + "|" + str(o, "kind") + "|" + str(o, "metadata", "namespace") + "|" + str(o, "metadata", "name")
}

// Kinds kustomize prints first, in this order, and last; everything else
// sits between them.
var (
	orderFirst = []string{"Namespace", "ResourceQuota", "StorageClass", "CustomResourceDefinition",
		"ServiceAccount", "PodSecurityPolicy", "Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding",
		"ConfigMap", "Secret", "Endpoints", "Service", "LimitRange", "PriorityClass", "PersistentVolume",
		"PersistentVolumeClaim", "Deployment", "StatefulSet", "CronJob", "PodDisruptionBudget"}
	orderLast = []string{"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"}
)

func kindRank(kind string) int {
	for i, k := range orderFirst {
		if k == kind {
			return i - len(orderFirst)
		}
	}
	for i, k := range orderLast {
		if k == kind {
			return i + 1
		}
	}
	return 0
}

// sortKey orders by group, version and kind, then by name and namespace,
// with kustomize's placeholders for the empty ones ('~' sorts after letters).
func sortKey(o obj) string {
	group, version, ok := strings.Cut(str(o, "apiVersion"), "/")
	if !ok {
		group, version = "", group
	}
	ns := str(o, "metadata", "namespace")
	if group == "" {
		group = "~G"
	}
	if version == "" {
		version = "~V"
	}
	if ns == "" {
		ns = "~X"
	}
	return group + "_" + version + "_" + str(o, "kind") + "|" + str(o, "metadata", "name") + "_" + ns
}

func sortLegacy(objs []obj) {
	sort.SliceStable(objs, func(i, j int) bool {
		ri, rj := kindRank(str(objs[i], "kind")), kindRank(str(objs[j], "kind"))
		if ri != rj {
			return ri < rj
		}
		return sortKey(objs[i]) < sortKey(objs[j])
	})
}

var docSep = regexp.MustCompile(`(?m)^---[ \t]*$`)

// readDocs decodes every non-empty document of a YAML stream.
func readDocs(data string) ([]obj, error) {
	var out []obj
	for _, doc := range docSep.Split(data, -1) {
		var o obj
		if err := yaml.Unmarshal([]byte(doc), &o); err != nil {
			return nil, err
		}
		if o != nil {
			out = append(out, o)
		}
	}
	return out, nil
}

// readResources decodes every document of a YAML file.
func readResources(path string) ([]*resource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	docs, err := readDocs(string(data))
	if err != nil {
		return nil, err
	}
	var objs []obj
	for _, o := range docs {
		items, ok := o["items"].([]any)
		if !ok || !strings.HasSuffix(str(o, "kind"), "List") {
			objs = append(objs, o)
			continue
		}
		for _, it := range items {
			m, ok := it.(obj)
			if !ok {
				return nil, errors.New("a List item is not an object")
			}
			objs = append(objs, m)
		}
	}
	var out []*resource
	for _, o := range objs {
		if str(o, "kind") == "" {
			return nil, errors.New("missing kind in object")
		}
		if str(o, "metadata", "name") == "" {
			return nil, errors.New("missing metadata.name in object")
		}
		out = append(out, newResource(o))
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

// patchAll applies patchesStrategicMerge, then patches, in the order listed.
func patchAll(root string, k *Kustomization, rs []*resource) ([]*resource, error) {
	var bodies []string
	for _, p := range k.PatchesStrategicMerge {
		// An entry is a file when one exists by that name, and a patch otherwise.
		if b, err := readInRoot(root, p); err == nil {
			p = string(b)
		}
		bodies = append(bodies, p)
	}
	for _, p := range k.Patches {
		switch {
		case p.Path != "" && p.Patch != "":
			return nil, errors.New("patch and path can't be set at the same time")
		case p.Path != "":
			b, err := readInRoot(root, p.Path)
			if err != nil {
				return nil, err
			}
			bodies = append(bodies, string(b))
		case p.Patch != "":
			bodies = append(bodies, p.Patch)
		default:
			return nil, errors.New("must specify one of patch and path")
		}
	}
	for _, body := range bodies {
		patches, err := readDocs(body)
		if err != nil {
			return nil, fmt.Errorf("unable to parse SM or JSON patch: %w", err)
		}
		for _, p := range patches {
			if str(p, "kind") == "" {
				return nil, errors.New("unable to parse SM or JSON patch: missing kind")
			}
			if rs, err = patchOne(rs, p); err != nil {
				return nil, err
			}
		}
	}
	return rs, nil
}

// patchOne lays a strategic merge patch over the one resource with its
// apiVersion, kind, name and namespace, now or earlier in the build.
func patchOne(rs []*resource, p obj) ([]*resource, error) {
	var hit []int
	for i, r := range rs {
		if str(r.o, "apiVersion") == str(p, "apiVersion") && str(r.o, "kind") == str(p, "kind") &&
			r.was(str(p, "metadata", "name"), str(p, "metadata", "namespace")) {
			hit = append(hit, i)
		}
	}
	if len(hit) != 1 {
		return nil, fmt.Errorf("no resource matches strategic merge patch %s %s/%s: failed to find unique target for patch",
			str(p, "kind"), str(p, "metadata", "namespace"), str(p, "metadata", "name"))
	}
	r := rs[hit[0]]
	if p["$patch"] == "delete" {
		return append(rs[:hit[0]:hit[0]], rs[hit[0]+1:]...), nil
	}
	name, ns := str(r.o, "metadata", "name"), str(r.o, "metadata", "namespace")
	kind := ""
	if builtin(r.o) {
		kind = str(r.o, "kind")
	}
	mergeMap(r.o, p, "", kind)
	meta(r.o)["name"] = name
	delete(meta(r.o), "namespace")
	if ns != "" {
		meta(r.o)["namespace"] = ns
	}
	return rs, nil
}

// builtin is whether kustomize knows the kind's schema, and so which lists
// in it merge item by item rather than being replaced.
func builtin(o obj) bool {
	g := group(o)
	return !strings.Contains(g, ".") || strings.HasSuffix(g, ".k8s.io")
}

// mergeKey is the field that identifies an item of the list at path, or ""
// when the list is replaced whole.
func mergeKey(kind, path string) string {
	if kind == "Service" && path == "spec/ports" {
		return "port"
	}
	switch path[strings.LastIndex(path, "/")+1:] {
	case "containers", "initContainers", "ephemeralContainers", "volumes", "env", "imagePullSecrets":
		return "name"
	case "volumeMounts":
		return "mountPath"
	case "volumeDevices":
		return "devicePath"
	case "ports":
		return "containerPort"
	}
	return ""
}

// mergeMap merges patch into dst the strategic way: maps key by key, null
// deletes, `$patch: replace` swaps the whole map. kind is the resource's kind
// when kustomize knows its schema, and "" when every list is replaced whole.
func mergeMap(dst, patch obj, path, kind string) {
	if patch["$patch"] == "replace" {
		for k := range dst {
			delete(dst, k)
		}
	}
	for k, v := range patch {
		if k == "$patch" {
			continue
		}
		at := strings.TrimPrefix(path+"/"+k, "/")
		switch v := v.(type) {
		case nil:
			delete(dst, k)
		case obj:
			d, ok := dst[k].(obj)
			if !ok {
				d = obj{}
				dst[k] = d
			}
			mergeMap(d, v, at, kind)
		case []any:
			old, _ := dst[k].([]any)
			dst[k] = mergeList(old, v, at, kind)
		default:
			dst[k] = v
		}
	}
}

// mergeList merges a patch list into a list. With a merge key, the patch's
// items come first, each merged into the item it names, then the items the
// patch left alone; `$patch: delete` drops an item and `$patch: replace`
// keeps only the patch's. Without one, the patch replaces the list.
func mergeList(dst, patch []any, path, kind string) []any {
	key := ""
	if kind != "" {
		key = mergeKey(kind, path)
	}
	replace := key == ""
	for _, p := range patch {
		if m, ok := p.(obj); ok && m["$patch"] == "replace" {
			replace = true
		}
	}
	var out []any
	used := map[int]bool{}
	for _, p := range patch {
		m, ok := p.(obj)
		if !ok {
			out = append(out, p)
			continue
		}
		if m["$patch"] == "replace" {
			continue
		}
		item := obj{}
		if !replace {
			for i, d := range dst {
				if d, ok := d.(obj); ok && !used[i] && reflect.DeepEqual(d[key], m[key]) {
					item, used[i] = d, true
					break
				}
			}
		}
		if m["$patch"] == "delete" {
			continue
		}
		mergeMap(item, m, path, kind)
		out = append(out, item)
	}
	if !replace {
		for i, d := range dst {
			if !used[i] {
				out = append(out, d)
			}
		}
	}
	return out
}
