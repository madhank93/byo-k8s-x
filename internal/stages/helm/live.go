package helm

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/madhank93/byo-k8s-x/internal/kube"
	"github.com/madhank93/byo-k8s-x/internal/runner"
)

// liveChart reads .Capabilities and lookup, the two things that differ
// between template, which never asks the cluster, and install or upgrade,
// which do. lkns is a namespace holding only what the grader put there.
func liveChart() map[string]string {
	return map[string]string{
		"Chart.yaml": "apiVersion: v2\nname: live\nversion: 0.1.0\n",
		"templates/caps.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-caps
data:
  kube: {{ .Capabilities.KubeVersion | quote }}
  plain: "{{ .Capabilities.KubeVersion }}"
  version: {{ .Capabilities.KubeVersion.Version | quote }}
  git: {{ .Capabilities.KubeVersion.GitVersion | quote }}
  major: {{ .Capabilities.KubeVersion.Major | quote }}
  minor: {{ .Capabilities.KubeVersion.Minor | quote }}
  apps: {{ .Capabilities.APIVersions.Has "apps/v1" | quote }}
  deployKind: {{ .Capabilities.APIVersions.Has "apps/v1/Deployment" | quote }}
  cmKind: {{ .Capabilities.APIVersions.Has "v1/ConfigMap" | quote }}
  crdKind: {{ .Capabilities.APIVersions.Has "apiextensions.k8s.io/v1/CustomResourceDefinition" | quote }}
  crdBeta: {{ .Capabilities.APIVersions.Has "apiextensions.k8s.io/v1beta1" | quote }}
  extra: {{ .Capabilities.APIVersions.Has "foo.example.com/v1/Bar" | quote }}
  seed: {{ dig "data" "k" "none" (lookup "v1" "ConfigMap" .Values.lkns "seed") | quote }}
  seedKind: {{ dig "kind" "none" (lookup "v1" "ConfigMap" .Values.lkns "seed") | quote }}
  missing: {{ lookup "v1" "ConfigMap" .Values.lkns "nope" | toJson | quote }}
  list: "{{ range dig "items" (list) (lookup "v1" "ConfigMap" .Values.lkns "") }}{{ .metadata.name }},{{ end }}"
  listKind: {{ dig "kind" "none" (lookup "v1" "ConfigMap" .Values.lkns "") | quote }}
  ns: {{ dig "metadata" "name" "none" (lookup "v1" "Namespace" "" .Values.lkns) | quote }}
  namespaces: {{ gt (len (dig "items" (list) (lookup "v1" "Namespace" "" ""))) 1 | quote }}
  everyNamespace: {{ gt (len (dig "items" (list) (lookup "v1" "ConfigMap" "" ""))) 2 | quote }}
  self: {{ dig "data" "version" "absent" (lookup "v1" "ConfigMap" .Release.Namespace (printf "%s-caps" .Release.Name)) | quote }}
`,
	}
}

// liveRender checks the same chart three ways against the real helm:
// template, offline, with the version and API list it was built with; then
// install and upgrade, which read the server's version, discovery's API list
// and real objects through lookup.
func liveRender(ctx context.Context, env *kube.Env, w *work) error {
	lkns, err := extraNamespace(ctx, env, "lookup")
	if err != nil {
		return err
	}
	defer env.Client.CoreV1().Namespaces().Delete(context.WithoutCancel(ctx), lkns, metav1.DeleteOptions{})
	if _, err := env.Client.CoreV1().ConfigMaps(lkns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "seed"},
		Data:       map[string]string{"k": "planted"},
	}, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("seed ConfigMap seed in %s: %w", lkns, err)
	}
	// The namespace's root CA ConfigMap arrives on its own; a list lookup
	// taken before it would differ between the two installs.
	if err := kube.WaitFor(ctx, "ConfigMap kube-root-ca.crt in "+lkns, func(ctx context.Context) (bool, error) {
		_, err := env.Client.CoreV1().ConfigMaps(lkns).Get(ctx, "kube-root-ca.crt", metav1.GetOptions{})
		return err == nil, nil
	}); err != nil {
		return err
	}

	live := w.chart("live", liveChart())
	set := "lkns=" + lkns
	for _, c := range [][]string{
		{"--set", set, "rel", live},
		{"--kube-version", "1.29.3", "--set", set, "rel", live},
		{"--api-versions", "foo.example.com/v1/Bar", "--set", set, "rel", live},
		{"--api-versions", "x.example.com/v2,foo.example.com/v1/Bar", "--set", set, "rel", live},
	} {
		if err := sameTemplate(w, append([]string{"template"}, c...)); err != nil {
			return err
		}
	}

	for _, verb := range []string{"install", "upgrade"} {
		for _, step := range []struct {
			run  func(...string) (*runner.Result, error)
			name string
			who  string
		}{
			{w.helm, "live-h", "the grader's helm"},
			{w.run, "live-p", "app"},
		} {
			res, err := step.run(verb, "--set", set, step.name, live)
			if err != nil {
				return err
			}
			if err := wantExit(res, 0); err != nil {
				return fmt.Errorf("%s %s --set %s %s DIR: %w", step.who, verb, set, step.name, err)
			}
		}
		want, err := w.helm("get", "manifest", "live-h")
		if err != nil {
			return err
		}
		got, err := w.helm("get", "manifest", "live-p")
		if err != nil {
			return err
		}
		if err := wantExact(got.Stdout, strings.ReplaceAll(want.Stdout, "live-h", "live-p"),
			"the manifest "+verb+" rendered, with .Capabilities and lookup read from the cluster, as helm "+verb+" renders it (helm get manifest live-p)"); err != nil {
			return err
		}
	}
	return nil
}
