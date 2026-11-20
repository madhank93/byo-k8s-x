// Your Helm.
//
// Stage 1: `chart DIR` loads a chart directory, validates Chart.yaml, and
// prints the chart's name and version followed by its template files.

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver/v3"
	"sigs.k8s.io/yaml"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: app <chart> [flags] ARGS")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func run(cmd string, args []string) error {
	switch cmd {
	case "chart":
		if len(args) != 1 {
			return fmt.Errorf("usage: chart DIR")
		}
		c, err := load(args[0])
		if err != nil {
			return err
		}
		fmt.Println(c.Metadata.Name, c.Metadata.Version)
		for _, t := range c.Templates {
			fmt.Println(t.Name)
		}
		return nil
	}
	return fmt.Errorf("unknown command %q", cmd)
}

// Metadata is Chart.yaml. Templates see it as .Chart, so the Go field names
// are the ones chart authors write: .Chart.Name, .Chart.AppVersion.
type Metadata struct {
	APIVersion  string `json:"apiVersion"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	AppVersion  string `json:"appVersion,omitempty"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type,omitempty"`
}

// File is one file of a chart, named by its slash path from the chart root.
type File struct {
	Name string
	Data []byte
}

type Chart struct {
	Metadata  Metadata
	Templates []File
}

func load(dir string) (*Chart, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "Chart.yaml"))
	if err != nil {
		return nil, fmt.Errorf("%s is not a chart: %w", dir, err)
	}
	c := &Chart{}
	if err := yaml.Unmarshal(raw, &c.Metadata); err != nil {
		return nil, fmt.Errorf("cannot load Chart.yaml: %w", err)
	}
	// Helm 2 charts often had no apiVersion, and Helm 3 still loads them as v1.
	if c.Metadata.APIVersion == "" {
		c.Metadata.APIVersion = "v1"
	}
	if err := validate(c.Metadata); err != nil {
		return nil, err
	}
	// WalkDir visits in lexical order, so the templates come out sorted.
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !strings.HasPrefix(rel, "templates/") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		c.Templates = append(c.Templates, File{Name: rel, Data: data})
		return nil
	})
	return c, err
}

// validate applies the checks Helm makes before it will use a chart.
func validate(m Metadata) error {
	switch {
	case m.Name == "":
		return fmt.Errorf("validation: chart.metadata.name is required")
	case m.Name != filepath.Base(m.Name):
		return fmt.Errorf("validation: chart.metadata.name %q is invalid", m.Name)
	case m.Version == "":
		return fmt.Errorf("validation: chart.metadata.version is required")
	}
	if _, err := semver.NewVersion(m.Version); err != nil {
		return fmt.Errorf("validation: chart.metadata.version %q is invalid", m.Version)
	}
	if m.Type != "" && m.Type != "application" && m.Type != "library" {
		return fmt.Errorf("validation: chart.metadata.type must be application or library")
	}
	return nil
}
