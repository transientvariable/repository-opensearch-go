package repository

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/transientvariable/anchor"

	json "github.com/json-iterator/go"
)

const (
	TemplateDirNameECS   = "ecs"
	TemplateDirNameIndex = "index"

	TemplateNameFormatECS = "%s_%s_%s"

	FieldDataStream      = "data_stream"
	FieldECSVersion      = "ecs_version"
	FieldMeta            = "_meta"
	FieldTemplatePath    = "path"
	FieldTemplateName    = "name"
	FieldTemplateVersion = "version"
)

// Template ...
type Template struct {
	content    []byte
	dataStream map[string]any
	ecsVersion string
	path       string
	version    int
}

// ECSVersion ...
func (t *Template) ECSVersion() string {
	return t.ecsVersion
}

// Name ...
func (t *Template) Name() string {
	name := strings.TrimSuffix(filepath.Base(t.path), filepath.Ext(t.path))
	if t.ecsVersion != "" {
		return fmt.Sprintf(
			TemplateNameFormatECS,
			TemplateDirNameECS,
			t.ecsVersion,
			name,
		)
	}
	return name
}

// Path ...
func (t *Template) Path() string {
	return t.path
}

// Version ...
func (t *Template) Version() int {
	return t.version
}

// Reader ...
func (t *Template) Reader() io.Reader {
	return bytes.NewReader(t.content)
}

// ReadTemplates ...
func ReadTemplates(path string) ([]*Template, error) {
	path = strings.TrimSpace(path)
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}

	var templates []*Template
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		p := filepath.Join(path, e.Name())
		if !e.IsDir() && filepath.Ext(p) == ".json" {
			t, err := read(p)
			if err != nil {
				return nil, err
			}

			templates = append(templates, t)
		}
	}
	return templates, nil
}

// String returns a string representation of the Template.
func (t *Template) String() string {
	tm := make(map[string]any)
	if t.ECSVersion() != "" {
		tm[FieldECSVersion] = t.ECSVersion()
	}

	if t.Name() != "" {
		tm[FieldTemplateName] = t.Name()
	}

	if t.Path() != "" {
		tm[FieldTemplatePath] = t.Path()
	}

	if t.Version() > 0 {
		tm[FieldTemplateVersion] = t.Version()
	}
	return string(anchor.ToJSONFormatted(tm))
}

func read(path string) (*Template, error) {
	f, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}

	var templateFile map[string]any
	err = json.Unmarshal(f, &templateFile)
	if err != nil {
		return nil, err
	}

	template := &Template{
		content: f,
		path:    path,
	}

	if meta, ok := templateFile[FieldMeta].(map[string]any); ok {
		if ecsVersion, ok := meta[FieldECSVersion].(string); ok {
			template.ecsVersion = strings.TrimSpace(ecsVersion)
		}
	}

	if ds, ok := templateFile[FieldDataStream].(map[string]any); ok {
		template.dataStream = ds
	}

	if version, ok := templateFile[FieldTemplateVersion].(int); ok {
		template.version = version
	}
	return template, nil
}
