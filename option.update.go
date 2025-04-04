package repository

import (
	"errors"
	"strings"

	"github.com/transientvariable/anchor"

	json "github.com/json-iterator/go"
)

// UpdateOption is a container for options used for configuring an update query.
type UpdateOption struct {
	// fields is a map of field names and their corresponding values. At least one valid field is required for update
	// queries.
	fields map[string]any
}

// Fields returns the fields for an update query.
func (o *UpdateOption) Fields() (map[string]any, error) {
	var options map[string]any
	err := json.Unmarshal(anchor.ToJSON(o.fields), &options)
	if err != nil {
		return nil, err
	}
	return options, nil
}

// String returns a string representation of UpdateOption.
func (o *UpdateOption) String() string {
	options := make(map[string]any)
	options["fields"] = o.fields
	return string(anchor.ToJSONFormatted(options))
}

// WithField adds the field and its corresponding value to UpdateOption for updating a document. If the field name is
// empty (len(field) == 0), or its corresponding value is nil, the field will not be added.
func WithField(field string, value any) func(*UpdateOption) {
	return func(o *UpdateOption) {
		field = strings.TrimSpace(field)
		if field != "" && value != nil {
			if o.fields == nil {
				o.fields = make(map[string]any)
			}
			o.fields[field] = value
		}
	}
}

// WithFields adds the provided map of fields to UpdateOption for updating a document. If the map of fields is empty
// (len(fields) == 0), no fields will be added. If a field name is empty (len(fields[key]) == 0), or its corresponding
// value is nil, the field will not be added.
func WithFields(fields map[string]any) func(*UpdateOption) {
	return func(o *UpdateOption) {
		if len(fields) > 0 {
			for f, v := range fields {
				WithField(f, v)(o)
			}
		}
	}
}

// PrepareUpdate prepares the update query for the provided document which must have a valid ID.
func (o *UpdateOption) PrepareUpdate(doc *Document) (json.RawMessage, error) {
	if doc.ID() == "" {
		return nil, errors.New("document id is required for update")
	}

	var fields map[string]any
	for k, v := range o.fields {
		f := strings.Split(k, ".")
		if len(f) < 0 {
			return nil, errors.New("encountered missing field:value pair")
		}

		m, err := expand(f, v)
		if err != nil {
			return nil, err
		}

		if fields == nil {
			fields = make(map[string]any)
		}

		for k, v := range m {
			fields[k] = v
		}
	}
	return anchor.ToJSON(map[string]any{"doc": fields}), nil
}

func expand(fields []string, value any) (map[string]any, error) {
	if len(fields) < 1 || value == nil {
		return nil, errors.New("expected at least one field:value pair")
	}

	f := strings.TrimSpace(fields[0])
	if f == "" {
		return nil, errors.New("field name is required")
	}

	if len(fields) == 1 {
		return map[string]any{f: value}, nil
	}

	n := len(fields) - (len(fields) - 1)
	r, err := expand(fields[n:], value)
	if err != nil {
		return nil, err
	}
	return map[string]any{fields[0]: r}, nil
}
