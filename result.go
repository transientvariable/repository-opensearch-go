package repository

import (
	"github.com/transientvariable/anchor"
)

// Result represents the result of a document Repository query.
type Result struct {
	Total     int            `json:"total"`
	Documents []*Document    `json:"documents,omitempty"`
	Error     error          `json:"error,omitempty"`
	Metrics   map[string]any `json:"metrics,omitempty"`
}

// String returns a string representation of the Result.
func (r *Result) String() string {
	return string(anchor.ToJSONFormatted(r))
}
