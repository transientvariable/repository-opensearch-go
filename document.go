package repository

import (
	"bytes"
	"io"

	"github.com/transientvariable/anchor"

	json "github.com/json-iterator/go"
)

// Document defines the container for documents maintained in a document store.
type Document struct {
	content json.RawMessage
	id      string
	index   string
	sort    []any
}

// NewDocument creates a new Document that represents data to be indexed or the result from a query.
//
// If a Document ID is not provided, it will be generated automatically when creating new documents. For update
// operations, the Document ID is required, and can be set using the options function WithDocumentID.
//
// The content and index name for a Document are required for most repository operations, and can be set using the
// options functions WithContent and WithIndex, respectively.
func NewDocument(options ...func(*Document)) *Document {
	doc := &Document{}
	for _, opt := range options {
		opt(doc)
	}
	return doc
}

// Content returns the raw Document content.
func (d *Document) Content() json.RawMessage {
	return d.content
}

// ID returns the Document ID, which can be the zero value for string.
func (d *Document) ID() string {
	return d.id
}

// Index returns the index name that the Document should be written to or was retrieved from.
func (d *Document) Index() string {
	return d.index
}

// Reader returns an io.Reader for the Document.Content().
func (d *Document) Reader() io.ReadSeeker {
	return bytes.NewReader(d.Content())
}

// Sort returns the Document sort values.
func (d *Document) Sort() []any {
	if len(d.sort) > 0 {
		sort := make([]any, len(d.sort))
		copy(sort, d.sort)
		return sort
	}
	return nil
}

// String returns a string representation of the Document.
func (d *Document) String() string {
	dm := map[string]any{
		"id":    d.ID(),
		"index": d.Index(),
	}

	if len(d.Content()) > 0 {
		var cm map[string]any
		if err := json.NewDecoder(d.Reader()).Decode(&cm); err != nil {
			dm["content"] = map[string]string{
				"error": "could not decode content",
			}
		}

		dm["content"] = cm
	} else {
		dm["content"] = make(map[string]any)
	}

	if len(d.Sort()) > 0 {
		dm["sort"] = d.Sort()
	}
	return string(anchor.ToJSONFormatted(dm))
}

// WithContent sets the Document content.
func WithContent(content json.RawMessage) func(*Document) {
	return func(document *Document) {
		document.content = content
	}
}

// WithDocumentID sets the Document ID.
func WithDocumentID(id string) func(*Document) {
	return func(document *Document) {
		document.id = id
	}
}

// WithDocumentSort sets the Document sort values.
func WithDocumentSort(sort ...any) func(*Document) {
	return func(document *Document) {
		document.sort = append(document.sort, sort...)
	}
}

// WithIndex sets the Document index name.
func WithIndex(index string) func(*Document) {
	return func(document *Document) {
		document.index = index
	}
}
