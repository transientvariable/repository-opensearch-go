package repository

import "fmt"

// opensearchError defines the type for errors that may be returned by OpenSearch repository operations.
type opensearchError string

// Error returns the cause of the OpenSearch repository error.
func (e opensearchError) Error() string {
	return string(e)
}

// Enumeration of errors that may be returned by OpenSearch repository operations.
const (
	ErrMalformedIndex           = opensearchError("index is missing or malformed")
	ErrMalformedDocumentContent = opensearchError("document content is missing or malformed")
	ErrClosed  = opensearchError("repository already closed")
	ErrInvalid = opensearchError("invalid argument")
)

// QueryError defines the error type for errors returned from a document repository resulting from an invalid or
// malformed query.
type QueryError struct {
	Operation string
	Message   string
}

// Error returns the cause of the QueryError error.
func (e *QueryError) Error() string {
	return fmt.Sprintf("%s: %s", e.Operation, e.Message)
}
