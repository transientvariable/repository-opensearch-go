package repository

import (
	"github.com/transientvariable/anchor"
)

// BulkIndexerOptions is a container for options used for configuring the BulkIndexer.
type BulkIndexerOptions struct {
	consumer chan<- *BulkIndexerResult
	name     string
}

// String returns a string representation of BulkIndexerOptions.
func (o *BulkIndexerOptions) String() string {
	options := make(map[string]any)
	options["name"] = o.name
	return string(anchor.ToJSONFormatted(options))
}

// WithConsumer ...
func WithConsumer(consumer chan<- *BulkIndexerResult) func(*BulkIndexerOptions) {
	return func(options *BulkIndexerOptions) {
		options.consumer = consumer
	}
}

// WithName ...
func WithName(name string) func(*BulkIndexerOptions) {
	return func(options *BulkIndexerOptions) {
		options.name = name
	}
}
