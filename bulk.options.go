package repository

import (
	"time"

	"github.com/transientvariable/anchor"
)

// BulkIndexerOptions is a container for options used for configuring the BulkIndexer.
type BulkIndexerOptions struct {
	consumer      chan<- *BulkIndexerResult
	flushInterval time.Duration
	flushSize     int
	name          string
	statsEnable   bool
	workers       int
}

// String returns a string representation of BulkIndexerOptions.
func (o *BulkIndexerOptions) String() string {
	options := make(map[string]any)
	options["name"] = o.name
	options["stats_enable"] = o.statsEnable
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

func WithStatsEnable(enable bool) func(*BulkIndexerOptions) {
	return func(o *BulkIndexerOptions) {
		o.statsEnable = enable
	}
}
