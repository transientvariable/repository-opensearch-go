package repository

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/config-go"
	"github.com/transientvariable/log-go"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchutil"

	json "github.com/json-iterator/go"
)

const (
	DefaultFlushSize     = 5_242_880       // 5MiB
	DefaultFlushInterval = 5 * time.Second // 5s
	DefaultStatsInterval = 5 * time.Second // 5s
)

// BulkIndexerResult is a container for the result of a BulkIndexer operation.
type BulkIndexerResult struct {
	Document *Document
	Error    error
}

// BulkIndexer is a parallel and asynchronous indexer for OpenSearch.
type BulkIndexer struct {
	bulkIndexer opensearchutil.BulkIndexer
	client      *opensearch.Client
	consumer    chan<- *BulkIndexerResult
	statsCtx    context.Context
	statsCancel context.CancelFunc
}

// NewBulkIndexer creates a new BulkIndexer with the provided options.
func NewBulkIndexer(options ...func(*BulkIndexerOptions)) (*BulkIndexer, error) {
	opts := &BulkIndexerOptions{}
	for _, opt := range options {
		opt(opts)
	}

	client := NewClient()

	bulkConfig := opensearchutil.BulkIndexerConfig{
		Client:        client,
		FlushInterval: durationValue(BulkFlushInterval, DefaultFlushInterval),
		FlushBytes:    int(sizeValue(BulkFlushSize, DefaultFlushSize)),
		NumWorkers:    intValue(BulkWorkers, runtime.NumCPU()),
		Refresh:       "true",
		OnError: func(ctx context.Context, err error) {
			log.Error("[opensearch] bulk error", log.Err(err))
		},
	}

	log.Debug(fmt.Sprintf("[opensearch] creating bulk indexer with configuration:\n%s", configToJSON(bulkConfig)))

	osBulkIndexer, err := opensearchutil.NewBulkIndexer(bulkConfig)
	if err != nil {
		return nil, err
	}

	bulkIndexer := &BulkIndexer{
		bulkIndexer: osBulkIndexer,
		client:      client,
		consumer:    opts.consumer,
	}

	if config.BoolMustResolve(BulkStatsEnable) {
		bulkIndexer.statsCtx, bulkIndexer.statsCancel = context.WithCancel(context.Background())
		go func() {
			bulkIndexer.logStats()
		}()
	}
	return bulkIndexer, nil
}

// Add adds the provided document(s) to the BulkIndexer.
func (b *BulkIndexer) Add(ctx context.Context, action string, document *Document) error {
	index := strings.TrimSpace(document.Index())
	if len(index) == 0 {
		return ErrMalformedIndex
	}

	log.Trace("[opensearch] adding bulk index item",
		log.String("index", index),
		log.String("id", document.ID()),
		log.String("action", action))

	if err := b.bulkIndexer.Add(ctx, b.prepareBulkIndexItem(action, document)); err != nil {
		return err
	}
	return nil
}

// Close ...
func (b *BulkIndexer) Close(ctx context.Context) error {
	if b.bulkIndexer != nil {
		return b.bulkIndexer.Close(ctx)
	}
	return nil
}

// TODO: Consider using sync.Pool for creating bulk items
func (b *BulkIndexer) prepareBulkIndexItem(action string, item *Document) opensearchutil.BulkIndexerItem {
	bulkItem := opensearchutil.BulkIndexerItem{
		Index:  item.Index(),
		Action: action,
		OnSuccess: func(ctx context.Context, item opensearchutil.BulkIndexerItem, response opensearchutil.BulkIndexerResponseItem) {
			log.Trace("[opensearch] completed bulk index request",
				log.String("index", response.Index),
				log.String("id", response.DocumentID))

			if b.consumer != nil {
				b.consumer <- &BulkIndexerResult{
					Document: NewDocument(
						WithDocumentID(response.DocumentID),
						WithIndex(response.Index),
					)}
			}
		},
		OnFailure: func(ctx context.Context, item opensearchutil.BulkIndexerItem, response opensearchutil.BulkIndexerResponseItem, err error) {
			log.Error("[opensearch] could not complete bulk index request",
				log.String("index", response.Index),
				log.String("id", response.DocumentID),
				log.String("type", response.Error.Type),
				log.String("reason", response.Error.Reason))

			if b.consumer != nil {
				b.consumer <- &BulkIndexerResult{
					Document: NewDocument(
						WithDocumentID(response.DocumentID),
						WithIndex(response.Index),
					),
					Error: fmt.Errorf("%s", response.Error.Reason),
				}
			}
		},
	}

	if item.ID() != "" {
		bulkItem.DocumentID = item.ID()
	}

	if action != "delete" && len(item.Content()) > 0 {
		bulkItem.Body = item.Reader()
	}
	return bulkItem
}

func (b *BulkIndexer) logStats() {
	ticker := time.NewTicker(DefaultStatsInterval)
	for {
		select {
		case <-ticker.C:
			if b.bulkIndexer != nil {
				log.Info(fmt.Sprintf("[opensearch] bulk stats:\n%s", anchor.ToJSONFormatted(b.bulkIndexer.Stats())))
			}
		case <-b.statsCtx.Done():
			return
		}
	}
}

func configToJSON(config opensearchutil.BulkIndexerConfig) json.RawMessage {
	type bulkIndexConfig struct {
		NumWorkers    int           `json:"num_workers"`    // The number of workers. Defaults to runtime.NumCPU().
		FlushBytes    int           `json:"flush_bytes"`    // The flush threshold in bytes. Defaults to 5MB.
		FlushInterval time.Duration `json:"flush_interval"` // The flush threshold as duration. Defaults to 30sec.

		// Parameters of the Bulk Cluster.
		Index               string
		ErrorTrace          bool        `json:"error_trace"`
		FilterPath          []string    `json:"filter_path,omitempty"`
		Header              http.Header `json:"header,omitempty"`
		Human               bool
		Pipeline            string
		Pretty              bool
		Refresh             string
		Routing             string
		Source              []string `json:"source,omitempty"`
		SourceExcludes      []string `json:"source_excludes,omitempty"`
		SourceIncludes      []string `json:"source_includes,omitempty"`
		Timeout             time.Duration
		WaitForActiveShards string `json:"wait_for_active_shards"`
	}
	return anchor.ToJSONFormatted(bulkIndexConfig{
		NumWorkers:          config.NumWorkers,
		FlushBytes:          config.FlushBytes,
		FlushInterval:       config.FlushInterval,
		Index:               config.Index,
		ErrorTrace:          config.ErrorTrace,
		FilterPath:          config.FilterPath,
		Header:              config.Header,
		Human:               config.Human,
		Pipeline:            config.Pipeline,
		Pretty:              config.Pretty,
		Refresh:             config.Refresh,
		Source:              config.Source,
		SourceExcludes:      config.SourceExcludes,
		SourceIncludes:      config.SourceIncludes,
		Timeout:             config.Timeout,
		WaitForActiveShards: config.WaitForActiveShards,
	})
}

func durationValue(path string, defaultValue time.Duration) time.Duration {
	if v, err := config.Duration(path); err == nil {
		if v > 0 {
			return v
		}
	}
	return defaultValue
}

func intValue(path string, defaultValue int) int {
	if v, err := config.Int(path); err == nil {
		if v > 0 {
			return v
		}
	}
	return defaultValue
}

func sizeValue(path string, defaultValue int64) int64 {
	if v, err := config.Size(path); err == nil {
		if v > 0 {
			return v
		}
	}
	return defaultValue
}
