package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/transientvariable/log-go"

	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

// Delete removes a document from the provided OpenSearch index and options.
func (r *Repository) Delete(ctx context.Context, index string, options ...func(*SearchOption)) (*Result, error) {
	index = strings.TrimSpace(index)
	if len(index) == 0 {
		return nil, r.logQueryError(ErrMalformedIndex)
	}

	log.Trace("[opensearch] executing query", log.String("index", index), log.String("query", "delete"))

	so := &SearchOption{}
	for _, option := range options {
		option(so)
	}

	log.Trace(fmt.Sprintf("[opensearch] delete options:\n%s", so))

	query := so.PrepareQuery()
	if !query.HasQuery() {
		return &Result{}, nil
	}

	log.Trace(fmt.Sprintf("[opensearch] deleting document(s) matching query:\n%s", query))

	refresh := true
	return r.execute(ctx, opensearchapi.DeleteByQueryRequest{
		Index:   []string{index},
		Body:    query.Reader(),
		Refresh: &refresh,
	})
}
