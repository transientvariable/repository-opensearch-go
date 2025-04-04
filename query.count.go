package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/transientvariable/log-go"

	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

// Count performs a search query for the provided index and options.
func (r *Repository) Count(ctx context.Context, index string, options ...func(*SearchOption)) (*Result, error) {
	index = strings.TrimSpace(index)
	if len(index) == 0 {
		return nil, r.logQueryError(ErrMalformedIndex)
	}

	log.Trace("[opensearch] executing query", log.String("index", index), log.String("query", "count"))

	so := &SearchOption{}
	for _, option := range options {
		option(so)
	}

	log.Trace(fmt.Sprintf("[opensearch] search options:\n%s", so))

	query := so.PrepareSearch()
	if !query.HasQuery() {
		return &Result{}, nil
	}

	log.Trace(fmt.Sprintf("[opensearch] retrieving count for documents matching query:\n%s", query))

	count, err := r.execute(ctx, opensearchapi.CountRequest{
		Index: []string{index},
		Body:  query.Reader(),
	})
	if err != nil {
		return nil, r.logQueryError(ErrMalformedIndex)
	}
	return count, nil
}
