package repository

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/transientvariable/log-go"

	"github.com/transientvariable/repository-opensearch-go/bandaid"
)

// Update performs an update for the provided document and options. This method does not perform an `upsert` if the
// document ID is missing or invalid.
func (r *Repository) Update(ctx context.Context, doc *Document, options ...func(*UpdateOption)) (*Result, error) {
	index := strings.TrimSpace(doc.Index())
	if len(index) == 0 {
		return nil, r.logQueryError(ErrMalformedIndex)
	}

	log.Trace("[opensearch] executing query", log.String("index", index), log.String("query", "update"))

	uo := &UpdateOption{}
	for _, option := range options {
		option(uo)
	}

	log.Trace(fmt.Sprintf("[opensearch] update options:\n%s", uo))

	query, err := uo.PrepareUpdate(doc)
	if err != nil {
		return nil, r.logQueryError(err)
	}

	log.Trace(fmt.Sprintf("[opensearch] updating document with query: %s", query))

	return r.execute(ctx, bandaid.UpdateRequest{
		Index:      index,
		DocumentID: doc.ID(),
		Body:       bytes.NewReader(query),
		Refresh:    "true",
	})
}
