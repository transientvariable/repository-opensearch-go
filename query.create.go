package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/transientvariable/log-go"

	"github.com/opensearch-project/opensearch-go/opensearchapi"

	json "github.com/json-iterator/go"
)

// Create adds the provided document(s) to an OpenSearch index.
func (r *Repository) Create(ctx context.Context, documents ...*Document) (*Result, error) {
	indexResult := &Result{}
	for _, doc := range documents {
		index := strings.TrimSpace(doc.Index())
		if len(index) == 0 {
			return nil, r.logQueryError(ErrMalformedIndex)
		}

		log.Trace("[opensearch] executing query",
			log.String("index", index),
			log.String("id", doc.ID()),
			log.String("query", "create"))

		result, err := r.execute(ctx, opensearchapi.IndexRequest{
			Index:      index,
			DocumentID: doc.ID(),
			Body:       doc.Reader(),
			Refresh:    "true",
		})
		if err != nil {
			return nil, err
		}

		indexResult.Total += result.Total
		indexResult.Documents = append(result.Documents)
	}
	return indexResult, nil
}

func (r *Repository) prepareIndexResult(response *opensearchapi.Response) (*Result, error) {
	type envelope struct {
		ID     string `json:"_id"`
		Result string `json:"result"`
	}

	var e envelope
	if err := json.NewDecoder(response.Body).Decode(&e); err != nil {
		return nil, r.logQueryError(fmt.Errorf("opensearch: could not decode response body: %w", err))
	}

	log.Trace("[opensearch] received query result",
		log.String("id", e.ID),
		log.String("result", e.Result))

	return &Result{
		Total: 1,
		Documents: []*Document{
			NewDocument(
				WithDocumentID(e.ID),
				WithContent([]byte(e.Result)),
			),
		},
	}, nil
}
