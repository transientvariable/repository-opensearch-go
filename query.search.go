package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/log-go"

	"github.com/opensearch-project/opensearch-go/opensearchapi"

	json "github.com/json-iterator/go"
)

const (
	logSearchResultThreshold = 10
	searchAfterTimestamp     = "1970-01-01T00:00:00Z"
)

// Search performs a search query for the provided index and options.
func (r *Repository) Search(ctx context.Context, index string, options ...func(*SearchOption)) (*Result, error) {
	index = strings.TrimSpace(index)
	if len(index) == 0 {
		return nil, r.logQueryError(ErrMalformedIndex)
	}

	log.Trace("[opensearch] executing query", log.String("index", index), log.String("query", "search"))

	so := &SearchOption{}
	for _, option := range options {
		option(so)
	}

	log.Trace(fmt.Sprintf("[opensearch] search options:\n%s", so))

	query := so.PrepareSearch()

	log.Trace(fmt.Sprintf("[opensearch] search query:\n%s", query))

	if !query.HasQuery() {
		return &Result{}, nil
	}

	if !so.LimitResult() {
		var (
			documents []*Document
			sum       float64
		)
		pages := r.paginate(ctx, index, so)
		for result := range pages {
			if so.Sum() && len(result.Metrics) > 0 {
				if v, ok := result.Metrics[so.SumKey()].(float64); ok {
					sum += v
				}
			} else {
				documents = append(documents, result.Documents...)
			}
		}

		result := &Result{
			Total:     len(documents),
			Documents: documents,
		}

		if sum > 0 {
			result.Metrics = map[string]any{so.SumKey(): sum}
		}
		return result, nil
	}
	return r.execute(ctx, opensearchapi.SearchRequest{
		Index: []string{index},
		Body:  query.Reader(),
	})
}

func (r *Repository) prepareCountResult(response *opensearchapi.Response) (*Result, error) {
	type envelope struct {
		Count int
	}

	var e envelope
	if err := json.NewDecoder(response.Body).Decode(&e); err != nil {
		return nil, r.logQueryError(fmt.Errorf("opensearch: could not decode body for query response: %w", err))
	}

	log.Debug("[opensearch] retrieved result count", log.Int("count", e.Count))

	return &Result{Total: e.Count}, nil
}

func (r *Repository) prepareSearchResult(response *opensearchapi.Response) (*Result, error) {
	type envelope struct {
		Took int
		Hits struct {
			Total struct {
				Value int
			}
			Hits []struct {
				Index  string          `json:"_index"`
				ID     string          `json:"_id"`
				Source json.RawMessage `json:"_source,omitempty"`
				Fields json.RawMessage `json:"fields,omitempty"`
				Sort   []any           `json:"sort,omitempty"`
			}
		}
		Aggregations map[string]any `json:"aggregations,omitempty"`
	}

	var e envelope
	if err := json.NewDecoder(response.Body).Decode(&e); err != nil {
		return nil, r.logQueryError(fmt.Errorf("opensearch: could not decode response body: %w", err))
	}

	hitCount := len(e.Hits.Hits)
	logMsg := "[opensearch] retrieved search hits"

	if hitCount > logSearchResultThreshold {
		logMsg = fmt.Sprintf("%s:\n...\n\n%s\n", logMsg, anchor.ToJSON(e.Hits.Hits[hitCount-logSearchResultThreshold:]))
	} else if hitCount > 0 {
		logMsg = fmt.Sprintf("%s:\n%s\n", logMsg, anchor.ToJSON(e.Hits.Hits))
	}

	log.Trace(logMsg, log.Int("hits", hitCount))

	result := &Result{}
	result.Total = e.Hits.Total.Value
	if len(e.Hits.Hits) > 0 {
		for _, hit := range e.Hits.Hits {
			var content json.RawMessage
			if hit.Source != nil {
				content = hit.Source
			} else if hit.Fields != nil {
				content = hit.Fields
			}

			result.Documents = append(result.Documents, NewDocument(
				WithIndex(hit.Index),
				WithDocumentID(hit.ID),
				WithContent(content),
				WithDocumentSort(hit.Sort...),
			))
		}
	} else {
		result.Documents = make([]*Document, 0)
	}

	// metrics
	if len(e.Aggregations) > 0 {
		result.Metrics = make(map[string]any)
		for k, v := range e.Aggregations {
			if m, ok := v.(map[string]any); ok {
				result.Metrics[k] = m["value"]
			}
		}
	}
	return result, nil
}

func (r *Repository) paginate(ctx context.Context, index string, options *SearchOption) <-chan *Result {
	results := make(chan *Result)

	so := options.Copy()
	sort := []string{
		"_id:" + SortDirectionAsc,
		"@timestamp:" + SortDirectionAsc,
	}

	WithSort(sort...)(so)
	WithSearchAfter("", searchAfterTimestamp)(so)

	pageIndex := 1
	go func() {
		defer close(results)

		for {
			log.Trace("[opensearch] preparing page",
				log.Int("index", pageIndex),
				log.Any("search_after", so.SearchAfter()))

			query := so.PrepareSearch()
			if len(query.Query) == 0 {
				return
			}

			log.Trace(fmt.Sprintf("[opensearch] retrieving page for query:\n%s", query))

			result, err := r.execute(ctx, opensearchapi.SearchRequest{
				Index: []string{index},
				Body:  query.Reader(),
			})
			if err != nil {
				log.Error("[opensearch] could not retrieve page",
					log.Err(err),
					log.Int("page_index", pageIndex))
				return
			}

			count := len(result.Documents)
			if count == 0 {
				return
			}

			log.Trace("[opensearch] retrieved results for page",
				log.Int("documents", count),
				log.Int("page_index", pageIndex))

			select {
			case results <- result:
			case <-ctx.Done():
				return
			}

			so = options.Copy()
			WithSort(sort...)(so)
			WithSearchAfter(result.Documents[len(result.Documents)-1].Sort()...)(so)
			pageIndex++
		}
	}()
	return results
}
