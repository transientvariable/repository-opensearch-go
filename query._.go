package repository

import (
	"bytes"
	"io"
	"strings"

	"github.com/transientvariable/anchor"

	json "github.com/json-iterator/go"
)

const (
	// SortDirectionAsc sort in ascending order.
	SortDirectionAsc = "asc"

	// SortDirectionDesc sort in descending order.
	SortDirectionDesc = "desc"
)

// Query defines the attributes for a document repository query.
type Query struct {
	Size           any              `json:"size,omitempty"`
	Source         any              `json:"_source,omitempty"`
	Query          map[string]any   `json:"query"`
	Sort           []map[string]any `json:"sort,omitempty"`
	SearchAfter    []any            `json:"search_after,omitempty"`
	TrackTotalHits any              `json:"track_total_hits,omitempty"`
	DocvalueFields []string         `json:"docvalue_fields,omitempty"`
	Aggs           map[string]any   `json:"aggs,omitempty"`
}

// AddBool adds the provided bool predicates to the Query.
func (q *Query) AddBool(queries ...BoolQuery) {
	boolQueries := make(map[string][]any)
	for _, query := range queries {
		predicate := strings.ToLower(strings.TrimSpace(query.Predicate))
		queryType := strings.ToLower(strings.TrimSpace(query.QueryType))

		if queryType != "" && query.Value != nil {
			for _, p := range boolPredicates {
				if predicate == p {
					if _, ok := boolQueries[predicate]; !ok {
						boolQueries[predicate] = []any{}
					}

					boolQueries[predicate] = append(boolQueries[predicate], map[string]any{
						queryType: query.Value,
					})
					break
				}
			}
		}
	}

	if len(boolQueries) > 0 {
		if len(q.Query) == 0 {
			q.Query = make(map[string]any)
		}

		if _, ok := q.Query["bool"].(map[string][]any); !ok {
			q.Query["bool"] = boolQueries
			return
		}

		bq := q.Query["bool"].(map[string][]any)
		for k, v := range boolQueries {
			bq[k] = append(bq[k], v...)
		}
	}
}

// HasQuery returns whether the Query contains a valid query.
func (q *Query) HasQuery() bool {
	return len(q.Query) > 0
}

// Raw returns the raw JSON for the query.
func (q *Query) Raw() json.RawMessage {
	return anchor.ToJSON(q)
}

// Reader returns an io.Reader for reading the query content.
func (q *Query) Reader() io.Reader {
	return bytes.NewReader(q.Raw())
}

// String returns a string representation of the query.
func (q *Query) String() string {
	return string(anchor.ToJSONFormatted(q))
}
