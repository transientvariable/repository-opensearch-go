package repository

import (
	"fmt"
	"strings"

	"github.com/transientvariable/anchor"
)

const (
	// MaxResultSize defines the maximum number of documents that can be returned in a single search result.
	MaxResultSize = 100_000

	BoolPredicateFilter  = "filter"
	BoolPredicateMust    = "must"
	BoolPredicateMustNot = "must_not"
	BoolPredicateShould  = "should"
)

var (
	boolPredicates = []string{
		BoolPredicateFilter,
		BoolPredicateMust,
		BoolPredicateMustNot,
		BoolPredicateShould,
	}
)

type scriptedMetricAgg struct {
	InitScript    string `json:"init_script,omitempty"`
	MapScript     string `json:"map_script"`
	CombineScript string `json:"combine_script"`
	ReduceScript  string `json:"reduce_script"`
}

// BoolQuery ...
type BoolQuery struct {
	Predicate string
	QueryType string
	Value     any
}

// SearchOption is a container for options used for configuring a search query.
type SearchOption struct {
	docvalueFields    bool
	excludeFields     []string
	includeFields     []string
	matches           []BoolQuery
	matchAll          bool
	queryString       string
	queryStringFields []string
	searchAfter       []any
	size              int
	sort              []map[string]any
	sourceEnabled     bool
	sumField          string
	sumKey            string
	terms             []BoolQuery
}

// Copy creates a deep copy of the SearchOption.
func (o *SearchOption) Copy() *SearchOption {
	options := &SearchOption{
		docvalueFields: o.docvalueFields,
		matchAll:       o.matchAll,
		queryString:    o.queryString,
		size:           o.size,
		sort:           o.sort,
		sourceEnabled:  o.sourceEnabled,
		sumField:       o.sumField,
		sumKey:         o.sumKey,
	}

	excludeFields := copyStrs(o.excludeFields)
	if len(excludeFields) > 0 {
		options.excludeFields = excludeFields
	}

	includeFields := copyStrs(o.includeFields)
	if len(includeFields) > 0 {
		options.includeFields = includeFields
	}

	var matches []BoolQuery
	for _, m := range o.matches {
		matches = append(matches, BoolQuery{
			Value:     m.Value,
			Predicate: m.Predicate,
			QueryType: m.QueryType,
		})
	}
	options.matches = matches

	queryStringFields := copyStrs(o.queryStringFields)
	if len(queryStringFields) > 0 {
		options.queryStringFields = queryStringFields
	}

	searchAfter := copyAny(o.searchAfter)
	if len(searchAfter) > 0 {
		options.searchAfter = searchAfter
	}

	var terms []BoolQuery
	for _, t := range o.terms {
		terms = append(terms, BoolQuery{
			Value:     t.Value,
			Predicate: t.Predicate,
			QueryType: t.QueryType,
		})
	}
	options.terms = terms
	return options
}

// LimitResult returns whether to limit the number of documents matching a search query.
func (o *SearchOption) LimitResult() bool {
	return o.size > 0
}

// SearchAfter returns the values used for demarcating pagination results.
func (o *SearchOption) SearchAfter() []any {
	if len(o.searchAfter) > 0 {
		searchAfter := make([]any, len(o.searchAfter))
		copy(searchAfter, o.searchAfter)
		return searchAfter
	}
	return nil
}

// Sum returns whether to sum a specific field for matching documents.
func (o *SearchOption) Sum() bool {
	return o.sumField != "" && o.sumKey != ""
}

// SumKey returns the key for retrieving the sum from search results.
func (o *SearchOption) SumKey() string {
	return o.sumKey
}

// String returns a string representation of SearchOption.
func (o *SearchOption) String() string {
	options := make(map[string]any)
	options["exclude_fields"] = o.excludeFields
	options["include_fields"] = o.includeFields
	options["matches"] = o.matches
	options["match_all"] = o.matchAll
	options["query_string"] = o.queryString
	options["query_string_fields"] = o.queryStringFields
	options["search_after"] = o.searchAfter
	options["size"] = o.size
	options["sort"] = o.sort
	options["source_enable"] = o.sourceEnabled
	options["sum_field"] = o.sumKey
	options["sum_key"] = o.sumKey
	options["terms"] = o.terms
	return string(anchor.ToJSONFormatted(options))
}

// WithDocvalueFields sets whether to include `docvalue` fields in search results. Default is false.
func WithDocvalueFields(docvalueFields bool) func(*SearchOption) {
	return func(o *SearchOption) {
		o.docvalueFields = docvalueFields
	}
}

// WithExcludeFields adds the field(s) to SearchOption to filter what fields are excluded from results.
func WithExcludeFields(fields ...string) func(*SearchOption) {
	return func(o *SearchOption) {
		o.excludeFields = append(o.excludeFields, fields...)
	}
}

// WithIncludeFields adds the field(s) to SearchOption to filter what fields are included in results.
func WithIncludeFields(fields ...string) func(*SearchOption) {
	return func(o *SearchOption) {
		o.includeFields = append(o.includeFields, fields...)
	}
}

// WithMatch adds a field to the SearchOption for matching results.
func WithMatch(field string, value any, predicate string) func(*SearchOption) {
	return func(o *SearchOption) {
		field = strings.TrimSpace(field)
		predicate = strings.TrimSpace(predicate)

		if field != "" && predicate != "" && value != nil {
			o.matches = append(o.matches, BoolQuery{
				Predicate: predicate,
				QueryType: "match",
				Value: map[string]any{
					field: value,
				},
			})
		}
	}
}

// WithMatchAll sets the whether to apply the same scoring weight to all documents matching a query.
func WithMatchAll(matchAll bool) func(*SearchOption) {
	return func(o *SearchOption) {
		o.matchAll = matchAll
	}
}

// WithQueryString adds the query string to SearchOption for matching results.
func WithQueryString(query string) func(*SearchOption) {
	return func(o *SearchOption) {
		o.queryString = strings.TrimSpace(query)
	}
}

// WithQueryStringFields adds the query string fields to SearchOption for matching documents. SearchCriteria string
// fields are only used when SearchOption.QueryString is set. If no fields are provided, and SearchOption.QueryString
// is set, all fields will be searched.
func WithQueryStringFields(fields ...string) func(*SearchOption) {
	return func(o *SearchOption) {
		o.queryStringFields = append(o.queryStringFields, fields...)
	}
}

// WithSearchAfter sets the fields that should be used for paginating results when the number of matching documents
// exceed the maximum result size threshold of MaxResultSize. The default value is `@timestamp`.
//
// If the number of matching documents is limited (e.g. WithLimit(true)) to a result size that is <= MaxResultSize
// (e.g. WithSize(MaxResultSize + 1)), this option will be ignored.
func WithSearchAfter(searchAfter ...any) func(*SearchOption) {
	return func(o *SearchOption) {
		o.searchAfter = append(o.searchAfter, searchAfter...)
	}
}

// WithSize sets the number of results to return for a SearchOption.
func WithSize(s int) func(*SearchOption) {
	return func(o *SearchOption) {
		o.size = s
	}
}

// WithSort sets list of `<field>:<direction>` pairs.
func WithSort(pairs ...string) func(*SearchOption) {
	return func(o *SearchOption) {
		var sort []map[string]any
		for _, p := range pairs {
			fd := strings.Split(p, ":")
			if len(fd) > 1 {
				f := strings.TrimSpace(fd[0])
				d := strings.TrimSpace(fd[1])

				if f != "" && d != "" {
					if d == SortDirectionAsc || d == SortDirectionDesc {
						sort = append(sort, map[string]any{
							f: map[string]string{
								"order": d,
							},
						})
					}
				}
			}
		}
		o.sort = sort
	}
}

// WithSource sets whether results should include source documents. If set to false, fields set using the methods
// WithExcludeFields and WithIncludeFields will be ignored.
func WithSource(enabled bool) func(*SearchOption) {
	return func(o *SearchOption) {
		o.sourceEnabled = enabled
	}
}

// WithSum sets the `<field>:<key>` pair for summing the values for a specific field for matching documents, where
// `<field>` denotes the name of the field to sum, and `<key>` denotes the name to use for extracting sum values from
// search results.
func WithSum(sum string) func(*SearchOption) {
	return func(o *SearchOption) {
		fk := strings.Split(strings.TrimSpace(sum), ":")
		if len(fk) == 2 {
			o.sumField = fk[0]
			o.sumKey = fk[1]
		}
	}
}

// WithTerm adds the criteria to the SearchOption for performing term queries.
func WithTerm(field string, value any, predicate string) func(*SearchOption) {
	return func(o *SearchOption) {
		field = strings.TrimSpace(field)
		predicate = strings.TrimSpace(predicate)

		if field != "" && predicate != "" && value != nil {
			o.terms = append(o.terms, BoolQuery{
				Predicate: predicate,
				QueryType: "term",
				Value: map[string]any{
					field: value,
				},
			})
		}
	}
}

// PrepareSearch prepares the search Query for use in repository operations.
func (o *SearchOption) PrepareSearch() *Query {
	query := o.PrepareQuery()

	// size
	if o.size > 0 {
		if o.size > MaxResultSize {
			query.Size = MaxResultSize
		} else {
			query.Size = o.size
		}
	}

	// source
	if o.sourceEnabled {
		if len(o.includeFields) > 0 {
			query.Source = map[string]any{
				"includes": o.includeFields,
			}
		}

		if len(o.excludeFields) > 0 {
			if s, ok := query.Source.(map[string]any); ok {
				s["excludes"] = o.excludeFields
			} else {
				query.Source = map[string]any{
					"excludes": o.excludeFields,
				}
			}
		}
	} else {
		query.Source = false
	}

	// sort
	if o.sort != nil {
		query.Sort = o.sort

		if len(o.searchAfter) > 0 {
			query.SearchAfter = o.searchAfter
			query.Size = MaxResultSize
			query.TrackTotalHits = false
		}
	}

	// sum
	if o.Sum() {
		query.Aggs = map[string]any{
			o.sumKey: map[string]any{
				"scripted_metric": &scriptedMetricAgg{
					InitScript:    "state.sum = []",
					MapScript:     fmt.Sprintf("state.sum.add(doc['%s'].size() > 0 ? doc['%s'].value : 0L)", o.sumField, o.sumField),
					CombineScript: "long total = 0; for (s in state['sum']) { total += s } return total",
					ReduceScript:  "long total = 0; for (s in states) { total += s } return total",
				},
			},
		}
		query.Source = false
	}

	// docvalue fields
	if o.docvalueFields && len(o.includeFields) > 0 {
		query.DocvalueFields = o.includeFields
	}
	return query
}

// PrepareQuery ...
func (o *SearchOption) PrepareQuery() *Query {
	query := &Query{}

	if len(o.terms) > 0 {
		query.AddBool(o.terms...)
	}

	if len(o.matches) > 0 {
		query.AddBool(o.matches...)
	}

	if o.queryString != "" {
		params := make(map[string]any)
		params["query"] = o.queryString

		if len(o.queryStringFields) > 0 {
			params["fields"] = o.queryStringFields
		}

		query.AddBool(BoolQuery{
			Predicate: BoolPredicateFilter,
			QueryType: "query_string",
			Value:     params,
		})
	}

	if o.matchAll {
		query.AddBool(BoolQuery{
			Predicate: BoolPredicateMust,
			QueryType: "match_all",
			Value:     map[string]any{},
		})
	}
	return query
}
