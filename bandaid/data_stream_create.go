package bandaid

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

// Adapted from the go-elasticsearch cluster library for OpenSearch.
//
// See: https://github.com/elastic/go-elasticsearch/blob/main/esapi/api.xpack.indices.create_data_stream.go

func newIndicesCreateDataStreamFunc(t opensearchapi.Transport) IndicesCreateDataStream {
	return func(name string, o ...func(*IndicesCreateDataStreamRequest)) (*opensearchapi.Response, error) {
		var r = IndicesCreateDataStreamRequest{Name: name}
		for _, f := range o {
			f(&r)
		}
		return r.Do(r.ctx, t)
	}
}

// ----- Cluster Definition -------------------------------------------------------

// IndicesCreateDataStream - Creates a data stream
type IndicesCreateDataStream func(name string, o ...func(*IndicesCreateDataStreamRequest)) (*opensearchapi.Response, error)

// IndicesCreateDataStreamRequest configures the Indices create Data Stream Cluster request.
type IndicesCreateDataStreamRequest struct {
	Name string

	Pretty     bool
	Human      bool
	ErrorTrace bool
	FilterPath []string

	Header http.Header

	ctx context.Context
}

// Do executes the request and returns response or error.
func (r IndicesCreateDataStreamRequest) Do(ctx context.Context, transport opensearchapi.Transport) (*opensearchapi.Response, error) {
	var (
		method string
		path   strings.Builder
		params map[string]string
	)

	method = "PUT"

	path.Grow(7 + 1 + len("_data_stream") + 1 + len(r.Name))
	path.WriteString("/")
	path.WriteString("_data_stream")
	path.WriteString("/")
	path.WriteString(r.Name)

	params = make(map[string]string)

	if r.Pretty {
		params["pretty"] = "true"
	}

	if r.Human {
		params["human"] = "true"
	}

	if r.ErrorTrace {
		params["error_trace"] = "true"
	}

	if len(r.FilterPath) > 0 {
		params["filter_path"] = strings.Join(r.FilterPath, ",")
	}

	req, err := newRequest(method, path.String(), nil)
	if err != nil {
		return nil, err
	}

	if len(params) > 0 {
		q := req.URL.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	if len(r.Header) > 0 {
		if len(req.Header) == 0 {
			req.Header = r.Header
		} else {
			for k, vv := range r.Header {
				for _, v := range vv {
					req.Header.Add(k, v)
				}
			}
		}
	}

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	res, err := transport.Perform(req)
	if err != nil {
		return nil, err
	}

	response := opensearchapi.Response{
		StatusCode: res.StatusCode,
		Body:       res.Body,
		Header:     res.Header,
	}

	return &response, nil
}

// newRequest creates an HTTP request.
func newRequest(method, path string, body io.Reader) (*http.Request, error) {
	return http.NewRequest(method, path, body)
}

// WithContext sets the request context.
func (f IndicesCreateDataStream) WithContext(v context.Context) func(*IndicesCreateDataStreamRequest) {
	return func(r *IndicesCreateDataStreamRequest) {
		r.ctx = v
	}
}

// WithPretty makes the response body pretty-printed.
func (f IndicesCreateDataStream) WithPretty() func(*IndicesCreateDataStreamRequest) {
	return func(r *IndicesCreateDataStreamRequest) {
		r.Pretty = true
	}
}

// WithHuman makes statistical values human-readable.
func (f IndicesCreateDataStream) WithHuman() func(*IndicesCreateDataStreamRequest) {
	return func(r *IndicesCreateDataStreamRequest) {
		r.Human = true
	}
}

// WithErrorTrace includes the stack trace for errors in the response body.
func (f IndicesCreateDataStream) WithErrorTrace() func(*IndicesCreateDataStreamRequest) {
	return func(r *IndicesCreateDataStreamRequest) {
		r.ErrorTrace = true
	}
}

// WithFilterPath filters the properties of the response body.
func (f IndicesCreateDataStream) WithFilterPath(v ...string) func(*IndicesCreateDataStreamRequest) {
	return func(r *IndicesCreateDataStreamRequest) {
		r.FilterPath = v
	}
}

// WithHeader adds the headers to the HTTP request.
func (f IndicesCreateDataStream) WithHeader(h map[string]string) func(*IndicesCreateDataStreamRequest) {
	return func(r *IndicesCreateDataStreamRequest) {
		if r.Header == nil {
			r.Header = make(http.Header)
		}
		for k, v := range h {
			r.Header.Add(k, v)
		}
	}
}

// WithOpaqueID adds the X-Opaque-Id header to the HTTP request.
func (f IndicesCreateDataStream) WithOpaqueID(s string) func(*IndicesCreateDataStreamRequest) {
	return func(r *IndicesCreateDataStreamRequest) {
		if r.Header == nil {
			r.Header = make(http.Header)
		}
		r.Header.Set("X-Opaque-Id", s)
	}
}
