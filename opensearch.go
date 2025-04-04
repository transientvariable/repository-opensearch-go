package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/cadre/storage"
	"github.com/transientvariable/config-go"
	"github.com/transientvariable/log-go"
	"github.com/transientvariable/repository-opensearch-go/bandaid"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"

	json "github.com/json-iterator/go"
)

const (
	clusterInfoRetryInterval = 1 * time.Second
	clusterInfoRetryMax      = 10
)

var (
	once       sync.Once
	repository *Repository
)

// Repository provides operations for interfacing with an OpenSearch cluster.
type Repository struct {
	client *opensearch.Client
}

// New creates a new OpenSearch Repository.
func New() *Repository {
	once.Do(func() {
		client := NewClient()
		if config.BoolMustResolve(MappingCreate) {
			if err := prepareTemplates(client); err != nil {
				log.Fatal("[opensearch] could not prepare templates", log.Err(err))
			}

			if err := prepareIndices(client); err != nil {
				log.Fatal("[opensearch] could not prepare indices", log.Err(err))
			}
		}
		repository = &Repository{
			client: client,
		}
	})
	return repository
}

// Close releases any resources held by the OpenSearch Repository.
func (r *Repository) Close() error {
	return nil
}

func (r *Repository) execute(ctx context.Context, request opensearchapi.Request) (*Result, error) {
	response, err := request.Do(ctx, r.client)
	if err != nil {
		if response.Body != nil {
			err := response.Body.Close()
			if err != nil {
				return nil, r.logQueryError(err)
			}
		}
		return nil, r.logQueryError(fmt.Errorf("opensearch: error executing query request: %w", err))
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			log.Error("[opensearch]", log.Err(err))
		}
	}(response.Body)

	if response.IsError() {
		var em map[string]any
		if err = json.NewDecoder(response.Body).Decode(&em); err != nil {
			return nil, r.logQueryError(fmt.Errorf("opensearch: could not decode body for query response: %w", err))
		}

		var errMsg string
		if e, ok := em["error"].(map[string]any); ok {
			errMsg = fmt.Sprintf("[%s] %s: %s", response.Status(), e["type"], e["reason"])
		} else {
			errMsg = fmt.Sprintf("[%s] %s", response.Status(), em["error"])
		}

		if response.StatusCode == http.StatusBadRequest {
			return nil, r.logQueryError(&QueryError{
				Operation: reflect.ValueOf(request).Type().String(),
				Message:   errMsg,
			})
		}
		return nil, r.logQueryError(errors.New(errMsg))
	}

	switch request.(type) {
	case opensearchapi.CountRequest:
		return r.prepareCountResult(response)
	case opensearchapi.IndexRequest, bandaid.UpdateRequest, opensearchapi.DeleteByQueryRequest:
		return r.prepareIndexResult(response)
	case opensearchapi.SearchRequest:
		return r.prepareSearchResult(response)
	default:
		return nil, r.logQueryError(fmt.Errorf("opensearch: encountered unsupported search request type: %s", reflect.TypeOf(request).Name()))
	}
}

func (r *Repository) logQueryError(err error) error {
	if err != nil {
		log.Error("[opensearch] query execution error", log.Err(err))
	}
	return err
}

func prepareTemplates(client *opensearch.Client) error {
	templateDir := config.ValueMustResolve(MappingTemplatePath)

	ecsTemplates, err := ReadTemplates(filepath.Join(templateDir, TemplateDirNameECS))
	if err != nil {
		return err
	}

	err = applyComponentTemplates(client, ecsTemplates...)
	if err != nil {
		return err
	}

	indexTemplates, err := ReadTemplates(filepath.Join(templateDir, TemplateDirNameIndex))
	if err != nil {
		return err
	}

	err = applyIndexTemplates(client, indexTemplates...)
	if err != nil {
		return err
	}
	return nil
}

func applyComponentTemplates(client *opensearch.Client, templates ...*Template) error {
	for _, template := range templates {
		templateExistsResponse, err := client.Cluster.ExistsComponentTemplate(template.Name())
		if err != nil {
			return err
		}

		if templateExistsResponse.StatusCode != http.StatusOK {
			log.Debug("[opensearch] applying component template",
				log.String("name", template.Name()),
				log.String("path", template.Path()))

			putComponentTemplateResponse, err := client.Cluster.PutComponentTemplate(template.Name(), template.Reader())
			if err != nil {
				return err
			}

			if putComponentTemplateResponse.IsError() {
				return errors.New(putComponentTemplateResponse.String())
			}
		} else {
			log.Debug("[opensearch] component template exists, skipping creation",
				log.String("name", template.Name()),
				log.String("path", template.Path()))
		}
	}
	return nil
}

func applyIndexTemplates(client *opensearch.Client, templates ...*Template) error {
	for _, template := range templates {
		templateExistsResponse, err := client.Indices.ExistsIndexTemplate(template.Name())
		if err != nil {
			return err
		}

		if templateExistsResponse.StatusCode != http.StatusOK {
			log.Debug("[opensearch] applying index template",
				log.String("name", template.Name()),
				log.String("path", template.Path()))

			putIndexTemplateResponse, err := client.Indices.PutIndexTemplate(template.Name(), template.Reader())
			if err != nil {
				return err
			}

			if putIndexTemplateResponse.IsError() {
				return errors.New(putIndexTemplateResponse.String())
			}
		} else {
			log.Debug("[opensearch] index template exists, skipping creation",
				log.String("name", template.Name()),
				log.String("path", template.Path()))
		}
	}
	return nil
}

func prepareIndices(client *opensearch.Client) error {
	type indicesConfig struct {
		DataStreams []string `json:"data_streams"`
		Indices     []string `json:"indices"`
	}

	d, err := os.Open(config.ValueMustResolve(MappingIndicesPath))
	if err != nil {
		return err
	}

	var indices indicesConfig
	if err = json.NewDecoder(d).Decode(&indices); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, ds := range indices.DataStreams {
		getDataStreamRequest := bandaid.IndicesGetDataStreamRequest{Name: []string{ds}}
		getDataStreamResponse, err := getDataStreamRequest.Do(ctx, client)
		if err != nil {
			return err
		}

		if getDataStreamResponse.StatusCode != http.StatusOK {
			log.Debug("[opensearch] creating data stream", log.String("name", ds))

			createDataStreamRequest := bandaid.IndicesCreateDataStreamRequest{Name: ds}
			createDataStreamResponse, err := createDataStreamRequest.Do(ctx, client)
			if err != nil {
				return err
			}

			if createDataStreamResponse.IsError() {
				return errors.New(createDataStreamResponse.String())
			}
		} else {
			log.Debug("[opensearch] data stream exists, skipping creation", log.String("name", ds))
		}
	}

	for _, index := range indices.Indices {
		err = createIndex(client, index)
		if err != nil {
			return err
		}

		if strings.HasPrefix(index, storage.IndexPrefixMetadataStorage) {
			err = createIndex(client, index+storage.NamespaceFragmentUpload)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func createIndex(client *opensearch.Client, index string) error {
	existsResponse, err := client.Indices.Exists([]string{index})
	if err != nil {
		return err
	}

	if existsResponse.StatusCode != http.StatusOK {
		log.Debug("[opensearch] creating index", log.String("name", index))

		createIndexResponse, err := client.Indices.Create(index)
		if err != nil {
			return err
		}

		if createIndexResponse.IsError() {
			return errors.New(createIndexResponse.String())
		}
	} else {
		log.Debug("[opensearch] index exists, skipping creation", log.String("name", index))
	}
	return nil
}

func copyAny(src []any) []any {
	if len(src) > 0 {
		dst := make([]any, len(src))
		copy(dst, src)
		return dst
	}
	return nil
}

func copyStrs(src []string) []string {
	if len(src) > 0 {
		dst := make([]string, len(src))
		copy(dst, src)
		return dst
	}
	return nil
}

func copyMapStrAny(src map[string]any) map[string]any {
	if len(src) > 0 {
		var dst map[string]any
		err := json.Unmarshal(anchor.ToJSON(src), &dst)
		if err != nil {
			return map[string]any{"error": err.Error()}
		}
		return dst
	}
	return nil
}
