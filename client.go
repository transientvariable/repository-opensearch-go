package repository

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/transientvariable/anchor/net/http"
	"github.com/transientvariable/log-go"

	"github.com/cenkalti/backoff/v4"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"

	gohttp "net/http"
)

var (
	client     *opensearch.Client
	clientOnce sync.Once
)

// NewClient creates a new OpenSearch cluster using the provided configuration and logger.
func NewClient(options ...func(*Option)) *opensearch.Client {
	clientOnce.Do(func() {
		opts := &Option{}
		for _, opt := range options {
			opt(opts)
		}

		txp := http.DefaultTransport()
		txp.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

		clientConfig := opensearch.Config{
			Transport: txp,
			Addresses: opts.addresses,
			Username:  opts.username,
			Password:  opts.password,
		}

		if opts.retryEnable {
			var retryStatus []int
			for _, v := range opts.retryStatus {
				s, err := strconv.Atoi(v)
				if err != nil {
					log.Fatal("[opensearch] could not parse retry status", log.Err(err))
				}
				retryStatus = append(retryStatus, s)
			}

			retryBackoff := backoff.NewExponentialBackOff()
			clientConfig.RetryBackoff = func(i int) time.Duration {
				if i == 1 {
					retryBackoff.Reset()
				}
				return retryBackoff.NextBackOff()
			}
			clientConfig.MaxRetries = opts.retryMax
		} else {
			clientConfig.DisableRetry = true
		}

		c, err := opensearch.NewClient(clientConfig)
		if err != nil {
			log.Fatal("[opensearch] could not create cluster", log.Err(err))
		}
		client = c

		info, err := clusterInfo(client)
		if err != nil {
			log.Fatal("[opensearch] unable to fetch cluster info", log.Err(err))
		}

		log.Info(fmt.Sprintf("[opensearch] cluster info:\n%s", info))
	})
	return client
}

func clusterInfo(client *opensearch.Client) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := make(chan any)
	var resultsWaitGroup sync.WaitGroup
	resultsWaitGroup.Add(1)

	go func() {
		pollClusterInfo(ctx, client, results)
		resultsWaitGroup.Done()
	}()

	go func() {
		resultsWaitGroup.Wait()
		close(results)
	}()

	retries := 0
	for result := range results {
		if retries < clusterInfoRetryMax {
			if response, ok := result.(*opensearchapi.Response); ok {
				if response.StatusCode == gohttp.StatusOK {
					b, err := io.ReadAll(response.Body)
					if err != nil {
						return "", err
					}
					return string(b), nil
				}
			}
			retries++

			log.Info("[opensearch] waiting for cluster to become available", log.Int("retries", retries))
		}
	}
	return "", errors.New("opensearch: maximum number of retries exceeded for retrieving cluster info")
}

func pollClusterInfo(ctx context.Context, client *opensearch.Client, results chan<- any) {
	ticker := time.NewTicker(clusterInfoRetryInterval)
	for {
		select {
		case <-ticker.C:
			result, err := client.Info()
			if err != nil {
				results <- err
				continue
			}
			results <- result
		case <-ctx.Done():
			return
		}
	}
}
