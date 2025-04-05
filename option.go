package repository

import "strings"

type Option struct {
	addresses           []string
	username            string
	password            string
	retryEnable         bool
	retryCount          int
	retryMax            int
	retryStatus         []string
	bulkStatsEnable     bool
	mappingCreate       bool
	mappingTemplatePath string
	mappingIndicesPath  string
}

func WithAddresses(addresses string) func(*Option) {
	return func(o *Option) {
		for _, a := range strings.Split(addresses, ",") {
			o.addresses = append(o.addresses, strings.TrimSpace(a))
		}
	}
}

func WithUsername(username string) func(*Option) {
	return func(o *Option) {
		o.username = username
	}
}

func WithPassword(password string) func(*Option) {
	return func(o *Option) {
		o.password = password
	}
}

func WithRetryEnable(enable bool) func(*Option) {
	return func(o *Option) {
		o.retryEnable = enable
	}
}

func WithRetryCount(count int) func(*Option) {
	return func(o *Option) {
		o.retryCount = count
	}
}

func WithBulkStatsEnable(enable bool) func(*Option) {
	return func(o *Option) {
		o.bulkStatsEnable = enable
	}
}

func WithRetryStatus(status string) func(*Option) {
	return func(o *Option) {
		for _, a := range strings.Split(status, ",") {
			o.retryStatus = append(o.retryStatus, strings.TrimSpace(a))
		}
	}
}

func WithMappingCreate(create bool) func(*Option) {
	return func(o *Option) {
		o.mappingCreate = create
	}
}

func WithMappingTemplatePath(path string) func(*Option) {
	return func(o *Option) {
		o.mappingTemplatePath = path
	}
}

func WithIndicesPath(path string) func(*Option) {
	return func(o *Option) {
		o.mappingIndicesPath = path
	}
}
