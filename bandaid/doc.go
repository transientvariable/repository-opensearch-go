package bandaid

// This package exists to address the missing features and/or bugs in the opensearch-go cluster library which are not
// currently resolved in the latest release:
//
//   https://github.com/opensearch-project/opensearch-go
//
// Ultimately, this package should be removed once the cluster library reaches parity with the server.
//
// 2022-06-30
//   - Added Cluster requests for creating and retrieving data streams
//   - Added Cluster update request to fix https://github.com/opensearch-project/opensearch-go/issues/132 for version 2.0
