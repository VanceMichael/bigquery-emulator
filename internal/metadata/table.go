package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/goccy/go-json"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

type Table struct {
	ID        string
	ProjectID string
	DatasetID string
	metadata  map[string]interface{}
	repo      *Repository
}

// Patch shallow-merges the provided top-level fields into the table metadata
// (the semantics of bigquery.tables.patch) and persists the result. A patch
// entry whose value is nil clears that field, which is how a PATCH request
// carrying an explicit JSON null (e.g. {"expirationTime": null}) removes a
// previously set value.
func (t *Table) Patch(ctx context.Context, tx *sql.Tx, patch map[string]interface{}) error {
	if t.metadata == nil {
		t.metadata = map[string]interface{}{}
	}
	for k, v := range patch {
		if v == nil {
			delete(t.metadata, k)
			continue
		}
		t.metadata[k] = v
	}
	return t.repo.UpdateTable(ctx, tx, t)
}

// Replace overwrites the table metadata with the provided resource (the
// semantics of bigquery.tables.update), preserving the immutable identity
// fields when the caller omits them.
func (t *Table) Replace(ctx context.Context, tx *sql.Tx, metadata map[string]interface{}) error {
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	for _, key := range []string{"id", "kind", "type", "tableReference", "creationTime", "selfLink"} {
		if _, ok := metadata[key]; ok {
			continue
		}
		if v, exists := t.metadata[key]; exists {
			metadata[key] = v
		}
	}
	t.metadata = metadata
	return t.repo.UpdateTable(ctx, tx, t)
}

func (t *Table) Insert(ctx context.Context, tx *sql.Tx) error {
	return t.repo.AddTable(ctx, tx, t)
}

func (t *Table) Delete(ctx context.Context, tx *sql.Tx) error {
	return t.repo.DeleteTable(ctx, tx, t)
}

// IsView reports whether the table metadata describes a (logical or
// materialized) view rather than an ordinary table.
func (t *Table) IsView() bool {
	typ, _ := t.metadata["type"].(string)
	return typ == "VIEW" || typ == "MATERIALIZED_VIEW"
}

// ExpirationTime returns the table's expiration time in milliseconds since
// the Unix epoch, and whether an expiration is configured. The value is read
// from the persisted table metadata, where expirationTime is normally stored
// as a JSON string (the bigqueryv2.Table field carries the ",string" JSON
// option) but may also be stored as a number by sources that build metadata
// directly; both shapes are accepted.
func (t *Table) ExpirationTime() (int64, bool) {
	if t.metadata == nil {
		return 0, false
	}
	return metadataExpirationTime(t.metadata["expirationTime"])
}

// IsExpired reports whether the table has an expiration time at or before
// nowMs (milliseconds since the Unix epoch). Tables without an expiration
// time never expire.
func (t *Table) IsExpired(nowMs int64) bool {
	expiration, ok := t.ExpirationTime()
	return ok && expiration <= nowMs
}

// metadataExpirationTime decodes an expirationTime value in any of the shapes
// it can take in persisted table metadata (JSON string, float64, int64,
// json.Number).
func metadataExpirationTime(v interface{}) (int64, bool) {
	switch value := v.(type) {
	case nil:
		return 0, false
	case string:
		if value == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case float64:
		return int64(value), true
	case int64:
		return value, true
	case int:
		return int64(value), true
	case json.Number:
		n, err := value.Int64()
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func (t *Table) Content() (*bigqueryv2.Table, error) {
	encoded, err := json.Marshal(t.metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to encode metadata: %w", err)
	}
	var v bigqueryv2.Table
	if err := json.Unmarshal(encoded, &v); err != nil {
		return nil, fmt.Errorf("failed to decode metadata to table: %w", err)
	}
	return &v, nil
}

func NewTable(repo *Repository, projectID, datasetID, tableID string, metadata map[string]interface{}) *Table {
	return &Table{
		ID:        tableID,
		ProjectID: projectID,
		DatasetID: datasetID,
		metadata:  metadata,
		repo:      repo,
	}
}
