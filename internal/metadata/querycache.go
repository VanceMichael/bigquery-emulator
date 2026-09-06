package metadata

import (
	"context"
	"database/sql"
	"fmt"
)

// Dependency kinds recorded alongside a cached query result.
const (
	DependencyTable   = "table"
	DependencyRoutine = "routine"
)

// Dependency is one table or routine whose content/schema a cached result
// was computed from. The cached entry may only be served while every
// dependency's current Version matches the snapshot taken at compute time.
type Dependency struct {
	Kind      string `json:"kind"`
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	Name      string `json:"name"`
	Version   int64  `json:"version"`
}

// QueryCacheEntry is one persisted query result: the serialized
// internaltypes.QueryResponse plus the dependency snapshot it was validated
// against.
type QueryCacheEntry struct {
	CacheKey     string
	ProjectID    string
	Response     string
	Dependencies string
	CreatedAt    int64
}

// FindQueryCacheEntry returns the cache entry for key, or nil when no entry
// exists.
func (r *Repository) FindQueryCacheEntry(ctx context.Context, tx *sql.Tx, key string) (*QueryCacheEntry, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT cacheKey, projectID, response, dependencies, createdAt FROM query_cache WHERE cacheKey = @cacheKey",
		sql.Named("cacheKey", key),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get query cache entry: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var entry QueryCacheEntry
	if err := rows.Scan(&entry.CacheKey, &entry.ProjectID, &entry.Response, &entry.Dependencies, &entry.CreatedAt); err != nil {
		return nil, err
	}
	return &entry, nil
}

// PutQueryCacheEntry stores (or replaces) a cache entry keyed by CacheKey.
func (r *Repository) PutQueryCacheEntry(ctx context.Context, tx *sql.Tx, entry *QueryCacheEntry) error {
	if _, err := tx.ExecContext(
		ctx,
		"DELETE FROM query_cache WHERE cacheKey = @cacheKey",
		sql.Named("cacheKey", entry.CacheKey),
	); err != nil {
		return fmt.Errorf("failed to delete old query cache entry: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		"INSERT query_cache (cacheKey, projectID, response, dependencies, createdAt) VALUES (@cacheKey, @projectID, @response, @dependencies, @createdAt)",
		sql.Named("cacheKey", entry.CacheKey),
		sql.Named("projectID", entry.ProjectID),
		sql.Named("response", entry.Response),
		sql.Named("dependencies", entry.Dependencies),
		sql.Named("createdAt", entry.CreatedAt),
	); err != nil {
		return fmt.Errorf("failed to insert query cache entry: %w", err)
	}
	return nil
}

// DeleteQueryCacheEntry removes a single cache entry. Used to evict entries
// found stale on lookup; failures are best-effort.
func (r *Repository) DeleteQueryCacheEntry(ctx context.Context, tx *sql.Tx, key string) error {
	if _, err := tx.ExecContext(
		ctx,
		"DELETE FROM query_cache WHERE cacheKey = @cacheKey",
		sql.Named("cacheKey", key),
	); err != nil {
		return fmt.Errorf("failed to delete query cache entry: %w", err)
	}
	return nil
}

// ClearQueryCache removes every cache entry. It is the safety valve invoked
// when a version bump fails: callers can no longer prove which entries are
// still valid, so subsequent requests recompute their results.
func (r *Repository) ClearQueryCache(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM query_cache"); err != nil {
		return fmt.Errorf("failed to clear query cache: %w", err)
	}
	return nil
}

// nextCacheSeq returns a process-wide monotonic version. Versions are
// global (rather than per-object) so that dropping and recreating a table
// under the same name can never collide with an earlier generation's
// version.
func (r *Repository) nextCacheSeq(ctx context.Context, tx *sql.Tx) (int64, error) {
	rows, err := tx.QueryContext(ctx, "SELECT v FROM cache_seq WHERE id = 1")
	if err != nil {
		return 0, fmt.Errorf("failed to get cache sequence: %w", err)
	}
	var current int64
	exists := false
	if rows.Next() {
		if err := rows.Scan(&current); err != nil {
			rows.Close()
			return 0, err
		}
		exists = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if !exists {
		if _, err := tx.ExecContext(ctx, "INSERT cache_seq (id, v) VALUES (1, 1)"); err != nil {
			return 0, fmt.Errorf("failed to initialize cache sequence: %w", err)
		}
		return 1, nil
	}
	next := current + 1
	if _, err := tx.ExecContext(
		ctx,
		"UPDATE cache_seq SET v = @v WHERE id = 1",
		sql.Named("v", next),
	); err != nil {
		return 0, fmt.Errorf("failed to update cache sequence: %w", err)
	}
	return next, nil
}

// BumpTableVersion advances the recorded version of a table or view to a
// fresh globally-unique value after any content or schema change.
func (r *Repository) BumpTableVersion(ctx context.Context, tx *sql.Tx, projectID, datasetID, tableID string) error {
	return r.bumpVersion(ctx, tx, "table_versions", "tableID", projectID, datasetID, tableID, "")
}

// EnsureTableVersion returns the current version of a table, creating the
// version row (snapshotting the current state) when it does not exist yet.
// Used to snapshot the dependencies of a result being cached, so that the
// first later mutation invalidates the entry.
func (r *Repository) EnsureTableVersion(ctx context.Context, tx *sql.Tx, projectID, datasetID, tableID string) (int64, error) {
	version, found, err := r.findVersion(ctx, tx, "table_versions", "tableID", projectID, datasetID, tableID)
	if err != nil {
		return 0, err
	}
	if found {
		return version, nil
	}
	seq, err := r.nextCacheSeq(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err := r.insertVersion(ctx, tx, "table_versions", "tableID", projectID, datasetID, tableID, seq, ""); err != nil {
		return 0, err
	}
	return seq, nil
}

// FindTableVersion returns the current version of a table and whether a
// version row exists.
func (r *Repository) FindTableVersion(ctx context.Context, tx *sql.Tx, projectID, datasetID, tableID string) (int64, bool, error) {
	return r.findVersion(ctx, tx, "table_versions", "tableID", projectID, datasetID, tableID)
}

// BumpRoutineVersion advances the recorded version of a routine (function,
// table function or procedure). A non-empty body replaces the stored SQL
// definition so dependency resolution can expand routines that reference
// tables.
func (r *Repository) BumpRoutineVersion(ctx context.Context, tx *sql.Tx, projectID, datasetID, routineID, body string) error {
	if err := r.bumpVersion(ctx, tx, "routine_versions", "routineID", projectID, datasetID, routineID, body); err != nil {
		return err
	}
	if body != "" {
		if _, err := tx.ExecContext(
			ctx,
			"UPDATE routine_versions SET body = @body WHERE projectID = @projectID AND datasetID = @datasetID AND routineID = @routineID",
			sql.Named("body", body),
			sql.Named("projectID", projectID),
			sql.Named("datasetID", datasetID),
			sql.Named("routineID", routineID),
		); err != nil {
			return fmt.Errorf("failed to update routine body: %w", err)
		}
	}
	return nil
}

// FindRoutineVersion returns the current version and stored SQL body of a
// routine and whether a version row exists.
func (r *Repository) FindRoutineVersion(ctx context.Context, tx *sql.Tx, projectID, datasetID, routineID string) (int64, string, bool, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT version, body FROM routine_versions WHERE projectID = @projectID AND datasetID = @datasetID AND routineID = @routineID",
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("routineID", routineID),
	)
	if err != nil {
		return 0, "", false, fmt.Errorf("failed to get routine version: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, "", false, nil
	}
	var version int64
	var body string
	if err := rows.Scan(&version, &body); err != nil {
		return 0, "", false, err
	}
	return version, body, true, nil
}

func (r *Repository) bumpVersion(ctx context.Context, tx *sql.Tx, table, idColumn, projectID, datasetID, objectID, body string) error {
	_, found, err := r.findVersion(ctx, tx, table, idColumn, projectID, datasetID, objectID)
	if err != nil {
		return err
	}
	seq, err := r.nextCacheSeq(ctx, tx)
	if err != nil {
		return err
	}
	if found {
		if _, err := tx.ExecContext(
			ctx,
			fmt.Sprintf("UPDATE %s SET version = @version WHERE projectID = @projectID AND datasetID = @datasetID AND %s = @objectID", table, idColumn),
			sql.Named("version", seq),
			sql.Named("projectID", projectID),
			sql.Named("datasetID", datasetID),
			sql.Named("objectID", objectID),
		); err != nil {
			return fmt.Errorf("failed to update %s version: %w", table, err)
		}
		// The body column exists only on routine_versions.
		if body != "" && table == "routine_versions" {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE routine_versions SET body = @body WHERE projectID = @projectID AND datasetID = @datasetID AND routineID = @routineID",
				sql.Named("body", body),
				sql.Named("projectID", projectID),
				sql.Named("datasetID", datasetID),
				sql.Named("routineID", objectID),
			); err != nil {
				return fmt.Errorf("failed to update routine body: %w", err)
			}
		}
		return nil
	}
	if err := r.insertVersion(ctx, tx, table, idColumn, projectID, datasetID, objectID, seq, body); err != nil {
		return err
	}
	return nil
}

func (r *Repository) findVersion(ctx context.Context, tx *sql.Tx, table, idColumn, projectID, datasetID, objectID string) (int64, bool, error) {
	rows, err := tx.QueryContext(
		ctx,
		fmt.Sprintf("SELECT version FROM %s WHERE projectID = @projectID AND datasetID = @datasetID AND %s = @objectID", table, idColumn),
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("objectID", objectID),
	)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get version from %s: %w", table, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, false, nil
	}
	var version int64
	if err := rows.Scan(&version); err != nil {
		return 0, false, err
	}
	return version, true, nil
}

func (r *Repository) insertVersion(ctx context.Context, tx *sql.Tx, table, idColumn, projectID, datasetID, objectID string, version int64, body string) error {
	if table == "routine_versions" {
		if _, err := tx.ExecContext(
			ctx,
			"INSERT routine_versions (projectID, datasetID, routineID, version, body) VALUES (@projectID, @datasetID, @routineID, @version, @body)",
			sql.Named("projectID", projectID),
			sql.Named("datasetID", datasetID),
			sql.Named("routineID", objectID),
			sql.Named("version", version),
			sql.Named("body", body),
		); err != nil {
			return fmt.Errorf("failed to insert routine version: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(
		ctx,
		fmt.Sprintf("INSERT %s (projectID, datasetID, %s, version) VALUES (@projectID, @datasetID, @objectID, @version)", table, idColumn),
		sql.Named("projectID", projectID),
		sql.Named("datasetID", datasetID),
		sql.Named("objectID", objectID),
		sql.Named("version", version),
	); err != nil {
		return fmt.Errorf("failed to insert %s version: %w", table, err)
	}
	return nil
}
