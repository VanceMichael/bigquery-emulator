package server_test

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
)

const (
	cacheProjectID = "cachetest"
	cacheDatasetID = "d"
)

func cacheBase(base string) string {
	return base + "/projects/" + cacheProjectID
}

func cacheQuery(t *testing.T, base, sql string, useQueryCache *bool) map[string]any {
	t.Helper()
	body := fmt.Sprintf(
		`{"query":%q,"useLegacySql":false,"defaultDataset":{"projectId":%q,"datasetId":%q}}`,
		sql, cacheProjectID, cacheDatasetID,
	)
	if useQueryCache != nil {
		body = fmt.Sprintf(`{"query":%q,"useLegacySql":false,"useQueryCache":%t,"defaultDataset":{"projectId":%q,"datasetId":%q}}`,
			sql, *useQueryCache, cacheProjectID, cacheDatasetID)
	}
	code, resp := httpJSON(t, http.MethodPost, cacheBase(base)+"/queries", body, nil)
	if code != http.StatusOK {
		t.Fatalf("jobs.query %q failed: %d (%v)", sql, code, resp)
	}
	return resp
}

func cacheQueryHit(t *testing.T, base, sql string) (map[string]any, bool) {
	t.Helper()
	resp := cacheQuery(t, base, sql, nil)
	hit, _ := resp["cacheHit"].(bool)
	return resp, hit
}

func cacheInsertAll(t *testing.T, base, table, rowsJSON string) {
	t.Helper()
	target := fmt.Sprintf("%s/datasets/%s/tables/%s/insertAll", cacheBase(base), cacheDatasetID, table)
	if code, body := httpJSON(t, http.MethodPost, target, rowsJSON, nil); code != http.StatusOK {
		t.Fatalf("insertAll %s: %d (%v)", table, code, body)
	}
}

func cacheCreateTable(t *testing.T, base, table, fieldsJSON string) {
	t.Helper()
	body := fmt.Sprintf(
		`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[%s]}}`,
		cacheProjectID, cacheDatasetID, table, fieldsJSON,
	)
	target := fmt.Sprintf("%s/datasets/%s/tables", cacheBase(base), cacheDatasetID)
	if code, resp := httpJSON(t, http.MethodPost, target, body, nil); code != http.StatusOK {
		t.Fatalf("create table %s: %d (%v)", table, code, resp)
	}
}

func setupCacheServer(t *testing.T) (*server.Server, *server.TestServer, string) {
	t.Helper()
	s, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Load(server.StructSource(types.NewProject(cacheProjectID, types.NewDataset(cacheDatasetID)))); err != nil {
		t.Fatal(err)
	}
	ts := s.TestServer()
	t.Cleanup(func() {
		ts.Close()
		s.Stop(context.Background())
	})
	return s, ts, ts.URL
}

func TestQueryCacheHitAndInvalidation(t *testing.T) {
	_, _, base := setupCacheServer(t)

	cacheCreateTable(t, base, "t",
		`{"name":"id","type":"INTEGER"},{"name":"v","type":"STRING"}`)
	cacheInsertAll(t, base, "t",
		`{"rows":[{"json":{"id":"1","v":"a"}},{"json":{"id":"2","v":"b"}}]}`)

	countSQL := "SELECT COUNT(*) AS c FROM d.t"

	// First run: computed.
	resp, hit := cacheQueryHit(t, base, countSQL)
	if hit {
		t.Fatal("first jobs.query must not be a cache hit")
	}
	if got := queryRows(t, resp); fmt.Sprint(got) != "[[2]]" {
		t.Fatalf("unexpected first result: %v", got)
	}
	// Second identical run: served from cache.
	if resp, hit = cacheQueryHit(t, base, countSQL); !hit {
		t.Fatal("identical jobs.query must be a cache hit")
	} else if got := queryRows(t, resp); fmt.Sprint(got) != "[[2]]" {
		t.Fatalf("cached result mismatch: %v", got)
	}

	// tabledata.insertAll changes table content: cache must invalidate.
	cacheInsertAll(t, base, "t", `{"rows":[{"json":{"id":"3","v":"c"}}]}`)
	if resp, hit = cacheQueryHit(t, base, countSQL); hit {
		t.Fatal("query after insertAll must be a cache miss")
	} else if got := queryRows(t, resp); fmt.Sprint(got) != "[[3]]" {
		t.Fatalf("unexpected post-insert result: %v", got)
	}
	if _, hit = cacheQueryHit(t, base, countSQL); !hit {
		t.Fatal("query must hit again after recompute")
	}

	// DML (INSERT) through jobs.query invalidates dependent results.
	cacheQuery(t, base, "INSERT INTO d.t (id, v) VALUES (4, 'd')", nil)
	if resp, hit = cacheQueryHit(t, base, countSQL); hit {
		t.Fatal("query after DML must be a cache miss")
	} else if got := queryRows(t, resp); fmt.Sprint(got) != "[[4]]" {
		t.Fatalf("unexpected post-DML result: %v", got)
	}
	if _, hit = cacheQueryHit(t, base, countSQL); !hit {
		t.Fatal("query must hit after DML recompute")
	}

	// Explicit useQueryCache=false bypasses both read and write; the
	// previously computed entry stays valid.
	noCache := false
	resp = cacheQuery(t, base, countSQL, &noCache)
	if h, _ := resp["cacheHit"].(bool); h {
		t.Fatal("useQueryCache=false must not return a cache hit")
	}
	if _, hit = cacheQueryHit(t, base, countSQL); !hit {
		t.Fatal("valid cache entry must survive a useQueryCache=false run")
	}
}

func TestQueryCacheUnrelatedTableChange(t *testing.T) {
	_, _, base := setupCacheServer(t)

	cacheCreateTable(t, base, "t", `{"name":"id","type":"INTEGER"}`)
	cacheCreateTable(t, base, "u", `{"name":"id","type":"INTEGER"}`)
	cacheInsertAll(t, base, "t", `{"rows":[{"json":{"id":"1"}}]}`)
	cacheInsertAll(t, base, "u", `{"rows":[{"json":{"id":"1"}}]}`)

	// Populate the cache for both tables' queries.
	if _, hit := cacheQueryHit(t, base, "SELECT COUNT(*) AS c FROM d.t"); hit {
		t.Fatal("first run must miss")
	}
	if _, hit := cacheQueryHit(t, base, "SELECT COUNT(*) AS c FROM d.u"); hit {
		t.Fatal("first run must miss")
	}
	if _, hit := cacheQueryHit(t, base, "SELECT COUNT(*) AS c FROM d.t"); !hit {
		t.Fatal("t query must hit")
	}
	if _, hit := cacheQueryHit(t, base, "SELECT COUNT(*) AS c FROM d.u"); !hit {
		t.Fatal("u query must hit")
	}

	// Changing only u must not invalidate t.
	cacheInsertAll(t, base, "u", `{"rows":[{"json":{"id":"2"}}]}`)
	if _, hit := cacheQueryHit(t, base, "SELECT COUNT(*) AS c FROM d.t"); !hit {
		t.Fatal("unrelated table change must not invalidate t query")
	}
	if resp, hit := cacheQueryHit(t, base, "SELECT COUNT(*) AS c FROM d.u"); hit {
		t.Fatal("u query must miss after u changed")
	} else if got := queryRows(t, resp); fmt.Sprint(got) != "[[2]]" {
		t.Fatalf("u result after insert: %v", got)
	}
}

func TestQueryCacheViewDependency(t *testing.T) {
	_, _, base := setupCacheServer(t)

	cacheCreateTable(t, base, "t", `{"name":"id","type":"INTEGER"}`)
	cacheInsertAll(t, base, "t", `{"rows":[{"json":{"id":"1"}},{"json":{"id":"2"}},{"json":{"id":"3"}}]}`)

	// Create a view referencing t.
	viewBody := fmt.Sprintf(
		`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":"v"},"view":{"query":"SELECT id * 2 AS doubled FROM d.t"}}`,
		cacheProjectID, cacheDatasetID)
	target := fmt.Sprintf("%s/datasets/%s/tables", cacheBase(base), cacheDatasetID)
	if code, resp := httpJSON(t, http.MethodPost, target, viewBody, nil); code != http.StatusOK {
		t.Fatalf("create view: %d (%v)", code, resp)
	}

	viewSQL := "SELECT SUM(doubled) AS s FROM d.v"
	resp, hit := cacheQueryHit(t, base, viewSQL)
	if hit {
		t.Fatal("first view query must miss")
	}
	if got := queryRows(t, resp); fmt.Sprint(got) != "[[12]]" {
		t.Fatalf("unexpected view result: %v", got)
	}
	if _, hit = cacheQueryHit(t, base, viewSQL); !hit {
		t.Fatal("view query must hit")
	}

	// A change to the underlying table must invalidate results read
	// through the view.
	cacheInsertAll(t, base, "t", `{"rows":[{"json":{"id":"4"}}]}`)
	if resp, hit = cacheQueryHit(t, base, viewSQL); hit {
		t.Fatal("view query must miss after underlying table change")
	} else if got := queryRows(t, resp); fmt.Sprint(got) != "[[20]]" {
		t.Fatalf("view result after change: %v", got)
	}
}

func TestQueryCacheBypassConditions(t *testing.T) {
	_, _, base := setupCacheServer(t)

	// Non-deterministic functions are never cached.
	for i := 0; i < 2; i++ {
		resp := cacheQuery(t, base, "SELECT RAND() AS r", nil)
		if h, _ := resp["cacheHit"].(bool); h {
			t.Fatalf("RAND() query %d must not be a cache hit", i+1)
		}
	}
	for i := 0; i < 2; i++ {
		resp := cacheQuery(t, base, "SELECT CURRENT_TIMESTAMP() AS ts", nil)
		if h, _ := resp["cacheHit"].(bool); h {
			t.Fatalf("CURRENT_TIMESTAMP() query %d must not be a cache hit", i+1)
		}
	}

	// Queries with a destination table bypass the cache.
	cacheCreateTable(t, base, "t", `{"name":"id","type":"INTEGER"}`)
	jobBody := fmt.Sprintf(
		`{"configuration":{"query":{"query":"SELECT 1 AS x","useLegacySql":false,"destinationTable":{"projectId":%q,"datasetId":%q,"tableId":"dst"},"defaultDataset":{"projectId":%q,"datasetId":%q}}}}`,
		cacheProjectID, cacheDatasetID, cacheProjectID, cacheDatasetID)
	for i := 0; i < 2; i++ {
		code, job := httpJSON(t, http.MethodPost, cacheBase(base)+"/jobs", jobBody, nil)
		if code != http.StatusOK {
			t.Fatalf("jobs.insert failed: %d (%v)", code, job)
		}
		stats, _ := job["statistics"].(map[string]any)
		queryStats, _ := stats["query"].(map[string]any)
		if h, _ := queryStats["cacheHit"].(bool); h {
			t.Fatalf("query job %d with destination table must not cache-hit", i+1)
		}
	}

	// Parameters are part of the cache key: distinct values never alias.
	p1 := fmt.Sprintf(`{"query":"SELECT @p AS x","useLegacySql":false,"parameterMode":"NAMED","queryParameters":[{"name":"p","parameterType":{"type":"INT64"},"parameterValue":{"value":"1"}}],"defaultDataset":{"projectId":%q,"datasetId":%q}}`, cacheProjectID, cacheDatasetID)
	p2 := fmt.Sprintf(`{"query":"SELECT @p AS x","useLegacySql":false,"parameterMode":"NAMED","queryParameters":[{"name":"p","parameterType":{"type":"INT64"},"parameterValue":{"value":"2"}}],"defaultDataset":{"projectId":%q,"datasetId":%q}}`, cacheProjectID, cacheDatasetID)
	for _, body := range []string{p1, p2, p1, p2} {
		code, resp := httpJSON(t, http.MethodPost, cacheBase(base)+"/queries", body, nil)
		if code != http.StatusOK {
			t.Fatalf("parameterized query failed: %d (%v)", code, resp)
		}
	}
	if _, hit := cacheQueryHit(t, base, "SELECT 1 AS x"); hit {
		t.Fatal("distinct request shapes must not share cache entries")
	}
}

func TestQueryCacheJobsInsertConsistency(t *testing.T) {
	_, _, base := setupCacheServer(t)
	cacheCreateTable(t, base, "t", `{"name":"id","type":"INTEGER"}`)
	cacheInsertAll(t, base, "t", `{"rows":[{"json":{"id":"1"}},{"json":{"id":"2"}},{"json":{"id":"3"}}]}`)

	insertJob := func() map[string]any {
		t.Helper()
		body := fmt.Sprintf(
			`{"configuration":{"query":{"query":"SELECT COUNT(*) AS c FROM d.t","useLegacySql":false,"defaultDataset":{"projectId":%q,"datasetId":%q}}}}`,
			cacheProjectID, cacheDatasetID)
		code, job := httpJSON(t, http.MethodPost, cacheBase(base)+"/jobs", body, nil)
		if code != http.StatusOK {
			t.Fatalf("jobs.insert failed: %d (%v)", code, job)
		}
		return job
	}
	cacheHitFromJob := func(job map[string]any) bool {
		stats, _ := job["statistics"].(map[string]any)
		queryStats, _ := stats["query"].(map[string]any)
		h, _ := queryStats["cacheHit"].(bool)
		return h
	}

	first := insertJob()
	if cacheHitFromJob(first) {
		t.Fatal("first jobs.insert must not cache-hit")
	}
	second := insertJob()
	if !cacheHitFromJob(second) {
		t.Fatal("second jobs.insert must cache-hit")
	}

	// jobs.get reports the same cacheHit as the job returned by insert.
	jobRef, _ := second["jobReference"].(map[string]any)
	jobID, _ := jobRef["jobId"].(string)
	code, gotJob := httpJSON(t, http.MethodGet, fmt.Sprintf("%s/jobs/%s", cacheBase(base), jobID), "", nil)
	if code != http.StatusOK {
		t.Fatalf("jobs.get failed: %d (%v)", code, gotJob)
	}
	if !cacheHitFromJob(gotJob) {
		t.Fatal("jobs.get must report the same cacheHit=true as the inserted job")
	}

	// getQueryResults returns the same rows and schema for the cached job.
	code, results := httpJSON(t, http.MethodGet, fmt.Sprintf("%s/queries/%s", cacheBase(base), jobID), "", nil)
	if code != http.StatusOK {
		t.Fatalf("getQueryResults failed: %d (%v)", code, results)
	}
	if total, _ := results["totalRows"].(string); total != "1" {
		t.Fatalf("getQueryResults totalRows = %q, want 1", total)
	}
	if complete, _ := results["jobComplete"].(bool); !complete {
		t.Fatal("cached job must be complete")
	}
	if rows := queryRows(t, results); fmt.Sprint(rows) != "[[3]]" {
		t.Fatalf("getQueryResults rows = %v, want [[3]]", rows)
	}

	// jobs.query and jobs.insert see the same underlying cached result.
	resp, hit := cacheQueryHit(t, base, "SELECT COUNT(*) AS c FROM d.t")
	if !hit {
		t.Fatal("jobs.query must hit the entry populated by jobs.insert")
	}
	if rows := queryRows(t, resp); fmt.Sprint(rows) != "[[3]]" {
		t.Fatalf("jobs.query rows = %v, want [[3]]", rows)
	}
}

func TestQueryCachePersistedRestart(t *testing.T) {
	const tableID = "t1"
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "emulator.db")
	storage := server.Storage(fmt.Sprintf("file:%s?cache=shared", dbFile))

	startRun := func() (*server.Server, *server.TestServer, string) {
		t.Helper()
		s, err := server.New(storage)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SetProject(cacheProjectID); err != nil {
			t.Fatal(err)
		}
		if err := s.Load(server.StructSource(types.NewProject(cacheProjectID, types.NewDataset(cacheDatasetID)))); err != nil {
			t.Fatalf("loading persisted project must be idempotent: %+v", err)
		}
		ts := s.TestServer()
		return s, ts, ts.URL
	}

	s1, ts1, base1 := startRun()
	cacheCreateTable(t, base1, tableID, `{"name":"id","type":"INTEGER"}`)
	cacheInsertAll(t, base1, tableID, `{"rows":[{"json":{"id":"1"}},{"json":{"id":"2"}}]}`)
	sql := fmt.Sprintf("SELECT COUNT(*) AS c FROM d.%s", tableID)
	if resp, hit := cacheQueryHit(t, base1, sql); hit {
		t.Fatal("first query must miss")
	} else if got := queryRows(t, resp); fmt.Sprint(got) != "[[2]]" {
		t.Fatalf("unexpected first result: %v", got)
	}
	if _, hit := cacheQueryHit(t, base1, sql); !hit {
		t.Fatal("second query must hit before restart")
	}
	ts1.Close()
	if err := s1.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	// Reopen the same persisted database: the cached entry survives and
	// validates against the persisted table versions.
	s2, ts2, base2 := startRun()
	defer func() {
		ts2.Close()
		s2.Stop(ctx)
	}()
	if resp, hit := cacheQueryHit(t, base2, sql); !hit {
		t.Fatal("query must hit cached entry after restart when data is unchanged")
	} else if got := queryRows(t, resp); fmt.Sprint(got) != "[[2]]" {
		t.Fatalf("cached post-restart result mismatch: %v", got)
	}

	// Mutating the table after restart must invalidate the persisted entry.
	cacheInsertAll(t, base2, tableID, `{"rows":[{"json":{"id":"3"}}]}`)
	if resp, hit := cacheQueryHit(t, base2, sql); hit {
		t.Fatal("query must miss after table change post-restart")
	} else if got := queryRows(t, resp); fmt.Sprint(got) != "[[3]]" {
		t.Fatalf("result after change: %v", got)
	}
}
