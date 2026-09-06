package server_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// runQueryRows runs q through the jobs.query path and returns every row.
func runQueryRows(t *testing.T, ctx context.Context, client *bigquery.Client, q string) [][]bigquery.Value {
	t.Helper()
	it, err := client.Query(q).Read(ctx)
	if err != nil {
		t.Fatalf("submit %q: %v", q, err)
	}
	var rows [][]bigquery.Value
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("iterate %q: %v", q, err)
		}
		rows = append(rows, row)
	}
	return rows
}

// listJobIDs returns the set of job ids visible through jobs.list.
func listJobIDs(t *testing.T, ctx context.Context, client *bigquery.Client) map[string]bool {
	t.Helper()
	ids := map[string]bool{}
	it := client.Jobs(ctx)
	for {
		j, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("jobs.list: %v", err)
		}
		ids[j.ID()] = true
	}
	return ids
}

// TestJobsInformationSchema covers the BigQuery-compatible
// INFORMATION_SCHEMA.JOBS region-scoped job history view: the region and
// project+region qualification forms, filtering and sorting, failed /
// cancelled / deleted jobs, and compatibility of the existing surfaces.
func TestJobsInformationSchema(t *testing.T) {
	ctx := context.Background()

	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(server.StructSource(types.NewProject("test", types.NewDataset("ds")))); err != nil {
		t.Fatal(err)
	}
	testServer := bqServer.TestServer()
	defer func() {
		testServer.Close()
		bqServer.Stop(ctx)
	}()

	client, err := bigquery.NewClient(ctx, "test",
		option.WithEndpoint(testServer.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Produce history: a DDL job, a DML job, a FAILED jobs.query and a
	// successful query with a materialized result set.
	runQueryRows(t, ctx, client, "CREATE TABLE ds.t (x INT64)")
	runQueryRows(t, ctx, client, "INSERT ds.t VALUES (1),(2)")
	if _, err := client.Query("SELECT * FROM ds.t WHERE x = no_such_column").Read(ctx); err == nil {
		t.Fatal("expected the invalid query to fail")
	}
	if _, err := client.Query("SELECT 1 AS one").Run(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("region scoped rows", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT job_id, job_type, state FROM `region-us`.INFORMATION_SCHEMA.JOBS ORDER BY creation_time")
		// The four setup jobs; the history query itself is not yet
		// committed while the view is scanned.
		if len(rows) != 4 {
			t.Fatalf("want 4 history rows, got %d: %v", len(rows), rows)
		}
		for _, r := range rows {
			if r[1] != "QUERY" {
				t.Errorf("job_type = %v, want QUERY", r[1])
			}
			if r[2] != "DONE" {
				t.Errorf("state = %v, want DONE", r[2])
			}
		}
	})

	t.Run("project and region qualified", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT job_id FROM `test.region-us`.INFORMATION_SCHEMA.JOBS")
		if len(rows) < 4 {
			t.Fatalf("want >=4 rows, got %d", len(rows))
		}
	})

	t.Run("separately quoted project and region", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT job_id FROM `test`.`region-us`.INFORMATION_SCHEMA.JOBS")
		if len(rows) < 4 {
			t.Fatalf("want >=4 rows, got %d", len(rows))
		}
	})

	t.Run("JOBS_BY_PROJECT alias", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT")
		if len(rows) < 4 {
			t.Fatalf("want >=4 rows, got %d", len(rows))
		}
	})

	t.Run("times and timestamps", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT creation_time, start_time, end_time FROM `region-us`.INFORMATION_SCHEMA.JOBS LIMIT 1")
		if len(rows) != 1 {
			t.Fatalf("want 1 row, got %d", len(rows))
		}
		for _, v := range rows[0][:3] {
			if _, ok := v.(time.Time); !ok {
				t.Errorf("timestamp column = %T, want time.Time", v)
			}
		}
	})

	t.Run("failed job exposes error_result", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT error_result.reason, error_result.message FROM `region-us`.INFORMATION_SCHEMA.JOBS "+
				"WHERE error_result IS NOT NULL")
		if len(rows) != 1 {
			t.Fatalf("want exactly 1 failed job row, got %d: %v", len(rows), rows)
		}
		if rows[0][0] != "jobInternalError" {
			t.Errorf("error_result.reason = %v, want jobInternalError", rows[0][0])
		}
		if rows[0][1] == "" {
			t.Error("error_result.message is empty")
		}
	})

	t.Run("filter and sort by query text", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS "+
				"WHERE query LIKE '%CREATE TABLE%' ORDER BY start_time DESC LIMIT 1")
		if len(rows) != 1 {
			t.Fatalf("want 1 row, got %d", len(rows))
		}
	})

	t.Run("destination table for result jobs", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT destination_table.dataset_id, destination_table.table_id "+
				"FROM `region-us`.INFORMATION_SCHEMA.JOBS "+
				"WHERE destination_table IS NOT NULL AND query = 'SELECT 1 AS one'")
		if len(rows) != 1 {
			t.Fatalf("want 1 destination row, got %d", len(rows))
		}
		// A SELECT without explicit destination is materialized into the
		// anonymous results dataset named after the job id.
		if rows[0][0] != rows[0][1] {
			t.Errorf("anonymous destination dataset/table mismatch: %v", rows[0])
		}
	})

	t.Run("cancelled job is marked stopped", func(t *testing.T) {
		job, err := client.Query("SELECT 42").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Cancel(ctx); err != nil {
			t.Fatal(err)
		}
		rows := runQueryRows(t, ctx, client,
			"SELECT error_result.reason FROM `region-us`.INFORMATION_SCHEMA.JOBS "+
				"WHERE job_id = '"+job.ID()+"'")
		if len(rows) != 1 || rows[0][0] != "stopped" {
			t.Fatalf("cancel not reflected in history: %v", rows)
		}
	})

	t.Run("deleted job disappears from history", func(t *testing.T) {
		job, err := client.Query("SELECT 43").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := job.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		if err := job.Delete(ctx); err != nil {
			t.Fatal(err)
		}
		rows := runQueryRows(t, ctx, client,
			"SELECT COUNT(*) AS c FROM `region-us`.INFORMATION_SCHEMA.JOBS "+
				"WHERE job_id = '"+job.ID()+"'")
		if len(rows) != 1 || rows[0][0].(int64) != 0 {
			t.Fatalf("deleted job still visible in history: %v", rows)
		}
	})

	t.Run("dry run creates no history row", func(t *testing.T) {
		before := len(listJobIDs(t, ctx, client))
		q := client.Query("SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS")
		q.QueryConfig.DryRun = true
		// Dry runs are reported through jobs.insert synchronously and must
		// not persist a job.
		if _, err := q.Run(ctx); err != nil {
			t.Fatal(err)
		}
		after := len(listJobIDs(t, ctx, client))
		if after != before {
			t.Fatalf("dry run changed jobs.list count: before=%d after=%d", before, after)
		}
	})

	t.Run("dataset scoped jobs view stays invalid", func(t *testing.T) {
		_, err := client.Query("SELECT job_id FROM ds.INFORMATION_SCHEMA.JOBS").Read(ctx)
		if err == nil {
			t.Fatal("dataset-scoped INFORMATION_SCHEMA.JOBS must not resolve")
		}
	})

	t.Run("ordinary INFORMATION_SCHEMA still works", func(t *testing.T) {
		rows := runQueryRows(t, ctx, client,
			"SELECT column_name FROM ds.INFORMATION_SCHEMA.COLUMNS "+
				"WHERE table_name = 't' ORDER BY ordinal_position")
		if len(rows) != 1 || rows[0][0] != "x" {
			t.Fatalf("INFORMATION_SCHEMA.COLUMNS broken: %v", rows)
		}
	})

	t.Run("jobs.list stays consistent", func(t *testing.T) {
		view := runQueryRows(t, ctx, client,
			"SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS")
		list := listJobIDs(t, ctx, client)
		// Every history row belongs to a job jobs.list can see. The view
		// cannot see the row of the query that scans it (committed after
		// the scan), so list may be one larger — never smaller.
		if len(list) < len(view) {
			t.Fatalf("jobs.list (%d) has fewer jobs than the history view (%d)", len(list), len(view))
		}
		for _, r := range view {
			if !list[r[0].(string)] {
				t.Errorf("history row %s missing from jobs.list", r[0])
			}
		}
	})

	t.Run("jobs.get and getQueryResults keep working", func(t *testing.T) {
		job, err := client.Query("SELECT 7 AS seven").Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := status.Err(); err != nil {
			t.Fatal(err)
		}
		got, err := client.JobFromID(ctx, job.ID())
		if err != nil {
			t.Fatalf("jobs.get: %v", err)
		}
		if got.ID() != job.ID() {
			t.Fatalf("jobs.get id mismatch: %s vs %s", got.ID(), job.ID())
		}
		it, err := client.Query("SELECT 8 AS eight").Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			t.Fatalf("getQueryResults path: %v", err)
		}
	})

	t.Run("raw REST query with region view", func(t *testing.T) {
		body := []byte(`{"query":"SELECT job_id FROM ` + "`region-us`" + `.INFORMATION_SCHEMA.JOBS LIMIT 1","useLegacySql":false}`)
		req, err := http.NewRequest(http.MethodPost, testServer.URL+"/projects/test/queries",
			bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("rest jobs.query status = %d", res.StatusCode)
		}
	})
}

// TestJobsInformationSchemaRestart verifies that job history kept across
// a process restart matches the REST metadata exactly: the view is
// derived from the same persisted jobs table that backs jobs.list.
func TestJobsInformationSchemaRestart(t *testing.T) {
	ctx := context.Background()
	dbFile := t.TempDir() + "/emulator.db"

	start := func() (*server.Server, *server.TestServer, *bigquery.Client) {
		bqServer, err := server.New(server.Storage("file:" + dbFile + "?cache=shared"))
		if err != nil {
			t.Fatal(err)
		}
		if err := bqServer.Load(server.StructSource(types.NewProject("test", types.NewDataset("ds")))); err != nil {
			t.Fatal(err)
		}
		ts := bqServer.TestServer()
		client, err := bigquery.NewClient(ctx, "test",
			option.WithEndpoint(ts.URL),
			option.WithoutAuthentication(),
		)
		if err != nil {
			t.Fatal(err)
		}
		return bqServer, ts, client
	}

	bqServer, ts, client := start()
	runQueryRows(t, ctx, client, "CREATE TABLE ds.t (x INT64)")
	runQueryRows(t, ctx, client, "INSERT ds.t VALUES (7)")
	if _, err := client.Query("SELECT bad FROM ds.t").Read(ctx); err == nil {
		t.Fatal("expected query failure")
	}
	beforeList := listJobIDs(t, ctx, client)
	client.Close()
	ts.Close()
	if err := bqServer.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	bqServer2, ts2, client2 := start()
	defer func() {
		client2.Close()
		ts2.Close()
		bqServer2.Stop(ctx)
	}()

	// The first post-restart history scan sees exactly the jobs present
	// before shutdown (its own job row is committed only afterwards).
	rows := runQueryRows(t, ctx, client2,
		"SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS ORDER BY start_time")
	if len(rows) != len(beforeList) {
		t.Fatalf("history rows after restart = %d, jobs.list before restart = %d", len(rows), len(beforeList))
	}
	for _, r := range rows {
		if !beforeList[r[0].(string)] {
			t.Errorf("history row %s did not exist before restart", r[0])
		}
	}

	// The failed job survived the restart with its error result.
	errRows := runQueryRows(t, ctx, client2,
		"SELECT error_result.reason FROM `region-us`.INFORMATION_SCHEMA.JOBS WHERE error_result IS NOT NULL")
	if len(errRows) != 1 || errRows[0][0] != "jobInternalError" {
		t.Fatalf("failed job not preserved across restart: %v", errRows)
	}

	// User data persisted as well.
	data := runQueryRows(t, ctx, client2, "SELECT x FROM ds.t")
	if len(data) != 1 || data[0][0] != int64(7) {
		t.Fatalf("user data not readable after restart: %v", data)
	}
}
