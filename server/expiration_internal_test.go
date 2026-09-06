package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	gjson "github.com/goccy/go-json"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/types"
)

// ---------- REST helpers ----------

func restDo(t *testing.T, ts *TestServer, method, path string, body interface{}) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := gjson.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, ts.URL+"/bigquery/v2"+path, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, respBody
}

func mustStatus(t *testing.T, status, want int, body []byte) {
	t.Helper()
	if status != want {
		t.Fatalf("unexpected HTTP status %d (want %d): %s", status, want, string(body))
	}
}

func createDatasetREST(t *testing.T, ts *TestServer, projectID, datasetID string, defaultTableExpirationMs int64) {
	t.Helper()
	ds := &bigqueryv2.Dataset{
		DatasetReference: &bigqueryv2.DatasetReference{
			ProjectId: projectID,
			DatasetId: datasetID,
		},
		DefaultTableExpirationMs: defaultTableExpirationMs,
	}
	status, body := restDo(t, ts, "POST", fmt.Sprintf("/projects/%s/datasets", projectID), ds)
	mustStatus(t, status, http.StatusOK, body)
}

func tableRef(projectID, datasetID, tableID string) *bigqueryv2.TableReference {
	return &bigqueryv2.TableReference{
		ProjectId: projectID,
		DatasetId: datasetID,
		TableId:   tableID,
	}
}

func createTableREST(t *testing.T, ts *TestServer, projectID, datasetID, tableID string, expirationMs int64) {
	t.Helper()
	tbl := &bigqueryv2.Table{
		TableReference: tableRef(projectID, datasetID, tableID),
		Schema: &bigqueryv2.TableSchema{Fields: []*bigqueryv2.TableFieldSchema{
			{Name: "v", Type: "INT64"},
		}},
		ExpirationTime: expirationMs,
	}
	status, body := restDo(t, ts, "POST",
		fmt.Sprintf("/projects/%s/datasets/%s/tables", projectID, datasetID), tbl)
	mustStatus(t, status, http.StatusOK, body)
}

func createViewREST(t *testing.T, ts *TestServer, projectID, datasetID, viewID string, expirationMs int64) {
	t.Helper()
	view := &bigqueryv2.Table{
		TableReference: tableRef(projectID, datasetID, viewID),
		View:           &bigqueryv2.ViewDefinition{Query: "SELECT 1 AS v"},
		ExpirationTime: expirationMs,
	}
	status, body := restDo(t, ts, "POST",
		fmt.Sprintf("/projects/%s/datasets/%s/tables", projectID, datasetID), view)
	mustStatus(t, status, http.StatusOK, body)
}

func getTableREST(t *testing.T, ts *TestServer, projectID, datasetID, tableID string) (int, *bigqueryv2.Table) {
	t.Helper()
	status, body := restDo(t, ts, "GET",
		fmt.Sprintf("/projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID), nil)
	if status != http.StatusOK {
		return status, nil
	}
	var tbl bigqueryv2.Table
	if err := gjson.Unmarshal(body, &tbl); err != nil {
		t.Fatalf("decode table %s: %v", string(body), err)
	}
	return status, &tbl
}

func listTablesREST(t *testing.T, ts *TestServer, projectID, datasetID string) *bigqueryv2.TableList {
	t.Helper()
	status, body := restDo(t, ts, "GET",
		fmt.Sprintf("/projects/%s/datasets/%s/tables", projectID, datasetID), nil)
	mustStatus(t, status, http.StatusOK, body)
	var list bigqueryv2.TableList
	if err := gjson.Unmarshal(body, &list); err != nil {
		t.Fatalf("decode table list: %v", err)
	}
	return &list
}

func patchTableREST(t *testing.T, ts *TestServer, projectID, datasetID, tableID string, patch map[string]interface{}) *bigqueryv2.Table {
	t.Helper()
	status, body := restDo(t, ts, "PATCH",
		fmt.Sprintf("/projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID), patch)
	mustStatus(t, status, http.StatusOK, body)
	var tbl bigqueryv2.Table
	if err := gjson.Unmarshal(body, &tbl); err != nil {
		t.Fatalf("decode patched table: %v", err)
	}
	return &tbl
}

func runQueryREST(t *testing.T, ts *TestServer, projectID string, query string) (int, []byte) {
	t.Helper()
	useLegacy := false
	req := &bigqueryv2.QueryRequest{Query: query, UseLegacySql: &useLegacy}
	return restDo(t, ts, "POST", fmt.Sprintf("/projects/%s/queries", projectID), req)
}

// ---------- direct (non-HTTP) helpers ----------

func loadProjectDataset(t *testing.T, s *Server, projectID, datasetID string) {
	t.Helper()
	if err := s.Load(StructSource(types.NewProject(projectID, types.NewDataset(datasetID)))); err != nil {
		t.Fatal(err)
	}
}

func contentQueryErr(s *Server, projectID, datasetID, query string) error {
	ctx := logger.WithLogger(context.Background(), s.logger)
	conn, err := s.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	_, err = s.contentRepo.Query(ctx, tx, projectID, datasetID, query, nil)
	return err
}

// setTableExpiration directly rewrites the persisted table metadata; ms==0
// clears the expiration. It is used to prepare restart/startup scenarios
// without going through the REST API.
func setTableExpiration(t *testing.T, s *Server, projectID, datasetID, tableID string, ms int64) {
	t.Helper()
	ctx := context.Background()
	project, err := s.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		t.Fatalf("find project: %v", err)
	}
	table := project.Dataset(datasetID).Table(tableID)
	if table == nil {
		t.Fatalf("table %s not found", tableID)
	}
	content, err := table.Content()
	if err != nil {
		t.Fatal(err)
	}
	content.ExpirationTime = ms
	encoded, err := gjson.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	var metadataMap map[string]interface{}
	if err := gjson.Unmarshal(encoded, &metadataMap); err != nil {
		t.Fatal(err)
	}
	conn, err := s.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Replace(ctx, tx.Tx(), metadataMap); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// ---------- tests ----------

// TestExpirationDefaultOnCreate: the dataset default only applies to ordinary
// tables without an explicit expirationTime; explicit values win and views
// are left alone.
func TestExpirationDefaultOnCreate(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "ds_default"
	)
	ctx := context.Background()
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	s.sweeperInterval = time.Hour
	loadProjectDataset(t, s, projectID, "other") // unrelated dataset
	ts := s.TestServer()
	defer func() {
		ts.Close()
		s.Stop(ctx)
	}()

	createDatasetREST(t, ts, projectID, datasetID, 60_000) // 1 minute default

	before := time.Now().UnixMilli()
	createTableREST(t, ts, projectID, datasetID, "plain", 0)
	createTableREST(t, ts, projectID, datasetID, "explicit", time.Now().Add(2*time.Hour).UnixMilli())
	createViewREST(t, ts, projectID, datasetID, "a_view", 0)
	after := time.Now().UnixMilli()

	_, plain := getTableREST(t, ts, projectID, datasetID, "plain")
	if plain.ExpirationTime == 0 {
		t.Fatal("ordinary table without explicit expiration did not inherit the dataset default")
	}
	if plain.ExpirationTime < before+55_000 || plain.ExpirationTime > after+65_000 {
		t.Fatalf("default expiration %d not within ~60s of creation (%d..%d)", plain.ExpirationTime, before+55_000, after+65_000)
	}
	_, explicit := getTableREST(t, ts, projectID, datasetID, "explicit")
	wantExplicit := time.Now().Add(2 * time.Hour).UnixMilli()
	if explicit.ExpirationTime < wantExplicit-5_000 || explicit.ExpirationTime > wantExplicit+5_000 {
		t.Fatalf("explicit expiration %d not preserved (want ~%d)", explicit.ExpirationTime, wantExplicit)
	}
	_, view := getTableREST(t, ts, projectID, datasetID, "a_view")
	if view.ExpirationTime != 0 {
		t.Fatalf("view inherited dataset default expiration: %d", view.ExpirationTime)
	}
	if view.Type != "VIEW" {
		t.Fatalf("view type = %q; want VIEW", view.Type)
	}

	// A dataset without a default never sets an expiration.
	createTableREST(t, ts, projectID, "other", "no_default", 0)
	_, noDefault := getTableREST(t, ts, projectID, "other", "no_default")
	if noDefault.ExpirationTime != 0 {
		t.Fatalf("table in dataset without default got expiration %d", noDefault.ExpirationTime)
	}
}

// TestExpirationSweepReclaims: expired tables and views are atomically
// reclaimed (metadata + content), future/unexpiring tables are untouched,
// and the sweep is idempotent.
func TestExpirationSweepReclaims(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "ds_sweep"
	)
	ctx := context.Background()
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	s.sweeperInterval = time.Hour
	loadProjectDataset(t, s, projectID, datasetID)
	ts := s.TestServer()
	defer func() {
		ts.Close()
		s.Stop(ctx)
	}()

	past := time.Now().Add(-time.Minute).UnixMilli()
	future := time.Now().Add(time.Hour).UnixMilli()
	createTableREST(t, ts, projectID, datasetID, "expired", past)
	createViewREST(t, ts, projectID, datasetID, "expired_view", past)
	createTableREST(t, ts, projectID, datasetID, "fresh", future)
	createTableREST(t, ts, projectID, datasetID, "permanent", 0)

	n, err := s.sweepExpiredTables(ctx, time.Now())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 2 {
		t.Fatalf("reclaimed %d tables; want 2", n)
	}

	// tables.get: expired objects 404, survivors 200.
	if status, _ := getTableREST(t, ts, projectID, datasetID, "expired"); status != http.StatusNotFound {
		t.Fatalf("GET expired table status = %d; want 404", status)
	}
	if status, _ := getTableREST(t, ts, projectID, datasetID, "expired_view"); status != http.StatusNotFound {
		t.Fatalf("GET expired view status = %d; want 404", status)
	}
	if status, _ := getTableREST(t, ts, projectID, datasetID, "fresh"); status != http.StatusOK {
		t.Fatalf("GET fresh table status = %d; want 200", status)
	}
	if status, _ := getTableREST(t, ts, projectID, datasetID, "permanent"); status != http.StatusOK {
		t.Fatalf("GET permanent table status = %d; want 200", status)
	}

	// tables.list and the parent count exclude reclaimed objects.
	list := listTablesREST(t, ts, projectID, datasetID)
	if list.TotalItems != 2 {
		t.Fatalf("tables.list TotalItems = %d; want 2", list.TotalItems)
	}
	for _, tbl := range list.Tables {
		if tbl.TableReference.TableId == "expired" || tbl.TableReference.TableId == "expired_view" {
			t.Fatalf("reclaimed %s still listed", tbl.TableReference.TableId)
		}
	}

	// SQL content: expired objects are gone, survivors queryable.
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `expired`"); err == nil {
		t.Fatal("query on reclaimed table unexpectedly succeeded")
	}
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `expired_view`"); err == nil {
		t.Fatal("query on reclaimed view unexpectedly succeeded")
	}
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `fresh`"); err != nil {
		t.Fatalf("query on surviving table failed: %v", err)
	}
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `permanent`"); err != nil {
		t.Fatalf("query on permanent table failed: %v", err)
	}

	// Storage API metadata resolution agrees with tables.get.
	if _, err := getTableMetadata(ctx, s, projectID, datasetID, "expired"); err == nil {
		t.Fatal("storage metadata lookup of reclaimed table unexpectedly succeeded")
	}
	if meta, err := getTableMetadata(ctx, s, projectID, datasetID, "fresh"); err != nil || meta == nil {
		t.Fatalf("storage metadata lookup of surviving table failed: %v", err)
	}

	// Idempotent: a second sweep reclaims nothing and errors nothing.
	n, err = s.sweepExpiredTables(ctx, time.Now())
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if n != 0 {
		t.Fatalf("second sweep reclaimed %d; want 0", n)
	}
}

// TestExpirationPatchClearAndOverride: PATCH can clear (null) or override the
// expiration; the sweeper honors the updated value.
func TestExpirationPatchClearAndOverride(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "ds_patch"
	)
	ctx := context.Background()
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	s.sweeperInterval = time.Hour
	loadProjectDataset(t, s, projectID, datasetID)
	ts := s.TestServer()
	defer func() {
		ts.Close()
		s.Stop(ctx)
	}()

	past := time.Now().Add(-time.Minute).UnixMilli()
	createTableREST(t, ts, projectID, datasetID, "t", past)

	// Clear the expiration: an explicit null removes the field.
	cleared := patchTableREST(t, ts, projectID, datasetID, "t", map[string]interface{}{
		"expirationTime": nil,
	})
	if cleared.ExpirationTime != 0 {
		t.Fatalf("PATCH null did not clear expiration: %d", cleared.ExpirationTime)
	}
	if n, err := s.sweepExpiredTables(ctx, time.Now()); err != nil || n != 0 {
		t.Fatalf("sweep after clear: n=%d err=%v; want 0/nil", n, err)
	}
	if status, _ := getTableREST(t, ts, projectID, datasetID, "t"); status != http.StatusOK {
		t.Fatalf("cleared table status = %d; want 200", status)
	}
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `t`"); err != nil {
		t.Fatalf("query on cleared table failed: %v", err)
	}

	// Override to a future deadline: the table survives.
	future := time.Now().Add(time.Hour).UnixMilli()
	patched := patchTableREST(t, ts, projectID, datasetID, "t", map[string]interface{}{
		"expirationTime": fmt.Sprintf("%d", future), // ,string wire format
	})
	if patched.ExpirationTime < future-5_000 || patched.ExpirationTime > future+5_000 {
		t.Fatalf("PATCH override expiration = %d; want ~%d", patched.ExpirationTime, future)
	}
	if n, err := s.sweepExpiredTables(ctx, time.Now()); err != nil || n != 0 {
		t.Fatalf("sweep after future override: n=%d err=%v; want 0/nil", n, err)
	}

	// Override to a past deadline: the next sweep reclaims.
	patchTableREST(t, ts, projectID, datasetID, "t", map[string]interface{}{
		"expirationTime": fmt.Sprintf("%d", time.Now().Add(-time.Second).UnixMilli()),
	})
	if n, err := s.sweepExpiredTables(ctx, time.Now()); err != nil || n != 1 {
		t.Fatalf("sweep after past override: n=%d err=%v; want 1/nil", n, err)
	}
	if status, _ := getTableREST(t, ts, projectID, datasetID, "t"); status != http.StatusNotFound {
		t.Fatalf("expired-overridden table status = %d; want 404", status)
	}
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `t`"); err == nil {
		t.Fatal("query on reclaimed table unexpectedly succeeded")
	}
}

// TestExpirationStartupCompensation: tables that expired while no sweeper
// ran (e.g. while the process was down) are reclaimed by the startup pass
// before traffic is served; future tables survive.
func TestExpirationStartupCompensation(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "ds_startup"
	)
	ctx := context.Background()
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	// Load fixtures (no REST server yet, so no sweeper is running).
	if err := s.Load(StructSource(types.NewProject(projectID, types.NewDataset(datasetID,
		types.NewTable("expired",
			[]*types.Column{types.NewColumn("v", types.INT64)},
			types.Data{{"v": 1}},
		),
		types.NewTable("future",
			[]*types.Column{types.NewColumn("v", types.INT64)},
			types.Data{{"v": 2}},
		),
	)))); err != nil {
		t.Fatal(err)
	}
	setTableExpiration(t, s, projectID, datasetID, "expired", time.Now().Add(-time.Minute).UnixMilli())
	setTableExpiration(t, s, projectID, datasetID, "future", time.Now().Add(time.Hour).UnixMilli())

	// Starting the test server runs the startup compensation pass.
	ts := s.TestServer()
	defer func() {
		ts.Close()
		s.Stop(ctx)
	}()

	if status, _ := getTableREST(t, ts, projectID, datasetID, "expired"); status != http.StatusNotFound {
		t.Fatalf("expired table after restart status = %d; want 404", status)
	}
	if status, futureTbl := getTableREST(t, ts, projectID, datasetID, "future"); status != http.StatusOK {
		t.Fatalf("future table after restart status = %d; want 200", status)
	} else if futureTbl.ExpirationTime == 0 {
		t.Fatal("future table lost its expiration on restart")
	}
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `future`"); err != nil {
		t.Fatalf("future table content not queryable after restart: %v", err)
	}
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `expired`"); err == nil {
		t.Fatal("expired table content still queryable after restart compensation")
	}
}

// TestExpirationRestartPersistence: reclaimed objects stay reclaimed across a
// restart, and objects with a future deadline (and their data) survive.
func TestExpirationRestartPersistence(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "ds_restart"
	)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "emulator.db")
	storage := Storage(fmt.Sprintf("file:%s?cache=shared", dbPath))

	// --- first process: create tables, reclaim the expired one ---
	s1, err := New(storage)
	if err != nil {
		t.Fatal(err)
	}
	s1.sweeperInterval = time.Hour
	loadProjectDataset(t, s1, projectID, datasetID)
	ts1 := s1.TestServer()
	createTableREST(t, ts1, projectID, datasetID, "alive", time.Now().Add(24*time.Hour).UnixMilli())
	createTableREST(t, ts1, projectID, datasetID, "doomed", time.Now().Add(-time.Minute).UnixMilli())
	if status, body := runQueryREST(t, ts1, projectID, "INSERT INTO `ds_restart.alive` (v) VALUES (42)"); status != http.StatusOK {
		t.Fatalf("insert into alive failed: %d %s", status, body)
	}
	if n, err := s1.sweepExpiredTables(ctx, time.Now()); err != nil || n != 1 {
		t.Fatalf("sweep on s1: n=%d err=%v; want 1/nil", n, err)
	}
	ts1.Close()
	if err := s1.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	// --- second process against the same database file ---
	s2, err := New(storage)
	if err != nil {
		t.Fatal(err)
	}
	s2.sweeperInterval = time.Hour
	// The CLI reloads the project/dataset source on startup; this must not
	// resurrect the reclaimed table.
	loadProjectDataset(t, s2, projectID, datasetID)
	ts2 := s2.TestServer()
	defer func() {
		ts2.Close()
		s2.Stop(ctx)
	}()

	if status, _ := getTableREST(t, ts2, projectID, datasetID, "doomed"); status != http.StatusNotFound {
		t.Fatalf("reclaimed table resurrected after restart: status = %d", status)
	}
	status, alive := getTableREST(t, ts2, projectID, datasetID, "alive")
	if status != http.StatusOK {
		t.Fatalf("future table lost after restart: status = %d", status)
	}
	if alive.ExpirationTime == 0 {
		t.Fatal("future table lost its expiration after restart")
	}
	if err := contentQueryErr(s2, projectID, datasetID, "SELECT * FROM `alive` WHERE v = 42"); err != nil {
		t.Fatalf("alive table data not queryable after restart: %v", err)
	}
}

// TestExpirationBackgroundSweeper: the stoppable background loop reclaims
// expired tables without touching future ones.
func TestExpirationBackgroundSweeper(t *testing.T) {
	const (
		projectID = "test"
		datasetID = "ds_bg"
	)
	ctx := context.Background()
	s, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	s.sweeperInterval = 50 * time.Millisecond
	loadProjectDataset(t, s, projectID, datasetID)
	ts := s.TestServer()
	defer func() {
		ts.Close()
		s.Stop(ctx)
	}()

	createTableREST(t, ts, projectID, datasetID, "soon_gone", time.Now().Add(-time.Second).UnixMilli())
	createTableREST(t, ts, projectID, datasetID, "stays", time.Now().Add(time.Hour).UnixMilli())

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if status, _ := getTableREST(t, ts, projectID, datasetID, "soon_gone"); status == http.StatusNotFound {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if status, _ := getTableREST(t, ts, projectID, datasetID, "soon_gone"); status != http.StatusNotFound {
		t.Fatal("background sweeper did not reclaim expired table within 3s")
	}
	if status, _ := getTableREST(t, ts, projectID, datasetID, "stays"); status != http.StatusOK {
		t.Fatalf("future table reclaimed by background sweeper: status = %d", status)
	}
	if err := contentQueryErr(s, projectID, datasetID, "SELECT * FROM `stays`"); err != nil {
		t.Fatalf("future table content lost: %v", err)
	}
}
