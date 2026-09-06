package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/goccy/bigquery-emulator/types"
)

func httpJSONInternal(t *testing.T, method, target, body string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, target, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("decode (status %d): %s", resp.StatusCode, data)
		}
	}
	return resp.StatusCode, out
}

func queryRowsInternal(body map[string]any) [][]string {
	rowsRaw, _ := body["rows"].([]any)
	rows := make([][]string, 0, len(rowsRaw))
	for _, r := range rowsRaw {
		fields, _ := r.(map[string]any)["f"].([]any)
		vals := make([]string, 0, len(fields))
		for _, f := range fields {
			v := f.(map[string]any)["v"]
			if v == nil {
				vals = append(vals, "<nil>")
				continue
			}
			vals = append(vals, fmt.Sprint(v))
		}
		rows = append(rows, vals)
	}
	return rows
}

// TestQueryCacheInvalidatedByStorageWrite ensures rows committed through the
// BigQuery Storage Write API invalidate cached query results that depend on
// the target table (the same guarantee as tabledata.insertAll / DML).
func TestQueryCacheInvalidatedByStorageWrite(t *testing.T) {
	const (
		projectID = "stest"
		datasetID = "ds"
		tableID   = "tbl"
	)
	ctx := context.Background()
	bqServer, err := New(TempStorage)
	if err != nil {
		t.Fatal(err)
	}
	if err := bqServer.Load(StructSource(types.NewProject(
		projectID,
		types.NewDataset(datasetID,
			types.NewTable(tableID, []*types.Column{types.NewColumn("id", types.INT64)}, nil),
		),
	))); err != nil {
		t.Fatal(err)
	}
	ts := bqServer.TestServer()
	defer func() { ts.Close(); bqServer.Stop(ctx) }()

	postQuery := func(sql string) (int, map[string]any) {
		body := fmt.Sprintf(`{"query":%q,"useLegacySql":false,"defaultDataset":{"projectId":%q,"datasetId":%q}}`,
			sql, projectID, datasetID)
		return httpJSONInternal(t, http.MethodPost, ts.URL+"/projects/"+projectID+"/queries", body)
	}

	// Prime the cache with a count over the (empty) table.
	code, resp := postQuery("SELECT COUNT(*) AS c FROM ds.tbl")
	if code != http.StatusOK {
		t.Fatalf("prime query: %d (%v)", code, resp)
	}
	if code, resp = postQuery("SELECT COUNT(*) AS c FROM ds.tbl"); code != http.StatusOK {
		t.Fatalf("hit query: %d (%v)", code, resp)
	}
	if hit, _ := resp["cacheHit"].(bool); !hit {
		t.Fatal("expected cache hit before Storage Write")
	}

	// Commit rows through the Storage Write API batch commit path.
	writeServer := &storageWriteServer{
		server:    bqServer,
		streamMap: map[string]*writeStreamStatus{},
	}
	streamName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/_default", projectID, datasetID, tableID)
	if _, err := writeServer.createDefaultStream(ctx, &storagepb.GetWriteStreamRequest{Name: streamName}); err != nil {
		t.Fatal(err)
	}
	status, canonicalName, err := writeServer.getOrCreateWriteStreamStatus(ctx, streamName)
	if err != nil {
		t.Fatal(err)
	}
	status.mu.Lock()
	status.rows = types.Data{{"id": int64(1)}, {"id": int64(2)}}
	status.mu.Unlock()
	commitResp, err := writeServer.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		WriteStreams: []string{canonicalName},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(commitResp.StreamErrors) != 0 {
		t.Fatalf("batch commit errors: %v", commitResp.StreamErrors)
	}

	// The cached count must now be invalidated and the recomputed result
	// reflect the committed rows.
	code, resp = postQuery("SELECT COUNT(*) AS c FROM ds.tbl")
	if code != http.StatusOK {
		t.Fatalf("post-write query: %d (%v)", code, resp)
	}
	if hit, _ := resp["cacheHit"].(bool); hit {
		t.Fatal("expected cache miss after Storage Write commit")
	}
	if rows := queryRowsInternal(resp); fmt.Sprint(rows) != "[[2]]" {
		t.Fatalf("post-write result = %v, want [[2]]", rows)
	}

	// And the fresh result is cached again.
	code, resp = postQuery("SELECT COUNT(*) AS c FROM ds.tbl")
	if code != http.StatusOK {
		t.Fatalf("re-hit query: %d (%v)", code, resp)
	}
	if hit, _ := resp["cacheHit"].(bool); !hit {
		t.Fatal("expected cache hit after recompute")
	}
}
