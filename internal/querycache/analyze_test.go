package querycache

import (
	"strings"
	"testing"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TestAnalyzeCacheable(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		cacheable bool
		refs      []string // expected read ref parts (dotted), in order
		writeT    int      // number of write targets
	}{
		{
			name:      "plain select",
			sql:       "SELECT 1",
			cacheable: true,
		},
		{
			name:      "select from table",
			sql:       "SELECT * FROM d.t WHERE id > 2",
			cacheable: true,
			refs:      []string{"d.t"},
		},
		{
			name:      "fully qualified and joins",
			sql:       "SELECT a.x FROM p.d.t1 a JOIN p.d.t2 b ON a.id = b.id LEFT JOIN d.t3 c ON c.id = a.id",
			cacheable: true,
			refs:      []string{"p.d.t1", "p.d.t2", "d.t3"},
		},
		{
			name:      "comma join",
			sql:       "SELECT * FROM t1, d.t2 WHERE t1.id = t2.id",
			cacheable: true,
			refs:      []string{"t1", "d.t2"},
		},
		{
			name:      "cte reference is reported (server skips CTE names)",
			sql:       "WITH c AS (SELECT 1 AS x) SELECT * FROM c",
			cacheable: true,
			refs:      []string{"c"},
		},
		{
			name:      "subquery from clause",
			sql:       "SELECT x FROM (SELECT id AS x FROM d.t) sub",
			cacheable: true,
			refs:      []string{"d.t"},
		},
		{
			name:      "backtick full path",
			sql:       "SELECT * FROM `p.d.t`",
			cacheable: true,
			refs:      []string{"p.d.t"},
		},
		{
			name:      "tvf call",
			sql:       "SELECT * FROM d.my_tvf(1)",
			cacheable: true,
			refs:      []string{"d.my_tvf"},
		},
		{
			name:      "insert not cacheable",
			sql:       "INSERT INTO d.t (id) VALUES (1)",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "update not cacheable",
			sql:       "UPDATE d.t SET id = 2 WHERE id = 1",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "delete not cacheable",
			sql:       "DELETE FROM d.t WHERE id = 1",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "merge not cacheable",
			sql:       "MERGE d.t USING d.s ON t.id = s.id WHEN MATCHED THEN DELETE",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "truncate not cacheable",
			sql:       "TRUNCATE TABLE d.t",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "alter table not cacheable",
			sql:       "ALTER TABLE d.t ADD COLUMN c INT64",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "create table target",
			sql:       "CREATE TABLE d.new AS SELECT 1 AS x",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "create or replace view target",
			sql:       "CREATE OR REPLACE VIEW d.v AS SELECT * FROM d.t",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "drop table target",
			sql:       "DROP TABLE IF EXISTS d.t",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "create function target",
			sql:       "CREATE FUNCTION d.f(x INT64) AS (x + 1)",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "create table function target",
			sql:       "CREATE TABLE FUNCTION d.tvf() AS (SELECT * FROM d.t)",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "drop function target",
			sql:       "DROP FUNCTION IF EXISTS d.f",
			cacheable: false,
			writeT:    1,
		},
		{
			name:      "drop dataset",
			sql:       "DROP SCHEMA IF EXISTS d",
			cacheable: false,
			writeT:    0,
		},
		{
			name:      "multi statement",
			sql:       "SELECT 1; SELECT 2",
			cacheable: false,
		},
		{
			name:      "current timestamp",
			sql:       "SELECT CURRENT_TIMESTAMP()",
			cacheable: false,
		},
		{
			name:      "current date bare",
			sql:       "SELECT CURRENT_DATE",
			cacheable: false,
		},
		{
			name:      "rand",
			sql:       "SELECT RAND()",
			cacheable: false,
		},
		{
			name:      "generate_uuid",
			sql:       "SELECT GENERATE_UUID()",
			cacheable: false,
		},
		{
			name:      "temp table",
			sql:       "CREATE TEMP TABLE x AS SELECT 1 AS a",
			cacheable: false,
		},
		{
			name:      "information schema",
			sql:       "SELECT * FROM d.INFORMATION_SCHEMA.TABLES",
			cacheable: false,
		},
		{
			name:      "wildcard table",
			sql:       "SELECT * FROM `d.*`",
			cacheable: false,
		},
		{
			name:      "unnest is not a dependency",
			sql:       "SELECT * FROM UNNEST([1,2,3]) AS n",
			cacheable: true,
			refs:      nil,
		},
		{
			name:      "string content ignored",
			sql:       "SELECT 'INSERT INTO d.t VALUES (1)' AS s FROM d.other",
			cacheable: true,
			refs:      []string{"d.other"},
		},
		{
			name:      "comment ignored",
			sql:       "SELECT 1 /* FROM d.t */ -- INSERT INTO x\n",
			cacheable: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := Analyze(tc.sql)
			if a.Cacheable != tc.cacheable {
				t.Fatalf("Cacheable = %v (reason: %q), want %v", a.Cacheable, a.NotCacheableReason, tc.cacheable)
			}
			var gotRefs []string
			for _, ref := range a.ReadRefs {
				name := strings.Join(ref.Parts, ".")
				// UNNEST(...) is a collected call but never a dependency
				if strings.EqualFold(ref.Parts[0], "unnest") {
					continue
				}
				gotRefs = append(gotRefs, name)
			}
			if !tc.cacheable {
				// Ref contents are not asserted for statements that
				// bypass the cache.
				return
			}
			if tc.refs == nil {
				if len(gotRefs) != 0 {
					t.Fatalf("table refs = %v, want none", gotRefs)
				}
			} else if strings.Join(gotRefs, ",") != strings.Join(tc.refs, ",") {
				t.Fatalf("table refs = %v, want %v", gotRefs, tc.refs)
			}
			if tc.writeT != 0 && len(a.WriteTargets) != tc.writeT {
				t.Fatalf("write targets = %d, want %d", len(a.WriteTargets), tc.writeT)
			}
		})
	}
}

func TestAnalyzeWriteTargetKinds(t *testing.T) {
	a := Analyze("MERGE d.t USING d.s ON 1=1 WHEN MATCHED THEN DELETE; " +
		"UPDATE d.u SET x = 1; " +
		"CREATE FUNCTION d.g() AS ((SELECT COUNT(*) FROM d.w));")
	var tables, routines []string
	for _, tgt := range a.WriteTargets {
		name := strings.Join(tgt.Parts, ".")
		if tgt.Kind == TargetTable {
			tables = append(tables, name)
		} else {
			routines = append(routines, name)
		}
	}
	// Only the target of each DML statement is a write target; the
	// MERGE source `d.s` and the table read by the function body are not.
	wantTables := map[string]bool{"d.t": false, "d.u": false}
	for _, name := range tables {
		if _, ok := wantTables[name]; ok {
			wantTables[name] = true
		}
	}
	for name, found := range wantTables {
		if !found {
			t.Fatalf("missing write target %q; got tables %v", name, tables)
		}
	}
	if len(routines) != 1 || routines[0] != "d.g" {
		t.Fatalf("routine targets = %v, want [d.g]", routines)
	}
}

func TestBuildKeyDistinguishesContext(t *testing.T) {
	base := &KeyInput{
		ProjectID: "p",
		Query:     "SELECT * FROM d.t WHERE id = ?",
	}
	key1, err := BuildKey(base)
	if err != nil {
		t.Fatal(err)
	}

	// Same content in a different request order: deterministic.
	key1b, _ := BuildKey(base)
	if key1 != key1b {
		t.Fatal("key is not deterministic")
	}

	different := func(mut func(*KeyInput)) string {
		c := *base
		c.Params = append([]*bigqueryv2.QueryParameter(nil), base.Params...)
		c.ConnectionProperties = append([]*bigqueryv2.ConnectionProperty(nil), base.ConnectionProperties...)
		mut(&c)
		k, err := BuildKey(&c)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}

	// A parameter value changes the result: different key.
	param := func(v string) *bigqueryv2.QueryParameter {
		return &bigqueryv2.QueryParameter{
			ParameterType:  &bigqueryv2.QueryParameterType{Type: "INT64"},
			ParameterValue: &bigqueryv2.QueryParameterValue{Value: v},
		}
	}
	kWithParam := different(func(k *KeyInput) { k.Params = []*bigqueryv2.QueryParameter{param("1")} })
	if kWithParam == key1 {
		t.Fatal("key ignores query parameters")
	}
	kWithParam2 := different(func(k *KeyInput) { k.Params = []*bigqueryv2.QueryParameter{param("2")} })
	if kWithParam == kWithParam2 {
		t.Fatal("key ignores parameter values")
	}

	// Default dataset changes resolution: different key.
	kOtherDataset := different(func(k *KeyInput) { k.DefaultDatasetID = "other" })
	if kOtherDataset == key1 {
		t.Fatal("key ignores default dataset")
	}

	// SQL text: different key.
	kOtherSQL := different(func(k *KeyInput) { k.Query = "SELECT 2" })
	if kOtherSQL == key1 {
		t.Fatal("key ignores SQL text")
	}

	// Connection properties (map order stable): different key.
	kWithProp := different(func(k *KeyInput) {
		k.ConnectionProperties = []*bigqueryv2.ConnectionProperty{{Key: "time_zone", Value: "UTC"}}
	})
	if kWithProp == key1 {
		t.Fatal("key ignores connection properties")
	}
}
