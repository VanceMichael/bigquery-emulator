package contentdata

import (
	"strings"
	"testing"

	"github.com/goccy/bigquery-emulator/internal/metadata"
)

func TestRewriteJobsHistoryQuery(t *testing.T) {
	const view = metadata.JobsHistoryViewName
	contains := func(t *testing.T, got, want string) {
		t.Helper()
		if !strings.Contains(got, want) {
			t.Errorf("rewritten query = %q, want substring %q", got, want)
		}
	}
	notContains := func(t *testing.T, got, want string) {
		t.Helper()
		if strings.Contains(got, want) {
			t.Errorf("rewritten query = %q, must not contain %q", got, want)
		}
	}

	t.Run("region qualified current project", func(t *testing.T) {
		got := rewriteJobsHistoryQuery(
			"SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS WHERE state = 'DONE'",
			"proj",
		)
		notContains(t, got, "`region-us`")
		contains(t, got, "(SELECT * FROM "+view+" WHERE project_id = 'proj')")
		// user clauses stay intact
		contains(t, got, "WHERE state = 'DONE'")
	})

	t.Run("project and region qualified", func(t *testing.T) {
		got := rewriteJobsHistoryQuery(
			"SELECT job_id FROM `other.region-asia-northeast1`.INFORMATION_SCHEMA.JOBS",
			"proj",
		)
		contains(t, got, "project_id = 'other'")
	})

	t.Run("separately quoted project and region", func(t *testing.T) {
		got := rewriteJobsHistoryQuery(
			"SELECT job_id FROM `other`.`region-eu`.INFORMATION_SCHEMA.JOBS",
			"proj",
		)
		contains(t, got, "project_id = 'other'")
	})

	t.Run("JOBS_BY_PROJECT variant", func(t *testing.T) {
		got := rewriteJobsHistoryQuery(
			"SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT",
			"proj",
		)
		contains(t, got, view)
		contains(t, got, "project_id = 'proj'")
	})

	t.Run("case insensitive", func(t *testing.T) {
		got := rewriteJobsHistoryQuery(
			"select job_id from `REGION-US`.information_schema.jobs",
			"proj",
		)
		contains(t, got, view)
	})

	t.Run("dataset scoped JOBS is not rewritten", func(t *testing.T) {
		src := "SELECT job_id FROM my_dataset.INFORMATION_SCHEMA.JOBS"
		got := rewriteJobsHistoryQuery(src, "proj")
		if got != src {
			t.Errorf("dataset-scoped JOBS must not be rewritten: %q", got)
		}
	})

	t.Run("other INFORMATION_SCHEMA views untouched", func(t *testing.T) {
		src := "SELECT column_name FROM ds.INFORMATION_SCHEMA.COLUMNS WHERE table_name='x'"
		got := rewriteJobsHistoryQuery(src, "proj")
		if got != src {
			t.Errorf("non-JOBS INFORMATION_SCHEMA queries must not be rewritten: %q", got)
		}
	})

	t.Run("mention inside string literal untouched", func(t *testing.T) {
		src := "SELECT '`region-us`.INFORMATION_SCHEMA.JOBS' AS s"
		got := rewriteJobsHistoryQuery(src, "proj")
		if got != src {
			t.Errorf("string literal content must be left untouched: %q", got)
		}
	})

	t.Run("mention inside line comment untouched", func(t *testing.T) {
		src := "SELECT 1 -- `region-us`.INFORMATION_SCHEMA.JOBS\n"
		got := rewriteJobsHistoryQuery(src, "proj")
		if got != src {
			t.Errorf("comment content must be left untouched: %q", got)
		}
	})

	t.Run("multiple references each filtered", func(t *testing.T) {
		got := rewriteJobsHistoryQuery(
			"SELECT * FROM `region-us`.INFORMATION_SCHEMA.JOBS a JOIN `other.region-us`.INFORMATION_SCHEMA.JOBS b ON a.job_id = b.job_id",
			"proj",
		)
		if strings.Count(got, view) != 2 {
			t.Errorf("want 2 view references, got %q", got)
		}
		contains(t, got, "project_id = 'proj'")
		contains(t, got, "project_id = 'other'")
	})

	t.Run("backtick glued to keyword", func(t *testing.T) {
		got := rewriteJobsHistoryQuery(
			"SELECT job_id FROM`region-us`.INFORMATION_SCHEMA.JOBS",
			"proj",
		)
		contains(t, got, view)
	})

	t.Run("single quote in project id escaped", func(t *testing.T) {
		got := rewriteJobsHistoryQuery(
			"SELECT 1 FROM `region-us`.INFORMATION_SCHEMA.JOBS",
			"pro'ject",
		)
		contains(t, got, "project_id = 'pro''ject'")
	})
}

func TestMaskQueryLiterals(t *testing.T) {
	// backtick identifiers must survive masking (dots included)
	masked := maskQueryLiterals("SELECT 'x' AS s, `p.region-us`.INFORMATION_SCHEMA.JOBS -- c\n")
	if !strings.Contains(masked, "`p.region-us`.INFORMATION_SCHEMA.JOBS") {
		t.Errorf("backtick identifiers must survive masking: %q", masked)
	}
	if strings.Contains(masked, "'x'") {
		t.Errorf("string literal should be masked: %q", masked)
	}
	if strings.Contains(masked, "-- c") {
		t.Errorf("line comment should be masked: %q", masked)
	}
}
