package contentdata

import (
	"regexp"
	"strings"

	"github.com/goccy/bigquery-emulator/internal/metadata"
)

// jobsHistoryTablePattern matches a region-qualified INFORMATION_SCHEMA.JOBS
// table reference, the only form BigQuery exposes for job history:
//
//	`region-us`.INFORMATION_SCHEMA.JOBS
//	`my-project.region-us`.INFORMATION_SCHEMA.JOBS
//	`my-project`.`region-us`.INFORMATION_SCHEMA.JOBS
//
// The optional leading segment (project) is the explicit project
// qualifier; the mandatory following segment is the region qualifier
// (`region-<location>`). The JOBS_BY_PROJECT / JOBS_BY_USER /
// JOBS_BY_ORGANIZATION / JOBS_BY_FOLDER variants are accepted as
// aliases: the emulator has no multi-project / multi-user audit
// surface, so every variant resolves to the same project-filtered
// rows.
//
// The expression only carries ASCII patterns; it is applied to a
// masked copy of the query in which string literals and comments are
// blanked out, so a SQL text that merely mentions INFORMATION_SCHEMA.JOBS
// inside a literal is left untouched.
var jobsHistoryTablePattern = regexp.MustCompile(
	`(?is)(?:(?P<project>` + "`(?:[^`]|``)*`" + `|[A-Za-z_][A-Za-z0-9_]*)` +
		`\s*\.\s*)?` +
		`(?P<region>` + "`(?:[^`]|``)*`" + `|[A-Za-z_][A-Za-z0-9_]*)` +
		`\s*\.\s*INFORMATION_SCHEMA\s*\.\s*JOBS(?:_BY_(?:PROJECT|USER|ORGANIZATION|FOLDER))?\b`,
)

// RewriteJobsHistoryQuery routes region-qualified INFORMATION_SCHEMA.JOBS
// references to the metadata-backed job history view. The user's WHERE /
// ORDER BY / LIMIT clauses are left in place and apply, unchanged, to the
// injected projection, so filtering and sorting behave exactly like
// BigQuery's view.
//
// currentProjectID is the project the job belongs to: it is used as the
// filter for the unqualified region form (`region-x`.INFORMATION_SCHEMA.JOBS),
// matching BigQuery, where that form reports the running project's jobs.
// An explicit project qualifier filters by that project instead.
//
// Dataset-scoped lookups (e.g. `my_dataset.INFORMATION_SCHEMA.JOBS`) are
// left unmodified: BigQuery does not provide a dataset-scoped jobs view,
// so the analyzer reports "table not found" just as it did before.
func RewriteJobsHistoryQuery(query, currentProjectID string) string {
	return rewriteJobsHistoryQuery(query, currentProjectID)
}

func rewriteJobsHistoryQuery(query, currentProjectID string) string {
	if !strings.Contains(strings.ToUpper(query), "INFORMATION_SCHEMA") {
		return query
	}
	masked := maskQueryLiterals(query)
	matches := jobsHistoryTablePattern.FindAllStringSubmatchIndex(masked, -1)
	if len(matches) == 0 {
		return query
	}

	var b strings.Builder
	last := 0
	for _, m := range matches {
		fullStart, fullEnd := m[0], m[1]
		// Reject anything that looks like a longer qualified name than the
		// documented [project.]region.INFORMATION_SCHEMA.JOBS path (e.g. an
		// extra leading `catalog.` segment). A quoted opener (`) delimits
		// tokens on its own, so a keyword glued to it (e.g. `FROM\`..\``)
		// is still a genuine reference.
		if fullStart > 0 {
			prev := rune(query[fullStart-1])
			if prev == '.' {
				continue
			}
			if query[fullStart] != '`' && isIdentRune(prev) {
				continue
			}
		}
		projectRaw := segmentFromMatch(query, m, jobsHistoryTablePattern.SubexpIndex("project"))
		regionRaw := segmentFromMatch(query, m, jobsHistoryTablePattern.SubexpIndex("region"))

		region, explicitProject := splitRegionQualifier(projectRaw, regionRaw)
		// Only rewrite genuine region qualifiers (`region-<location>`);
		// anything else (e.g. a dataset name) keeps its original,
		// analyzable form.
		if !strings.HasPrefix(strings.ToLower(region), "region") {
			continue
		}
		projectID := currentProjectID
		if explicitProject != "" {
			projectID = explicitProject
		}

		b.WriteString(query[last:fullStart])
		b.WriteString("(SELECT * FROM ")
		b.WriteString(metadata.JobsHistoryViewName)
		b.WriteString(" WHERE project_id = '")
		b.WriteString(escapeStringLiteral(projectID))
		b.WriteString("')")
		last = fullEnd
	}
	if last == 0 {
		return query
	}
	b.WriteString(query[last:])
	return b.String()
}

func segmentFromMatch(query string, match []int, groupIndex int) string {
	if groupIndex < 0 {
		return ""
	}
	start, end := match[2*groupIndex], match[2*groupIndex+1]
	if start < 0 || end < 0 {
		return ""
	}
	return unquoteIdentifier(query[start:end])
}

// splitRegionQualifier turns the (unquoted) project and region match
// groups into a (region, explicitProject) pair. A single backticked
// segment can carry both parts, `project.region-us`, mirroring the
// dotted-identifier splitting the SQL engine itself performs.
func splitRegionQualifier(projectRaw, regionRaw string) (region, project string) {
	regionParts := strings.Split(regionRaw, ".")
	region = regionParts[len(regionParts)-1]
	if projectRaw != "" {
		projectParts := strings.Split(projectRaw, ".")
		project = projectParts[0]
		return
	}
	if len(regionParts) > 1 {
		project = regionParts[0]
	}
	return
}

func unquoteIdentifier(seg string) string {
	seg = strings.TrimSpace(seg)
	if len(seg) >= 2 && seg[0] == '`' && seg[len(seg)-1] == '`' {
		seg = seg[1 : len(seg)-1]
		seg = strings.ReplaceAll(seg, "``", "`")
	}
	return seg
}

func escapeStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func isIdentRune(r rune) bool {
	return r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// maskQueryLiterals returns a copy of query with the contents of string
// literals and comments replaced by spaces, while backtick-quoted
// identifiers are left intact (they are part of table references and may
// contain dots, e.g. `project.region-us`). Newlines are preserved so
// parser error offsets keep lining up.
func maskQueryLiterals(query string) string {
	out := make([]byte, len(query))
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '`':
			out[i] = '`'
			i++
			for i < len(query) {
				if query[i] == '`' {
					if i+1 < len(query) && query[i+1] == '`' {
						out[i] = '`'
						out[i+1] = '`'
						i += 2
						continue
					}
					out[i] = '`'
					break
				}
				out[i] = query[i]
				i++
			}
		case c == '\'' || c == '"':
			// Triple-quoted strings ('''...''' / """...""") if present.
			if i+2 < len(query) && query[i+1] == c && query[i+2] == c {
				out[i], out[i+1], out[i+2] = ' ', ' ', ' '
				i += 3
				for i < len(query) {
					if i+2 < len(query) && query[i] == c && query[i+1] == c && query[i+2] == c {
						out[i], out[i+1], out[i+2] = ' ', ' ', ' '
						i += 2
						break
					}
					if query[i] == '\n' {
						out[i] = '\n'
					} else {
						out[i] = ' '
					}
					i++
				}
				continue
			}
			quote := c
			out[i] = ' '
			i++
			for i < len(query) {
				if query[i] == quote {
					if i+1 < len(query) && query[i+1] == quote {
						out[i], out[i+1] = ' ', ' '
						i += 2
						continue
					}
					out[i] = ' '
					break
				}
				if query[i] == '\n' {
					out[i] = '\n'
				} else {
					out[i] = ' '
				}
				i++
			}
		case c == '-' && i+1 < len(query) && query[i+1] == '-':
			for i < len(query) && query[i] != '\n' {
				out[i] = ' '
				i++
			}
			if i < len(query) {
				out[i] = '\n'
			}
		case c == '/' && i+1 < len(query) && query[i+1] == '*':
			out[i], out[i+1] = ' ', ' '
			i += 2
			for i < len(query) {
				if query[i] == '*' && i+1 < len(query) && query[i+1] == '/' {
					out[i], out[i+1] = ' ', ' '
					i++
					break
				}
				if query[i] == '\n' {
					out[i] = '\n'
				} else {
					out[i] = ' '
				}
				i++
			}
		default:
			out[i] = c
		}
	}
	return string(out)
}
