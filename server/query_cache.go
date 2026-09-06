package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/goccy/googlesqlite"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	"github.com/goccy/bigquery-emulator/internal/querycache"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
)

// queryCacheTTL mirrors BigQuery's documented best-effort cache lifetime
// (roughly 24 hours): entries older than this are treated as misses even
// when their dependencies are unchanged.
const queryCacheTTL = 24 * time.Hour

// queryCacheEnabled interprets the useQueryCache option of jobs.query /
// jobs.insert. The option defaults to true in BigQuery; only an explicit
// false disables the cache.
func queryCacheEnabled(useQueryCache *bool) bool {
	return useQueryCache == nil || *useQueryCache
}

// buildQueryCacheKey computes the stable cache key for a query request.
// Two requests that must produce the same result (same effective project,
// default dataset, SQL text, parameters and result-affecting options) hash
// identically.
func (s *Server) buildQueryCacheKey(
	requestProjectID, effectiveProjectID, defaultDatasetID string,
	defaultDataset *bigqueryv2.DatasetReference,
	query string,
	params []*bigqueryv2.QueryParameter,
	parameterMode string,
	connectionProperties []*bigqueryv2.ConnectionProperty,
	useLegacySQL bool,
) (string, error) {
	key := &querycache.KeyInput{
		ProjectID:            requestProjectID,
		DefaultProjectID:     effectiveProjectID,
		DefaultDatasetID:     defaultDatasetID,
		Query:                query,
		ParameterMode:        parameterMode,
		ConnectionProperties: connectionProperties,
		UseLegacySQL:         useLegacySQL,
		Params:               params,
	}
	if defaultDataset != nil {
		if defaultDataset.ProjectId != "" {
			key.DefaultProjectID = defaultDataset.ProjectId
		}
		key.DefaultDatasetID = defaultDataset.DatasetId
	}
	return querycache.BuildKey(key)
}

// cacheLookup returns the cached response for key when a fresh entry exists
// and every dependency still has its snapshotted version. A miss (including
// any storage error or an invalid entry) is reported as false: the caller
// recomputes the result, so cache failures never affect query correctness.
func (s *Server) cacheLookup(ctx context.Context, tx *connection.Tx, key string) (*internaltypes.QueryResponse, bool) {
	if err := tx.MetadataRepoMode(); err != nil {
		logger.Logger(ctx).Warn("query cache: failed to switch metadata mode", zap.Error(err))
		return nil, false
	}
	entry, err := s.metaRepo.FindQueryCacheEntry(ctx, tx.Tx(), key)
	if err != nil {
		logger.Logger(ctx).Warn("query cache: lookup failed", zap.Error(err))
		return nil, false
	}
	if entry == nil {
		return nil, false
	}
	if time.Now().Unix()-entry.CreatedAt > int64(queryCacheTTL.Seconds()) {
		if err := s.metaRepo.DeleteQueryCacheEntry(ctx, tx.Tx(), key); err != nil {
			logger.Logger(ctx).Warn("query cache: failed to evict expired entry", zap.Error(err))
		}
		return nil, false
	}
	var deps []metadata.Dependency
	if entry.Dependencies != "" {
		if err := json.Unmarshal([]byte(entry.Dependencies), &deps); err != nil {
			logger.Logger(ctx).Warn("query cache: corrupted dependencies, evicting", zap.Error(err))
			_ = s.metaRepo.DeleteQueryCacheEntry(ctx, tx.Tx(), key)
			return nil, false
		}
	}
	for _, dep := range deps {
		var (
			version int64
			found   bool
		)
		switch dep.Kind {
		case metadata.DependencyTable:
			version, found, err = s.metaRepo.FindTableVersion(ctx, tx.Tx(), dep.ProjectID, dep.DatasetID, dep.Name)
		case metadata.DependencyRoutine:
			version, _, found, err = s.metaRepo.FindRoutineVersion(ctx, tx.Tx(), dep.ProjectID, dep.DatasetID, dep.Name)
		default:
			err = fmt.Errorf("unknown dependency kind %q", dep.Kind)
		}
		if err != nil {
			logger.Logger(ctx).Warn("query cache: dependency check failed, treating as miss", zap.Error(err))
			return nil, false
		}
		if !found || version != dep.Version {
			// The entry is stale: evict it best-effort so subsequent
			// requests repopulate from a fresh computation.
			if delErr := s.metaRepo.DeleteQueryCacheEntry(ctx, tx.Tx(), key); delErr != nil {
				logger.Logger(ctx).Warn("query cache: failed to evict stale entry", zap.Error(delErr))
			}
			return nil, false
		}
	}
	var response internaltypes.QueryResponse
	if err := json.Unmarshal([]byte(entry.Response), &response); err != nil {
		logger.Logger(ctx).Warn("query cache: corrupted response payload, evicting", zap.Error(err))
		_ = s.metaRepo.DeleteQueryCacheEntry(ctx, tx.Tx(), key)
		return nil, false
	}
	// JSON decoding turns nested TableRow/[]*TableCell values into generic
	// maps/slices and drops the (non-serialized) cell column names. Restore
	// the in-memory shapes so a cache hit is indistinguishable from a fresh
	// computation (e.g. when materializing the jobs.insert results table).
	rehydrateResponse(&response)
	return &response, true
}

// rehydrateResponse restores the concrete TableRow/[]*TableCell shapes and
// column names that JSON round-trip erases from cached result rows.
func rehydrateResponse(resp *internaltypes.QueryResponse) {
	if resp == nil || resp.Schema == nil {
		return
	}
	for _, row := range resp.Rows {
		if row == nil {
			continue
		}
		rehydrateRow(row, resp.Schema.Fields)
	}
}

func rehydrateRow(row *internaltypes.TableRow, fields []*bigqueryv2.TableFieldSchema) {
	for idx, cell := range row.F {
		var field *bigqueryv2.TableFieldSchema
		if idx < len(fields) {
			field = fields[idx]
		}
		rehydrateCell(cell, field)
	}
}

func rehydrateCell(cell *internaltypes.TableCell, field *bigqueryv2.TableFieldSchema) {
	if cell == nil {
		return
	}
	if field != nil && cell.Name == "" {
		cell.Name = field.Name
	}
	switch v := cell.V.(type) {
	case map[string]interface{}:
		// Nested record: {"f":[{...cells...}]}
		rawCells, ok := v["f"].([]interface{})
		if !ok {
			return
		}
		cells := make([]*internaltypes.TableCell, 0, len(rawCells))
		for _, raw := range rawCells {
			c := &internaltypes.TableCell{}
			if cm, ok := raw.(map[string]interface{}); ok {
				c.V = cm["v"]
			}
			cells = append(cells, c)
		}
		var subFields []*bigqueryv2.TableFieldSchema
		if field != nil {
			subFields = field.Fields
		}
		for idx, sub := range cells {
			var subField *bigqueryv2.TableFieldSchema
			if idx < len(subFields) {
				subField = subFields[idx]
			}
			rehydrateCell(sub, subField)
		}
		cell.V = internaltypes.TableRow{F: cells}
	case []interface{}:
		// Repeated field: [{...cells...}, ...]; each element is shaped by
		// the same field schema (itself for scalars, field.Fields for
		// repeated records).
		cells := make([]*internaltypes.TableCell, 0, len(v))
		for _, raw := range v {
			c := &internaltypes.TableCell{}
			if cm, ok := raw.(map[string]interface{}); ok {
				c.V = cm["v"]
			} else {
				c.V = raw
			}
			cells = append(cells, c)
		}
		for _, sub := range cells {
			rehydrateCell(sub, field)
		}
		cell.V = cells
	}
}

// cacheStore persists a freshly computed result with its dependency
// snapshot. It is best-effort: any failure is logged and the (already
// computed) response is still returned; the next identical request simply
// recomputes and retries storing.
func (s *Server) cacheStore(ctx context.Context, tx *connection.Tx, key, projectID string, response *internaltypes.QueryResponse, deps []metadata.Dependency) {
	if err := tx.MetadataRepoMode(); err != nil {
		logger.Logger(ctx).Warn("query cache: failed to switch metadata mode", zap.Error(err))
		return
	}
	// Never persist the per-request job reference or the cache-hit marker
	// (a stored entry describes a freshly computed result).
	stored := *response
	stored.JobReference = nil
	stored.CacheHit = false
	responseJSON, err := json.Marshal(&stored)
	if err != nil {
		logger.Logger(ctx).Warn("query cache: failed to encode response", zap.Error(err))
		return
	}
	depsJSON, err := json.Marshal(deps)
	if err != nil {
		logger.Logger(ctx).Warn("query cache: failed to encode dependencies", zap.Error(err))
		return
	}
	if err := s.metaRepo.PutQueryCacheEntry(ctx, tx.Tx(), &metadata.QueryCacheEntry{
		CacheKey:     key,
		ProjectID:    projectID,
		Response:     string(responseJSON),
		Dependencies: string(depsJSON),
		CreatedAt:    time.Now().Unix(),
	}); err != nil {
		logger.Logger(ctx).Warn("query cache: failed to store entry", zap.Error(err))
	}
}

// bumpTableVersion advances the version of one table or view after a
// content or schema change. If the version table cannot be updated, the
// whole cache is cleared so no stale result can ever be served.
func (s *Server) bumpTableVersion(ctx context.Context, tx *connection.Tx, projectID, datasetID, tableID string) {
	if projectID == "" || datasetID == "" || tableID == "" {
		return
	}
	s.bumpVersion(ctx, tx, func() error {
		return s.metaRepo.BumpTableVersion(ctx, tx.Tx(), projectID, datasetID, tableID)
	}, fmt.Sprintf("table %s.%s.%s", projectID, datasetID, tableID))
}

// bumpRoutineVersion advances the version of one routine after its
// definition changes. body (when non-empty) records the SQL definition so
// routine references can be expanded for dependencies.
func (s *Server) bumpRoutineVersion(ctx context.Context, tx *connection.Tx, projectID, datasetID, routineID, body string) {
	if projectID == "" || datasetID == "" || routineID == "" {
		return
	}
	s.bumpVersion(ctx, tx, func() error {
		return s.metaRepo.BumpRoutineVersion(ctx, tx.Tx(), projectID, datasetID, routineID, body)
	}, fmt.Sprintf("routine %s.%s.%s", projectID, datasetID, routineID))
}

func (s *Server) bumpVersion(ctx context.Context, tx *connection.Tx, bump func() error, what string) {
	if err := tx.MetadataRepoMode(); err != nil {
		logger.Logger(ctx).Error("query cache: metadata mode switch failed; clearing cache", zap.String("object", what), zap.Error(err))
		s.clearQueryCache(ctx, tx)
		return
	}
	if err := bump(); err != nil {
		logger.Logger(ctx).Error("query cache: version bump failed; clearing cache to preserve correctness", zap.String("object", what), zap.Error(err))
		s.clearQueryCache(ctx, tx)
	}
}

// clearQueryCache drops every cached entry. Best-effort: failures are
// logged (the in-flight change is still committed; future version bumps
// keep invalidating entries).
func (s *Server) clearQueryCache(ctx context.Context, tx *connection.Tx) {
	if err := tx.MetadataRepoMode(); err != nil {
		logger.Logger(ctx).Error("query cache: metadata mode switch failed while clearing cache", zap.Error(err))
		return
	}
	if err := s.metaRepo.ClearQueryCache(ctx, tx.Tx()); err != nil {
		logger.Logger(ctx).Error("query cache: failed to clear cache", zap.Error(err))
	}
}

// invalidateForQuery bumps versions for every object a successfully
// executed statement changed. It combines SQL-text derived targets
// (DML targets, ALTER/TRUNCATE, TEMP-less CREATE/DROP, routine DDL and
// CALLed procedures) with the engine's ChangedCatalog (which reports DDL
// additions/deletions with full name paths and routine bodies).
func (s *Server) invalidateForQuery(
	ctx context.Context,
	tx *connection.Tx,
	defaultProjectID, defaultDatasetID, sql string,
	changed *googlesqlite.ChangedCatalog,
) {
	s.invalidateForQueryImpl(ctx, tx, defaultProjectID, defaultDatasetID, sql, changed, map[string]bool{})
}

func (s *Server) invalidateForQueryImpl(
	ctx context.Context,
	tx *connection.Tx,
	defaultProjectID, defaultDatasetID, sql string,
	changed *googlesqlite.ChangedCatalog,
	visitedRoutines map[string]bool,
) {
	analysis := querycache.Analyze(sql)
	for _, target := range analysis.WriteTargets {
		projectID, datasetID, name, ok := qualifyRef(target.Parts, defaultProjectID, defaultDatasetID)
		if !ok {
			continue
		}
		switch target.Kind {
		case querycache.TargetTable:
			s.bumpTableVersion(ctx, tx, projectID, datasetID, name)
		case querycache.TargetRoutine:
			s.bumpRoutineVersion(ctx, tx, projectID, datasetID, name, "")
			// A CALLed procedure (or invoked function whose definition we
			// know) can read and write tables: invalidate everything its
			// body touches. If we cannot see the definition we cannot
			// reason about it, so the whole cache is cleared.
			s.invalidateForRoutineBody(ctx, tx, projectID, datasetID, name, visitedRoutines)
		}
	}
	for _, dsTarget := range analysis.DropDatasets {
		projectID := dsTarget.ProjectID
		if projectID == "" {
			projectID = defaultProjectID
		}
		if projectID == "" || dsTarget.DatasetID == "" {
			s.clearQueryCache(ctx, tx)
			continue
		}
		project, err := s.metaRepo.FindProject(ctx, projectID)
		if err != nil || project == nil {
			// Cannot enumerate what the drop removes: clear the cache
			// rather than risk a stale entry.
			s.clearQueryCache(ctx, tx)
			continue
		}
		dataset := project.Dataset(dsTarget.DatasetID)
		if dataset == nil {
			// Dataset unknown to the metadata catalog (e.g. created
			// entirely through SQL) so its tables cannot be enumerated;
			// clear the cache to be safe.
			s.clearQueryCache(ctx, tx)
			continue
		}
		for _, table := range dataset.Tables() {
			s.bumpTableVersion(ctx, tx, projectID, dsTarget.DatasetID, table.ID)
		}
		for _, routine := range dataset.Routines() {
			s.bumpRoutineVersion(ctx, tx, projectID, dsTarget.DatasetID, routine.ID, "")
		}
	}
	if changed != nil {
		if changed.Table != nil {
			for _, spec := range changed.Table.Added {
				s.bumpSpecTable(ctx, tx, spec.NamePath)
			}
			for _, spec := range changed.Table.Updated {
				s.bumpSpecTable(ctx, tx, spec.NamePath)
			}
			for _, spec := range changed.Table.Deleted {
				s.bumpSpecTable(ctx, tx, spec.NamePath)
			}
		}
		if changed.Function != nil {
			for _, spec := range changed.Function.Added {
				projectID, datasetID, name := routineNamePath(spec.NamePath, defaultProjectID, defaultDatasetID)
				s.bumpRoutineVersion(ctx, tx, projectID, datasetID, name, spec.Body)
			}
			for _, spec := range changed.Function.Deleted {
				projectID, datasetID, name := routineNamePath(spec.NamePath, defaultProjectID, defaultDatasetID)
				s.bumpRoutineVersion(ctx, tx, projectID, datasetID, name, "")
			}
		}
	}
}

// invalidateForRoutineBody re-runs invalidation against the stored SQL body
// of a routine that was itself just bumped (typically a CALLed procedure).
// A routine whose definition is unavailable forces a full cache clear.
func (s *Server) invalidateForRoutineBody(ctx context.Context, tx *connection.Tx, projectID, datasetID, routineID string, visited map[string]bool) {
	key := projectID + "|" + datasetID + "|" + routineID
	if visited[key] {
		return
	}
	visited[key] = true
	if err := tx.MetadataRepoMode(); err != nil {
		s.clearQueryCache(ctx, tx)
		return
	}
	_, body, found, err := s.metaRepo.FindRoutineVersion(ctx, tx.Tx(), projectID, datasetID, routineID)
	if err != nil {
		s.clearQueryCache(ctx, tx)
		return
	}
	if !found {
		// The routine exists for the engine but is unknown to the version
		// registry (e.g. created before the cache existed): we cannot prove
		// which cached results it might affect.
		s.clearQueryCache(ctx, tx)
		return
	}
	if body == "" {
		// JavaScript routines cannot reference tables; nothing to expand.
		return
	}
	s.invalidateForQueryImpl(ctx, tx, projectID, datasetID, body, nil, visited)
}

func (s *Server) bumpSpecTable(ctx context.Context, tx *connection.Tx, namePath []string) {
	projectID, datasetID, tableID, ok := tableNamePath(namePath)
	if !ok {
		return
	}
	s.bumpTableVersion(ctx, tx, projectID, datasetID, tableID)
}

// tableNamePath splits a 3-segment [project, dataset, table] name path.
func tableNamePath(namePath []string) (string, string, string, bool) {
	if len(namePath) != 3 {
		return "", "", "", false
	}
	return namePath[0], namePath[1], namePath[2], true
}

// routineNamePath normalizes a routine name path ([dataset, routine] or
// [project, dataset, routine]) into a fully qualified triple.
func routineNamePath(namePath []string, defaultProjectID, defaultDatasetID string) (string, string, string) {
	switch len(namePath) {
	case 3:
		return namePath[0], namePath[1], namePath[2]
	case 2:
		return defaultProjectID, namePath[0], namePath[1]
	default:
		return defaultProjectID, defaultDatasetID, strings.Join(namePath, ".")
	}
}

// qualifyRef resolves a 1-3 part reference as written in a statement into a
// fully qualified triple, using the statement's effective project and
// default dataset for unqualified forms.
func qualifyRef(parts []string, defaultProjectID, defaultDatasetID string) (string, string, string, bool) {
	switch len(parts) {
	case 3:
		return parts[0], parts[1], parts[2], true
	case 2:
		if defaultProjectID == "" {
			return "", "", "", false
		}
		return defaultProjectID, parts[0], parts[1], true
	case 1:
		if defaultProjectID == "" || defaultDatasetID == "" {
			return "", "", "", false
		}
		return defaultProjectID, defaultDatasetID, parts[0], true
	default:
		return "", "", "", false
	}
}

// resolveDependencies builds the dependency snapshot that validates a cached
// result: every persistent table/view the statement reads (transitively,
// expanding views and SQL routines) and every catalog routine it calls,
// each paired with its current version. It returns ok=false when a
// non-CTE reference cannot be resolved to a persistent object, which makes
// the statement ineligible for caching rather than risking a stale entry.
func (s *Server) resolveDependencies(
	ctx context.Context,
	tx *connection.Tx,
	defaultProjectID, defaultDatasetID string,
	analysis *querycache.Analysis,
) ([]metadata.Dependency, bool) {
	r := &depResolver{
		s:              s,
		ctx:            ctx,
		tx:             tx,
		defaultProject: defaultProjectID,
		defaultDataset: defaultDatasetID,
		cteNames:       analysis.CTENames,
		deps:           map[string]*metadata.Dependency{},
		visitedTables:  map[string]bool{},
		visitedRoutine: map[string]bool{},
	}
	for _, ref := range analysis.ReadRefs {
		if ref.Wildcard {
			return nil, false
		}
		if ref.Kind == querycache.RefCall {
			if !r.addRoutineFromRef(ref.Parts) {
				return nil, false
			}
			continue
		}
		if !r.addTableFromRef(ref.Parts) {
			return nil, false
		}
	}
	deps := make([]metadata.Dependency, 0, len(r.deps))
	for _, dep := range r.deps {
		deps = append(deps, *dep)
	}
	return deps, true
}

type depResolver struct {
	s              *Server
	ctx            context.Context
	tx             *connection.Tx
	defaultProject string
	defaultDataset string
	cteNames       map[string]struct{}
	deps           map[string]*metadata.Dependency
	visitedTables  map[string]bool
	visitedRoutine map[string]bool
}

func depKey(kind, projectID, datasetID, name string) string {
	return kind + "|" + projectID + "|" + datasetID + "|" + name
}

// addTableFromRef resolves one FROM/JOIN reference to a persistent table or
// view and records its version, expanding a view's definition (and the
// tables/routines it references) recursively.
func (r *depResolver) addTableFromRef(parts []string) bool {
	if len(parts) == 1 {
		if _, isCTE := r.cteNames[lower(parts[0])]; isCTE {
			return true
		}
	}
	projectID, datasetID, tableID, table, ok := r.resolveTable(parts)
	if !ok {
		// An unqualified name that is not a CTE and does not resolve to a
		// persistent table may be a temp table or something the analysis
		// cannot track: do not cache.
		return false
	}
	key := depKey(metadata.DependencyTable, projectID, datasetID, tableID)
	if r.visitedTables[key] {
		return true
	}
	r.visitedTables[key] = true
	if err := r.tx.MetadataRepoMode(); err != nil {
		return false
	}
	version, err := r.s.metaRepo.EnsureTableVersion(r.ctx, r.tx.Tx(), projectID, datasetID, tableID)
	if err != nil {
		logger.Logger(r.ctx).Warn("query cache: failed to snapshot table version", zap.Error(err))
		return false
	}
	r.deps[key] = &metadata.Dependency{
		Kind:      metadata.DependencyTable,
		ProjectID: projectID,
		DatasetID: datasetID,
		Name:      tableID,
		Version:   version,
	}
	// Views depend on the objects their definition queries.
	content, err := table.Content()
	if err != nil {
		logger.Logger(r.ctx).Warn("query cache: failed to read table content", zap.Error(err))
		return false
	}
	var viewQuery string
	switch {
	case content.View != nil:
		viewQuery = content.View.Query
	case content.MaterializedView != nil:
		viewQuery = content.MaterializedView.Query
	}
	if viewQuery != "" {
		if !r.expandSQL(viewQuery, projectID, datasetID) {
			return false
		}
	}
	return true
}

// addRoutineFromRef resolves a routine-call reference and records its
// version; SQL routine bodies are expanded for table/routine references.
// Unresolved calls are treated as built-in functions and are not
// dependencies (the cache only tracks catalog routines).
func (r *depResolver) addRoutineFromRef(parts []string) bool {
	projectID, datasetID, routineID, ok := qualifyRef(parts, r.defaultProject, r.defaultDataset)
	if !ok {
		// A call without a default dataset (e.g. `f()` with no default
		// dataset) cannot be a catalog routine; ignore it like a built-in.
		return true
	}
	key := depKey(metadata.DependencyRoutine, projectID, datasetID, routineID)
	if r.visitedRoutine[key] {
		return true
	}
	if err := r.tx.MetadataRepoMode(); err != nil {
		return false
	}
	version, body, found, err := r.s.metaRepo.FindRoutineVersion(r.ctx, r.tx.Tx(), projectID, datasetID, routineID)
	if err != nil {
		logger.Logger(r.ctx).Warn("query cache: failed to look up routine version", zap.Error(err))
		return false
	}
	if !found {
		// Built-in function or a routine the registry has never seen;
		// nothing the cache can invalidate on.
		return true
	}
	r.visitedRoutine[key] = true
	r.deps[key] = &metadata.Dependency{
		Kind:      metadata.DependencyRoutine,
		ProjectID: projectID,
		DatasetID: datasetID,
		Name:      routineID,
		Version:   version,
	}
	if body != "" {
		if !r.expandSQL(body, projectID, datasetID) {
			return false
		}
	}
	return true
}

// expandSQL analyzes one SQL definition (view query, routine body) in the
// context of the defining object and adds its tables and routines as
// dependencies of the cached result. It reports false when the definition
// references an object that cannot be resolved to a persistent table, in
// which case the enclosing statement must not be cached.
func (r *depResolver) expandSQL(sql, defProjectID, defDatasetID string) bool {
	// The definition owns its name-resolution scope: its CTEs apply, the
	// enclosing query's CTEs do not.
	sub := &depResolver{
		s:              r.s,
		ctx:            r.ctx,
		tx:             r.tx,
		defaultProject: defProjectID,
		defaultDataset: defDatasetID,
		cteNames:       map[string]struct{}{},
		deps:           r.deps,
		visitedTables:  r.visitedTables,
		visitedRoutine: r.visitedRoutine,
	}
	analysis := querycache.Analyze(sql)
	for name := range analysis.CTENames {
		sub.cteNames[name] = struct{}{}
	}
	for _, ref := range analysis.ReadRefs {
		if ref.Wildcard {
			return false
		}
		if ref.Kind == querycache.RefCall {
			// unresolved routine calls are built-ins: safe to ignore
			_ = sub.addRoutineFromRef(ref.Parts)
			continue
		}
		if !sub.addTableFromRef(ref.Parts) {
			return false
		}
	}
	return true
}

// resolveTable maps a 1-3 part reference to a persistent table in the
// metadata catalog.
func (r *depResolver) resolveTable(parts []string) (string, string, string, *metadata.Table, bool) {
	var projectID, datasetID, tableID string
	switch len(parts) {
	case 3:
		projectID, datasetID, tableID = parts[0], parts[1], parts[2]
	case 2:
		if r.defaultProject == "" {
			return "", "", "", nil, false
		}
		projectID, datasetID, tableID = r.defaultProject, parts[0], parts[1]
	case 1:
		if r.defaultProject == "" || r.defaultDataset == "" {
			return "", "", "", nil, false
		}
		projectID, datasetID, tableID = r.defaultProject, r.defaultDataset, parts[0]
	default:
		return "", "", "", nil, false
	}
	project, err := r.s.metaRepo.FindProject(r.ctx, projectID)
	if err != nil || project == nil {
		return "", "", "", nil, false
	}
	dataset := project.Dataset(datasetID)
	if dataset == nil {
		return "", "", "", nil, false
	}
	table := dataset.Table(tableID)
	if table == nil {
		return "", "", "", nil, false
	}
	return projectID, datasetID, tableID, table, true
}

func lower(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		out = append(out, r)
	}
	return string(out)
}
