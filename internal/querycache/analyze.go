// Package querycache provides the SQL text analysis used by the
// BigQuery-compatible query result cache: it decides whether a statement is
// eligible for caching, extracts the tables and routines a statement reads
// (so a stored result can be invalidated when any of them changes) and the
// tables/routines a statement writes (so those entries can be invalidated
// after DML/DDL), and builds the stable cache key from the request context.
//
// The analysis is deliberately conservative: every construct it cannot prove
// to be a read-only, deterministic reference to a persistent object either
// marks the statement as non-cacheable or is reported as an unresolved
// reference, which the caller treats as "do not cache". It never produces a
// false "cacheable" for a statement that performs writes or depends on
// session-scoped or non-deterministic state.
package querycache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// RefKind identifies what an extracted reference points at.
type RefKind int

const (
	// RefTable is a table or view appearing in a FROM/JOIN position.
	RefTable RefKind = iota
	// RefCall is a path immediately followed by '(' (a routine call,
	// including table-valued functions used in FROM position).
	RefCall
)

// Ref is one table- or routine-like reference extracted from the SQL text.
type Ref struct {
	// Parts are the (up to three) qualified name segments, as written in
	// the statement (a single backtick-quoted "p.d.t" is split into its
	// three segments). Case is preserved for catalog resolution.
	Parts []string
	// Kind is RefTable (FROM/JOIN factor) or RefCall (path followed by '(').
	Kind RefKind
	// Wildcard is set when the reference contains a '*' (e.g. `ds.*`),
	// which makes the statement non-cacheable.
	Wildcard bool
}

// TargetKind distinguishes table targets from routine targets.
type TargetKind int

const (
	TargetTable TargetKind = iota
	TargetRoutine
)

// Target is a table or routine that a statement modifies (DDL/DML target).
type Target struct {
	Parts []string
	Kind  TargetKind
}

// DatasetTarget names a dataset that a statement drops.
type DatasetTarget struct {
	ProjectID string
	DatasetID string
}

// Analysis holds the result of analyzing one (possibly multi-statement)
// SQL text.
type Analysis struct {
	// Cacheable reports whether the text is a single read-only SELECT that
	// contains no construct the cache cannot reason about. It does NOT take
	// request options into account (useQueryCache, destination tables, ...):
	// the caller combines those with this flag.
	Cacheable bool
	// NotCacheableReason explains why Cacheable is false (for logging).
	NotCacheableReason string
	// ReadRefs are the tables/views in FROM/JOIN positions and every
	// routine-call-like path. Resolution against the catalog is left to the
	// caller.
	ReadRefs []Ref
	// WriteTargets are the tables/routines modified by DDL/DML statements.
	WriteTargets []Target
	// DropDatasets are datasets that statements drop (all tables and
	// routines in them must be treated as changed).
	DropDatasets []DatasetTarget
	// CTENames are the single-part names introduced by `name AS (` CTE
	// definitions (lowercased). An unqualified read ref matching one of
	// these is not a table reference.
	CTENames map[string]struct{}
}

// token kinds
const (
	tkWord   = iota // bare word (keyword/identifier), Text keeps original case
	tkQuoted        // backtick-quoted identifier, Text is the inner content
	tkPunct         // single-character punctuation
)

type token struct {
	kind int
	text string
}

func (t token) lower() string { return strings.ToLower(t.text) }

// isWord reports whether t is a word token equal (case-insensitively) to s.
func (t token) isWord(s string) bool { return t.kind == tkWord && strings.EqualFold(t.text, s) }

// tokenize splits GoogleSQL text into tokens, masking out string literals,
// numeric literals and comments so keyword/reference scanning never sees
// their contents. Backtick-quoted identifiers are preserved.
func tokenize(sql string) []token {
	tokens := []token{}
	r := []rune(sql)
	i := 0
	n := len(r)
	for i < n {
		c := r[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && r[i+1] == '-':
			// line comment
			i += 2
			for i < n && r[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && r[i+1] == '*':
			// block comment (BigQuery allows nested block comments)
			depth := 1
			i += 2
			for i < n && depth > 0 {
				if i+1 < n && r[i] == '/' && r[i+1] == '*' {
					depth++
					i += 2
					continue
				}
				if i+1 < n && r[i] == '*' && r[i+1] == '/' {
					depth--
					i += 2
					continue
				}
				i++
			}
		case c == '\'':
			i = skipString(r, i, '\'')
		case c == '"':
			// GoogleSQL treats double quotes as string delimiters.
			i = skipString(r, i, '"')
		case c == '`':
			var sb strings.Builder
			i++
			closed := false
			for i < n {
				ch := r[i]
				if ch == '\\' && i+1 < n {
					sb.WriteRune(r[i+1])
					i += 2
					continue
				}
				if ch == '`' {
					if i+1 < n && r[i+1] == '`' {
						// doubled backtick is a literal backtick
						sb.WriteRune('`')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				sb.WriteRune(ch)
				i++
			}
			if !closed {
				// unterminated quote: return what we have; the statement
				// will fail to execute anyway
			}
			tokens = append(tokens, token{kind: tkQuoted, text: sb.String()})
		case isIdentStart(c):
			start := i
			i++
			for i < n && isIdentPart(r[i]) {
				i++
			}
			tokens = append(tokens, token{kind: tkWord, text: string(r[start:i])})
		case c >= '0' && c <= '9':
			// numeric literal: mask it entirely; it never participates in
			// references or keywords
			i++
			for i < n {
				ch := r[i]
				if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') ||
					ch == '.' || ch == 'x' || ch == 'X' || ch == 'e' || ch == 'E' || ch == '+' || ch == '-' {
					i++
					continue
				}
				break
			}
		default:
			tokens = append(tokens, token{kind: tkPunct, text: string(c)})
			i++
		}
	}
	return tokens
}

// skipString consumes a single/double quoted string starting at the quote
// rune and returns the index just past the closing quote. Both backslash
// escapes and doubled quotes ('') are honored.
func skipString(r []rune, start int, quote rune) int {
	i := start + 1
	for i < len(r) {
		c := r[i]
		if c == '\\' && i+1 < len(r) {
			i += 2
			continue
		}
		if c == quote {
			if i+1 < len(r) && r[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

func isIdentStart(c rune) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c rune) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

// parsePath parses a qualified name (segments of words/quoted idents joined
// by '.') starting at tokens[start]. It returns the up-to-three segments
// (quoted segments may themselves contain dots, which are split out), the
// index past the path, whether the path is immediately followed by '(', and
// whether a '*' wildcard was encountered in or right after the path.
func parsePath(tokens []token, start int) (parts []string, next int, isCall bool, wildcard bool, ok bool) {
	i := start
	for {
		if i >= len(tokens) {
			return nil, start, false, false, false
		}
		t := tokens[i]
		if t.kind == tkPunct && t.text == "*" {
			// leading or standalone '*'
			return parts, i + 1, false, true, len(parts) > 0
		}
		if t.kind != tkWord && t.kind != tkQuoted {
			break
		}
		segments := []string{t.text}
		if t.kind == tkQuoted {
			if strings.Contains(t.text, "*") {
				wildcard = true
			}
			segments = strings.Split(t.text, ".")
		}
		parts = append(parts, segments...)
		i++
		// optional continuation: '.segment' or '.*'
		if i < len(tokens) && tokens[i].kind == tkPunct && tokens[i].text == "." {
			i++
			if i < len(tokens) && tokens[i].kind == tkPunct && tokens[i].text == "*" {
				return parts, i + 1, false, true, true
			}
			continue
		}
		break
	}
	if len(parts) == 0 {
		return nil, start, false, false, false
	}
	if i < len(tokens) && tokens[i].kind == tkPunct && tokens[i].text == "(" {
		isCall = true
	}
	return parts, i, isCall, wildcard, true
}

// keywords that terminate a FROM-clause factor list when seen at the
// factor's parenthesis depth
var fromClauseEndKeywords = map[string]struct{}{
	"where": {}, "group": {}, "having": {}, "order": {}, "limit": {},
	"offset": {}, "union": {}, "except": {}, "intersect": {}, "window": {},
	"qualify": {}, "values": {},
}

// non-deterministic BigQuery functions that prevent result caching. The
// CURRENT_* / SESSION_USER / CURRENT_USER forms may be written without
// parentheses; the others require a call.
var nondeterministicBareWords = map[string]struct{}{
	"current_date":      {},
	"current_time":      {},
	"current_timestamp": {},
	"current_datetime":  {},
	"session_user":      {},
	"current_user":      {},
}

var nondeterministicCallWords = map[string]struct{}{
	"now":            {},
	"rand":           {},
	"generate_uuid":  {},
	"user":           {},
}

// Analyze parses the given GoogleSQL text and returns its cache analysis.
func Analyze(sql string) *Analysis {
	tokens := tokenize(sql)
	a := &Analysis{Cacheable: true, CTENames: map[string]struct{}{}}

	collectCTENames(tokens, a)
	collectReadRefs(tokens, a)
	collectCalls(tokens, a)
	a.ReadRefs = dedupeRefs(a.ReadRefs)

	// Statement-level classification and write-target extraction.
	statements := splitStatements(tokens)
	if len(statements) == 0 {
		a.Cacheable = false
		a.NotCacheableReason = "empty statement"
		return a
	}
	if len(statements) > 1 {
		a.Cacheable = false
		a.NotCacheableReason = "multi-statement script"
	}
	for _, stmt := range statements {
		extractWriteTargets(stmt, a)
	}
	if a.Cacheable && !isReadOnlyStatement(statements[0]) {
		a.Cacheable = false
		a.NotCacheableReason = "statement is not a read-only SELECT"
	}

	// Text-level disqualifiers that apply regardless of statement shape.
	for idx, t := range tokens {
		if t.kind != tkWord {
			continue
		}
		w := t.lower()
		if w == "temp" || w == "temporary" {
			a.Cacheable = false
			a.NotCacheableReason = "references TEMP/TEMPORARY objects"
		}
		if w == "information_schema" {
			a.Cacheable = false
			a.NotCacheableReason = "references INFORMATION_SCHEMA"
		}
		if _, exists := nondeterministicBareWords[w]; exists {
			a.Cacheable = false
			a.NotCacheableReason = "uses non-deterministic function " + w
		}
		if _, exists := nondeterministicCallWords[w]; exists {
			if idx+1 < len(tokens) && tokens[idx+1].kind == tkPunct && tokens[idx+1].text == "(" {
				a.Cacheable = false
				a.NotCacheableReason = "uses non-deterministic function " + w
			}
		}
	}
	// @@system_variable references depend on session state.
	for idx := 0; idx+1 < len(tokens); idx++ {
		t1, t2 := tokens[idx], tokens[idx+1]
		if t1.kind == tkPunct && t1.text == "@" && t2.kind == tkPunct && t2.text == "@" {
			a.Cacheable = false
			a.NotCacheableReason = "references session system variables (@@)"
		}
	}
	for _, ref := range a.ReadRefs {
		if ref.Wildcard {
			a.Cacheable = false
			a.NotCacheableReason = "references wildcard tables"
		}
	}
	return a
}

// collectCTENames records every single-part name immediately followed by
// `AS (` — the shape of a CTE definition. Over-collection is harmless: a
// name that is not actually a CTE only causes an unqualified table
// reference to be (conservatively) treated as a CTE by the caller, which
// never returns stale data for real tables.
func collectCTENames(tokens []token, a *Analysis) {
	for i := 0; i+2 < len(tokens); i++ {
		t := tokens[i]
		if t.kind != tkWord && t.kind != tkQuoted {
			continue
		}
		if !tokens[i+1].isWord("as") {
			continue
		}
		if tokens[i+2].kind != tkPunct || tokens[i+2].text != "(" {
			continue
		}
		name := t.text
		if t.kind == tkQuoted && strings.Contains(name, ".") {
			// CTE names are always single-part; ignore qualified forms.
			continue
		}
		a.CTENames[strings.ToLower(name)] = struct{}{}
	}
}

// collectReadRefs walks FROM/JOIN clauses at every parenthesis depth and
// records the table factors (and TVF calls) they name.
func collectReadRefs(tokens []token, a *Analysis) {
	depth := 0
	factorDepth := -1
	expectFactor := false
	inFromList := false
	for i := 0; i < len(tokens); i++ {
		t := tokens[i]
		if t.kind == tkPunct {
			switch t.text {
			case "(":
				if expectFactor {
					// subquery factor: its interior is scanned when the
					// walk reaches the inner FROM keyword. The subquery
					// itself is a table factor, so a comma at the same
					// depth afterwards introduces another factor.
					expectFactor = false
					inFromList = true
					depth++
					continue
				}
				depth++
				continue
			case ")":
				depth--
				if depth < factorDepth {
					factorDepth = -1
					inFromList = false
					expectFactor = false
				}
				continue
			case ",":
				if inFromList && depth == factorDepth {
					expectFactor = true
				}
				continue
			}
		}
		if t.kind == tkWord {
			w := t.lower()
			if w == "from" || w == "join" {
				expectFactor = true
				factorDepth = depth
				inFromList = false
				continue
			}
			if expectFactor && w == "unnest" {
				// UNNEST(array) is an expression, not a table reference
				expectFactor = false
				continue
			}
			if inFromList && depth == factorDepth {
				if _, end := fromClauseEndKeywords[w]; end {
					inFromList = false
					expectFactor = false
					continue
				}
			}
		}
		if expectFactor {
			parts, _, isCall, wildcard, ok := parsePath(tokens, i)
			if !ok {
				// not a name factor (e.g. a keyword-construct); drop out of
				// factor state and keep scanning
				expectFactor = false
				continue
			}
			a.ReadRefs = append(a.ReadRefs, Ref{
				Parts:    parts,
				Kind:     refKindForCall(isCall),
				Wildcard: wildcard,
			})
			expectFactor = false
			inFromList = true
		}
	}
}

func refKindForCall(isCall bool) RefKind {
	if isCall {
		return RefCall
	}
	return RefTable
}

// callKeywordSkips are keywords that can directly precede a parenthesised
// expression (e.g. `FROM (subquery)`, `name AS (cte body)`, `IN (...)`). They
// are never routine calls.
var callKeywordSkips = map[string]struct{}{
	"as": {}, "from": {}, "in": {}, "then": {}, "else": {},
	"when": {}, "return": {}, "using": {}, "on": {},
}

// collectCalls records every qualified path immediately followed by '(' as
// a possible routine call. Calls that do not resolve to a catalog routine
// (built-ins, keywords, casts) are ignored by the caller; unresolved calls
// never make a statement non-cacheable on their own.
func collectCalls(tokens []token, a *Analysis) {
	for i := 0; i < len(tokens); i++ {
		t := tokens[i]
		if t.kind != tkWord && t.kind != tkQuoted {
			continue
		}
		// A path segment directly preceded by '.' is the tail of a
		// qualified call (whose head is handled when the loop reaches it).
		if i > 0 {
			if prev := tokens[i-1]; prev.kind == tkPunct && prev.text == "." {
				continue
			}
		}
		if t.kind == tkWord {
			if _, skip := callKeywordSkips[t.lower()]; skip {
				continue
			}
		}
		parts, _, isCall, wildcard, ok := parsePath(tokens, i)
		if !ok || !isCall || wildcard {
			continue
		}
		a.ReadRefs = append(a.ReadRefs, Ref{Parts: parts, Kind: RefCall})
	}
}

// dedupeRefs drops references identical in kind and parts, preserving order.
func dedupeRefs(refs []Ref) []Ref {
	seen := map[string]bool{}
	out := make([]Ref, 0, len(refs))
	for _, ref := range refs {
		key := refKey(ref)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, ref)
	}
	return out
}

func refKey(ref Ref) string {
	return fmt.Sprintf("%d|%s", ref.Kind, strings.Join(ref.Parts, "."))
}

// splitStatements breaks tokens into statements at depth-0 semicolons.
func splitStatements(tokens []token) [][]token {
	var (
		out        [][]token
		cur        []token
		depth      int
	)
	for _, t := range tokens {
		if t.kind == tkPunct {
			switch t.text {
			case "(":
				depth++
			case ")":
				depth--
			case ";":
				if depth == 0 {
					if len(cur) > 0 {
						out = append(out, cur)
						cur = nil
					}
					continue
				}
			}
		}
		cur = append(cur, t)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// rootKeyword returns the first keyword of a statement, skipping over a
// leading WITH clause and its CTE definitions (`WITH x AS (...), y AS (...)`).
func rootKeyword(stmt []token) (string, int) {
	if len(stmt) == 0 || stmt[0].kind != tkWord {
		return "", 0
	}
	i := 0
	if !stmt[i].isWord("with") {
		return stmt[i].lower(), i
	}
	// WITH [RECURSIVE] <name> AS ( <...> ) [, <name> AS ( <...> ) ] ...
	i++
	if i < len(stmt) && stmt[i].isWord("recursive") {
		i++
	}
	for i < len(stmt) {
		// expect a CTE name
		if stmt[i].kind != tkWord && stmt[i].kind != tkQuoted {
			break
		}
		i++
		// expect AS
		if i >= len(stmt) || !stmt[i].isWord("as") {
			break
		}
		i++
		// expect '(' and skip to its matching ')'
		if i >= len(stmt) || stmt[i].kind != tkPunct || stmt[i].text != "(" {
			break
		}
		depth := 0
		for i < len(stmt) {
			if stmt[i].kind == tkPunct && stmt[i].text == "(" {
				depth++
			}
			if stmt[i].kind == tkPunct && stmt[i].text == ")" {
				depth--
				i++
				if depth == 0 {
					break
				}
				continue
			}
			i++
		}
		// either a comma (another CTE) or the statement keyword follows
		if i < len(stmt) && stmt[i].kind == tkPunct && stmt[i].text == "," {
			i++
			continue
		}
		break
	}
	if i >= len(stmt) || stmt[i].kind != tkWord {
		return "", i
	}
	return stmt[i].lower(), i
}

// isReadOnlyStatement reports whether the statement's root keyword is
// SELECT (possibly prefixed by WITH CTEs). Every other statement type is
// treated as a potential side effect and bypasses the cache.
func isReadOnlyStatement(stmt []token) bool {
	root, _ := rootKeyword(stmt)
	return root == "select"
}

// extractWriteTargets appends the DDL/DML targets of one statement to a.
func extractWriteTargets(stmt []token, a *Analysis) {
	root, i := rootKeyword(stmt)
	switch root {
	case "insert":
		i++
		if i < len(stmt) && (stmt[i].isWord("into") || stmt[i].isWord("overwrite")) {
			i++
		}
		addTableTarget(stmt, i, a)
	case "update":
		i++
		addTableTarget(stmt, i, a)
	case "delete":
		i++
		if i < len(stmt) && stmt[i].isWord("from") {
			i++
		}
		addTableTarget(stmt, i, a)
	case "merge":
		i++
		if i < len(stmt) && stmt[i].isWord("into") {
			i++
		}
		addTableTarget(stmt, i, a)
	case "truncate":
		i++
		if i < len(stmt) && stmt[i].isWord("table") {
			i++
		}
		addTableTarget(stmt, i, a)
	case "optimize":
		i++
		if i < len(stmt) && stmt[i].isWord("table") {
			i++
		}
		addTableTarget(stmt, i, a)
	case "alter":
		i++
		// ALTER [MATERIALIZED] {TABLE|VIEW} ...
		if i < len(stmt) && stmt[i].isWord("materialized") {
			i++
		}
		if i < len(stmt) && (stmt[i].isWord("table") || stmt[i].isWord("view") || stmt[i].isWord("snapshot")) {
			i++
		}
		i = skipIfExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "drop":
		i++
		extractDropTarget(stmt, i, a)
	case "create":
		i++
		if i < len(stmt) && stmt[i].isWord("or") {
			i += 2 // OR REPLACE
		}
		if i < len(stmt) && (stmt[i].isWord("temp") || stmt[i].isWord("temporary")) {
			// session-scoped object; nothing shared to invalidate
			return
		}
		extractCreateTarget(stmt, i, a)
	case "call":
		i++
		// CALL path( ... ): the procedure is a routine target; its body
		// (if known to the caller) may reference tables.
		parts, _, _, _, ok := parsePath(stmt, i)
		if ok {
			a.WriteTargets = append(a.WriteTargets, Target{Parts: parts, Kind: TargetRoutine})
		}
	}
}

func extractDropTarget(stmt []token, i int, a *Analysis) {
	if i >= len(stmt) || stmt[i].kind != tkWord {
		return
	}
	w := stmt[i].lower()
	switch w {
	case "materialized":
		i++
		// MATERIALIZED VIEW
		if i < len(stmt) && stmt[i].isWord("view") {
			i++
		}
		i = skipIfExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "table":
		i++
		if i < len(stmt) && stmt[i].isWord("function") {
			// DROP TABLE FUNCTION <name>
			i++
			i = skipIfExists(stmt, i)
			addRoutineTarget(stmt, i, a)
			return
		}
		i = skipIfExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "view", "snapshot":
		i++
		i = skipIfExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "external":
		i++
		if i < len(stmt) && stmt[i].isWord("table") {
			i++
		}
		i = skipIfExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "function", "procedure":
		i++
		i = skipIfExists(stmt, i)
		addRoutineTarget(stmt, i, a)
	case "schema", "dataset":
		i++
		i = skipIfExists(stmt, i)
		if i < len(stmt) {
			parts, _, _, _, ok := parsePath(stmt, i)
			if ok {
				a.DropDatasets = append(a.DropDatasets, datasetTargetFromParts(parts))
			}
		}
	}
}

func extractCreateTarget(stmt []token, i int, a *Analysis) {
	if i >= len(stmt) || stmt[i].kind != tkWord {
		return
	}
	w := stmt[i].lower()
	switch w {
	case "external":
		i++
		if i < len(stmt) && stmt[i].isWord("table") {
			i++
		}
		i = skipIfNotExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "snapshot":
		i++
		if i < len(stmt) && stmt[i].isWord("table") {
			i++
		}
		i = skipIfNotExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "materialized":
		i++
		if i < len(stmt) && stmt[i].isWord("view") {
			i++
		}
		i = skipIfNotExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "table":
		i++
		if i < len(stmt) && stmt[i].isWord("function") {
			// CREATE TABLE FUNCTION <name>
			i++
			i = skipIfNotExists(stmt, i)
			addRoutineTarget(stmt, i, a)
			return
		}
		i = skipIfNotExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "view":
		i++
		i = skipIfNotExists(stmt, i)
		addTableTarget(stmt, i, a)
	case "table_function":
		i++
		i = skipIfNotExists(stmt, i)
		addRoutineTarget(stmt, i, a)
	case "function", "procedure":
		i++
		i = skipIfNotExists(stmt, i)
		addRoutineTarget(stmt, i, a)
	case "schema", "dataset":
		// nothing to invalidate for a newly created empty dataset
	}
}

func skipIfExists(stmt []token, i int) int {
	if i+1 < len(stmt) && stmt[i].isWord("if") && stmt[i+1].isWord("exists") {
		return i + 2
	}
	return i
}

func skipIfNotExists(stmt []token, i int) int {
	if i+2 < len(stmt) && stmt[i].isWord("if") && stmt[i+1].isWord("not") && stmt[i+2].isWord("exists") {
		return i + 3
	}
	return i
}

func addTableTarget(stmt []token, i int, a *Analysis) {
	parts, _, _, _, ok := parsePath(stmt, i)
	if !ok {
		return
	}
	a.WriteTargets = append(a.WriteTargets, Target{Parts: parts, Kind: TargetTable})
}

func addRoutineTarget(stmt []token, i int, a *Analysis) {
	parts, _, _, _, ok := parsePath(stmt, i)
	if !ok {
		return
	}
	a.WriteTargets = append(a.WriteTargets, Target{Parts: parts, Kind: TargetRoutine})
}

func datasetTargetFromParts(parts []string) DatasetTarget {
	switch len(parts) {
	case 3:
		return DatasetTarget{ProjectID: parts[0], DatasetID: parts[1]}
	case 2:
		return DatasetTarget{ProjectID: parts[0], DatasetID: parts[1]}
	default:
		// single-part dataset targets are qualified with the request
		// project by the caller
		return DatasetTarget{DatasetID: parts[0]}
	}
}

// KeyInput is the request context that determines a query result: two
// requests with identical KeyInput produce identical results, and any
// difference that can change the result produces a different key.
type KeyInput struct {
	ProjectID            string                           `json:"projectId"`
	DefaultProjectID     string                           `json:"defaultProjectId,omitempty"`
	DefaultDatasetID     string                           `json:"defaultDatasetId,omitempty"`
	Query                string                           `json:"query"`
	ParameterMode        string                           `json:"parameterMode,omitempty"`
	ConnectionProperties []*bigqueryv2.ConnectionProperty `json:"connectionProperties,omitempty"`
	UseLegacySQL         bool                             `json:"useLegacySql,omitempty"`
	Params               []*bigqueryv2.QueryParameter     `json:"params"`
}

// BuildKey returns the stable cache key for the given request context.
// Parameters and connection properties are serialized deterministically
// (encoding/json sorts map keys) so equivalent requests hash identically.
func BuildKey(in *KeyInput) (string, error) {
	params := make([]*bigqueryv2.QueryParameter, len(in.Params))
	copy(params, in.Params)
	props := make([]*bigqueryv2.ConnectionProperty, len(in.ConnectionProperties))
	copy(props, in.ConnectionProperties)
	sort.SliceStable(props, func(i, j int) bool {
		return props[i].Key < props[j].Key
	})
	clone := *in
	clone.Params = params
	clone.ConnectionProperties = props
	encoded, err := json.Marshal(&clone)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
