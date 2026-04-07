package query

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zcop/log-agent/internal/parser"
)

// QueryResult holds the output of a query execution.
type QueryResult struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Total   int              `json:"total"`
	Elapsed time.Duration    `json:"elapsed"`
	Query   string           `json:"query"`
}

// Executor runs parsed SQL against log entries.
type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

// Execute runs a SelectStmt against a slice of LogEntry.
func (ex *Executor) Execute(stmt *SelectStmt, entries []*parser.LogEntry) (*QueryResult, error) {
	start := time.Now()

	// 1. Filter (WHERE)
	var filtered []*parser.LogEntry
	for _, e := range entries {
		if stmt.Where != nil {
			match, err := ex.evalBool(stmt.Where, e)
			if err != nil {
				return nil, fmt.Errorf("WHERE: %w", err)
			}
			if !match {
				continue
			}
		}
		filtered = append(filtered, e)
	}

	var result *QueryResult

	// 2. GROUP BY or plain select
	if len(stmt.GroupBy) > 0 || hasAgg(stmt.Columns) {
		var err error
		result, err = ex.executeGroupBy(stmt, filtered)
		if err != nil {
			return nil, err
		}
	} else {
		result = ex.executePlain(stmt, filtered)
	}

	result.Elapsed = time.Since(start)
	return result, nil
}

func (ex *Executor) executePlain(stmt *SelectStmt, entries []*parser.LogEntry) *QueryResult {
	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		ex.sortEntries(entries, stmt.OrderBy)
	}

	total := len(entries)

	// OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(entries) {
		entries = entries[stmt.Offset:]
	} else if stmt.Offset >= len(entries) {
		entries = nil
	}

	// LIMIT
	if stmt.Limit >= 0 && stmt.Limit < len(entries) {
		entries = entries[:stmt.Limit]
	}

	// Build columns list
	isStar := len(stmt.Columns) == 1 && stmt.Columns[0].Star
	var cols []string
	if isStar {
		cols = allFields()
	} else {
		for _, c := range stmt.Columns {
			cols = append(cols, colName(c))
		}
	}

	// Build rows
	rows := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		row := make(map[string]any)
		if isStar {
			row = entryMap(e)
		} else {
			for _, c := range stmt.Columns {
				name := colName(c)
				row[name] = ex.evalValue(c.Expr, e)
			}
		}
		rows = append(rows, row)
	}

	return &QueryResult{Columns: cols, Rows: rows, Total: total}
}

func (ex *Executor) executeGroupBy(stmt *SelectStmt, entries []*parser.LogEntry) (*QueryResult, error) {
	// Build groups
	type group struct {
		key     map[string]string
		entries []*parser.LogEntry
	}

	groups := map[string]*group{}
	var groupOrder []string

	for _, e := range entries {
		keyParts := make(map[string]string)
		var keyStr string
		for _, field := range stmt.GroupBy {
			val := fieldValue(e, field)
			keyParts[field] = val
			keyStr += val + "\x00"
		}
		g, ok := groups[keyStr]
		if !ok {
			g = &group{key: keyParts}
			groups[keyStr] = g
			groupOrder = append(groupOrder, keyStr)
		}
		g.entries = append(g.entries, e)
	}

	// Build rows from groups
	var rows []map[string]any
	for _, k := range groupOrder {
		g := groups[k]
		row := make(map[string]any)
		// Add group keys
		for field, val := range g.key {
			row[field] = val
		}
		// Compute aggregates
		for _, col := range stmt.Columns {
			name := colName(col)
			if col.IsAgg {
				val := ex.computeAgg(col, g.entries)
				row[name] = val
			} else if _, isIdent := col.Expr.(*IdentExpr); isIdent {
				// Already set from group key
			}
		}
		rows = append(rows, row)
	}

	// HAVING filter
	if stmt.Having != nil {
		var filtered []map[string]any
		for _, row := range rows {
			match, err := ex.evalRowBool(stmt.Having, row)
			if err != nil {
				return nil, fmt.Errorf("HAVING: %w", err)
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		ex.sortRows(rows, stmt.OrderBy)
	}

	total := len(rows)

	// OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(rows) {
		rows = rows[stmt.Offset:]
	} else if stmt.Offset >= len(rows) {
		rows = nil
	}

	// LIMIT
	if stmt.Limit >= 0 && stmt.Limit < len(rows) {
		rows = rows[:stmt.Limit]
	}

	// Determine columns
	var cols []string
	for _, c := range stmt.Columns {
		cols = append(cols, colName(c))
	}

	return &QueryResult{Columns: cols, Rows: rows, Total: total}, nil
}

func (ex *Executor) computeAgg(col SelectColumn, entries []*parser.LogEntry) any {
	agg, ok := col.Expr.(*AggExpr)
	if !ok {
		return nil
	}

	switch agg.Func {
	case "COUNT":
		if agg.Star {
			return len(entries)
		}
		if agg.Distinct {
			seen := map[string]struct{}{}
			for _, e := range entries {
				v := ex.evalValueStr(agg.Arg, e)
				seen[v] = struct{}{}
			}
			return len(seen)
		}
		count := 0
		for _, e := range entries {
			v := ex.evalValueStr(agg.Arg, e)
			if v != "" {
				count++
			}
		}
		return count

	case "MIN":
		var minVal string
		first := true
		for _, e := range entries {
			v := ex.evalValueStr(agg.Arg, e)
			if first || v < minVal {
				minVal = v
				first = false
			}
		}
		return minVal

	case "MAX":
		var maxVal string
		first := true
		for _, e := range entries {
			v := ex.evalValueStr(agg.Arg, e)
			if first || v > maxVal {
				maxVal = v
				first = false
			}
		}
		return maxVal

	case "SUM":
		sum := 0.0
		for _, e := range entries {
			v := ex.evalValueStr(agg.Arg, e)
			f, _ := strconv.ParseFloat(v, 64)
			sum += f
		}
		return sum

	case "AVG":
		if len(entries) == 0 {
			return 0.0
		}
		sum := 0.0
		for _, e := range entries {
			v := ex.evalValueStr(agg.Arg, e)
			f, _ := strconv.ParseFloat(v, 64)
			sum += f
		}
		return math.Round(sum/float64(len(entries))*100) / 100
	}

	return nil
}

// --- Expression evaluation against LogEntry ---

func (ex *Executor) evalBool(expr Expr, e *parser.LogEntry) (bool, error) {
	switch v := expr.(type) {
	case *BinaryExpr:
		switch v.Op {
		case "AND":
			l, err := ex.evalBool(v.Left, e)
			if err != nil {
				return false, err
			}
			if !l {
				return false, nil
			}
			return ex.evalBool(v.Right, e)
		case "OR":
			l, err := ex.evalBool(v.Left, e)
			if err != nil {
				return false, err
			}
			if l {
				return true, nil
			}
			return ex.evalBool(v.Right, e)
		case "=":
			return ex.cmpStr(v.Left, v.Right, e, func(a, b string) bool { return a == b }), nil
		case "!=":
			return ex.cmpStr(v.Left, v.Right, e, func(a, b string) bool { return a != b }), nil
		case "<":
			return ex.cmpOrd(v.Left, v.Right, e, func(c int) bool { return c < 0 }), nil
		case ">":
			return ex.cmpOrd(v.Left, v.Right, e, func(c int) bool { return c > 0 }), nil
		case "<=":
			return ex.cmpOrd(v.Left, v.Right, e, func(c int) bool { return c <= 0 }), nil
		case ">=":
			return ex.cmpOrd(v.Left, v.Right, e, func(c int) bool { return c >= 0 }), nil
		case "LIKE":
			left := ex.evalValueStr(v.Left, e)
			right := ex.evalValueStr(v.Right, e)
			return matchLike(left, right), nil
		case "REGEX":
			left := ex.evalValueStr(v.Left, e)
			right := ex.evalValueStr(v.Right, e)
			re, err := regexp.Compile("(?i)" + right)
			if err != nil {
				return false, fmt.Errorf("invalid regex %q: %w", right, err)
			}
			return re.MatchString(left), nil
		}
	case *UnaryExpr:
		if v.Op == "NOT" {
			b, err := ex.evalBool(v.Expr, e)
			return !b, err
		}
	case *BetweenExpr:
		val := ex.evalValueStr(v.Expr, e)
		low := ex.evalValueStr(v.Low, e)
		high := ex.evalValueStr(v.High, e)
		return val >= low && val <= high, nil
	case *InExpr:
		val := ex.evalValueStr(v.Expr, e)
		for _, item := range v.Values {
			if ex.evalValueStr(item, e) == val {
				return true, nil
			}
		}
		return false, nil
	case *IsNullExpr:
		val := ex.evalValueStr(v.Expr, e)
		isNull := val == ""
		if v.Not {
			return !isNull, nil
		}
		return isNull, nil
	case *LiteralExpr:
		if b, ok := v.Value.(bool); ok {
			return b, nil
		}
	}
	return false, fmt.Errorf("cannot evaluate %T as boolean", expr)
}

// evalRowBool evaluates expr against an aggregated row map (for HAVING).
func (ex *Executor) evalRowBool(expr Expr, row map[string]any) (bool, error) {
	switch v := expr.(type) {
	case *BinaryExpr:
		switch v.Op {
		case "AND":
			l, err := ex.evalRowBool(v.Left, row)
			if err != nil {
				return false, err
			}
			if !l {
				return false, nil
			}
			return ex.evalRowBool(v.Right, row)
		case "OR":
			l, err := ex.evalRowBool(v.Left, row)
			if err != nil {
				return false, err
			}
			if l {
				return true, nil
			}
			return ex.evalRowBool(v.Right, row)
		default:
			left := ex.evalRowValue(v.Left, row)
			right := ex.evalRowValue(v.Right, row)
			return cmpAny(left, right, v.Op), nil
		}
	case *UnaryExpr:
		b, err := ex.evalRowBool(v.Expr, row)
		return !b, err
	}
	return false, nil
}

func (ex *Executor) evalRowValue(expr Expr, row map[string]any) any {
	switch v := expr.(type) {
	case *IdentExpr:
		return row[v.Name]
	case *LiteralExpr:
		return v.Value
	case *AggExpr:
		return row[v.String()]
	}
	return nil
}

func cmpAny(a, b any, op string) bool {
	af := toFloat(a)
	bf := toFloat(b)
	switch op {
	case "=":
		return af == bf
	case "!=":
		return af != bf
	case ">":
		return af > bf
	case "<":
		return af < bf
	case ">=":
		return af >= bf
	case "<=":
		return af <= bf
	}
	return false
}

func toFloat(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	}
	return 0
}

func (ex *Executor) evalValue(expr Expr, e *parser.LogEntry) any {
	switch v := expr.(type) {
	case *IdentExpr:
		return fieldValue(e, v.Name)
	case *LiteralExpr:
		return v.Value
	case *StarExpr:
		return nil
	}
	return nil
}

func (ex *Executor) evalValueStr(expr Expr, e *parser.LogEntry) string {
	switch v := expr.(type) {
	case *IdentExpr:
		return fieldValue(e, v.Name)
	case *LiteralExpr:
		switch val := v.Value.(type) {
		case string:
			return val
		case float64:
			return strconv.FormatFloat(val, 'f', -1, 64)
		case bool:
			if val {
				return "true"
			}
			return "false"
		}
		return ""
	}
	return ""
}

func (ex *Executor) cmpStr(left, right Expr, e *parser.LogEntry, fn func(a, b string) bool) bool {
	a := strings.ToLower(ex.evalValueStr(left, e))
	b := strings.ToLower(ex.evalValueStr(right, e))
	return fn(a, b)
}

func (ex *Executor) cmpOrd(left, right Expr, e *parser.LogEntry, fn func(int) bool) bool {
	a := ex.evalValueStr(left, e)
	b := ex.evalValueStr(right, e)
	// Try timestamp comparison
	if ta, err := time.Parse(time.RFC3339, a); err == nil {
		if tb, err := time.Parse(time.RFC3339, b); err == nil {
			return fn(ta.Compare(tb))
		}
	}
	// Try numeric comparison
	if fa, err := strconv.ParseFloat(a, 64); err == nil {
		if fb, err := strconv.ParseFloat(b, 64); err == nil {
			if fa < fb {
				return fn(-1)
			} else if fa > fb {
				return fn(1)
			}
			return fn(0)
		}
	}
	// String comparison
	return fn(strings.Compare(strings.ToLower(a), strings.ToLower(b)))
}

// --- Sorting ---

func (ex *Executor) sortEntries(entries []*parser.LogEntry, clauses []OrderClause) {
	sort.SliceStable(entries, func(i, j int) bool {
		for _, c := range clauses {
			a := ex.evalValueStr(c.Expr, entries[i])
			b := ex.evalValueStr(c.Expr, entries[j])
			if a == b {
				continue
			}
			less := a < b
			if c.Desc {
				less = !less
			}
			return less
		}
		return false
	})
}

func (ex *Executor) sortRows(rows []map[string]any, clauses []OrderClause) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, c := range clauses {
			a := ex.evalRowValue(c.Expr, rows[i])
			b := ex.evalRowValue(c.Expr, rows[j])
			af := toFloat(a)
			bf := toFloat(b)
			if af == bf {
				continue
			}
			less := af < bf
			if c.Desc {
				less = !less
			}
			return less
		}
		return false
	})
}

// --- Helpers ---

func fieldValue(e *parser.LogEntry, name string) string {
	switch strings.ToLower(name) {
	case "timestamp", "ts", "time":
		return e.Timestamp.Format(time.RFC3339Nano)
	case "level", "lvl":
		return e.Level
	case "thread":
		return e.Thread
	case "rid", "request_id", "requestid":
		return e.RID
	case "uid", "user_id", "userid":
		return e.UID
	case "uri", "path", "endpoint":
		return e.URI
	case "logger", "class":
		return e.Logger
	case "msg", "message":
		return e.Message
	case "file":
		return e.File
	case "raw":
		return e.Raw
	case "line":
		return strconv.FormatInt(e.Line, 10)
	}
	return ""
}

func allFields() []string {
	return []string{"timestamp", "level", "thread", "rid", "uid", "uri", "logger", "msg", "file"}
}

func entryMap(e *parser.LogEntry) map[string]any {
	return map[string]any{
		"timestamp": e.Timestamp.Format(time.RFC3339Nano),
		"level":     e.Level,
		"thread":    e.Thread,
		"rid":       e.RID,
		"uid":       e.UID,
		"uri":       e.URI,
		"logger":    e.Logger,
		"msg":       e.Message,
		"file":      e.File,
	}
}

func colName(c SelectColumn) string {
	if c.Alias != "" {
		return c.Alias
	}
	if c.Star {
		return "*"
	}
	return c.Expr.String()
}

func hasAgg(cols []SelectColumn) bool {
	for _, c := range cols {
		if c.IsAgg {
			return true
		}
	}
	return false
}

// matchLike implements SQL LIKE with % and _ wildcards.
func matchLike(str, pattern string) bool {
	str = strings.ToLower(str)
	pattern = strings.ToLower(pattern)
	return matchLikeRec(str, pattern, 0, 0)
}

func matchLikeRec(str, pat string, si, pi int) bool {
	for pi < len(pat) {
		if pat[pi] == '%' {
			pi++
			// % matches zero or more chars
			for si <= len(str) {
				if matchLikeRec(str, pat, si, pi) {
					return true
				}
				si++
			}
			return false
		}
		if si >= len(str) {
			return false
		}
		if pat[pi] == '_' || pat[pi] == str[si] {
			si++
			pi++
		} else {
			return false
		}
	}
	return si == len(str)
}

// ParseAndExecute is a convenience function that lexes, parses, and executes.
func ParseAndExecute(sql string, entries []*parser.LogEntry) (*QueryResult, error) {
	tokens, err := NewLexer(sql).Tokenize()
	if err != nil {
		return nil, err
	}
	stmt, err := NewParser(tokens).Parse()
	if err != nil {
		return nil, err
	}
	result, err := NewExecutor().Execute(stmt, entries)
	if err != nil {
		return nil, err
	}
	result.Query = sql
	return result, nil
}
