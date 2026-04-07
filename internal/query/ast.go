package query

import "fmt"

// --- AST Node Types ---

// SelectStmt is the top-level AST node for a SELECT query.
type SelectStmt struct {
	Columns []SelectColumn
	From    string
	Where   Expr
	GroupBy []string
	Having  Expr
	OrderBy []OrderClause
	Limit   int // -1 = no limit
	Offset  int
}

type SelectColumn struct {
	Expr     Expr
	Alias    string
	IsAgg    bool
	AggFunc  string // COUNT, SUM, etc.
	Distinct bool
	Star     bool // SELECT *
}

type OrderClause struct {
	Expr Expr
	Desc bool
}

// --- Expressions ---

type Expr interface {
	exprNode()
	String() string
}

// IdentExpr — a field name: level, uid, msg, etc.
type IdentExpr struct {
	Name string
}

func (e *IdentExpr) exprNode()      {}
func (e *IdentExpr) String() string { return e.Name }

// LiteralExpr — a string, number, bool, or null literal.
type LiteralExpr struct {
	Value any    // string, float64, bool, nil
	Raw   string // original text
}

func (e *LiteralExpr) exprNode()      {}
func (e *LiteralExpr) String() string { return e.Raw }

// BinaryExpr — left op right (=, !=, <, >, AND, OR, LIKE, REGEX)
type BinaryExpr struct {
	Left  Expr
	Op    string // "=","!=","<",">","<=",">=","AND","OR","LIKE","REGEX"
	Right Expr
}

func (e *BinaryExpr) exprNode() {}
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Op, e.Right)
}

// UnaryExpr — NOT expr
type UnaryExpr struct {
	Op   string // "NOT"
	Expr Expr
}

func (e *UnaryExpr) exprNode()      {}
func (e *UnaryExpr) String() string { return fmt.Sprintf("(NOT %s)", e.Expr) }

// BetweenExpr — field BETWEEN low AND high
type BetweenExpr struct {
	Expr Expr
	Low  Expr
	High Expr
}

func (e *BetweenExpr) exprNode() {}
func (e *BetweenExpr) String() string {
	return fmt.Sprintf("(%s BETWEEN %s AND %s)", e.Expr, e.Low, e.High)
}

// InExpr — field IN ('a', 'b', 'c')
type InExpr struct {
	Expr   Expr
	Values []Expr
}

func (e *InExpr) exprNode() {}
func (e *InExpr) String() string {
	return fmt.Sprintf("(%s IN (...))", e.Expr)
}

// IsNullExpr — field IS [NOT] NULL
type IsNullExpr struct {
	Expr Expr
	Not  bool
}

func (e *IsNullExpr) exprNode() {}
func (e *IsNullExpr) String() string {
	if e.Not {
		return fmt.Sprintf("(%s IS NOT NULL)", e.Expr)
	}
	return fmt.Sprintf("(%s IS NULL)", e.Expr)
}

// AggExpr — COUNT(*), COUNT(DISTINCT uid), SUM(x), etc.
type AggExpr struct {
	Func     string // COUNT, SUM, AVG, MIN, MAX
	Arg      Expr   // nil for COUNT(*)
	Star     bool
	Distinct bool
}

func (e *AggExpr) exprNode() {}
func (e *AggExpr) String() string {
	if e.Star {
		return fmt.Sprintf("%s(*)", e.Func)
	}
	if e.Distinct {
		return fmt.Sprintf("%s(DISTINCT %s)", e.Func, e.Arg)
	}
	return fmt.Sprintf("%s(%s)", e.Func, e.Arg)
}

// StarExpr — used in SELECT *
type StarExpr struct{}

func (e *StarExpr) exprNode()      {}
func (e *StarExpr) String() string { return "*" }

// --- Errors ---

type ParseError struct {
	Pos int
	Msg string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at position %d: %s", e.Pos, e.Msg)
}
