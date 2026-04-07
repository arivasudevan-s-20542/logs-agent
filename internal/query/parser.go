package query

import (
	"fmt"
	"strconv"
)

// Parser is a recursive descent parser for a SQL subset.
type Parser struct {
	tokens []Token
	pos    int
}

func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens}
}

// Parse parses a SELECT statement.
func (p *Parser) Parse() (*SelectStmt, error) {
	stmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if !p.is(TokEOF) {
		return nil, p.errorf("unexpected token %q after statement", p.cur().Literal)
	}
	return stmt, nil
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	if err := p.expect(TokSelect); err != nil {
		return nil, err
	}

	stmt := &SelectStmt{Limit: -1}

	// SELECT columns
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM
	if err := p.expect(TokFrom); err != nil {
		return nil, err
	}
	if !p.is(TokIdent) {
		return nil, p.errorf("expected table name after FROM")
	}
	stmt.From = p.advance().Literal

	// WHERE (optional)
	if p.is(TokWhere) {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// GROUP BY (optional)
	if p.is(TokGroup) {
		p.advance()
		if err := p.expect(TokBy); err != nil {
			return nil, err
		}
		fields, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = fields
	}

	// HAVING (optional)
	if p.is(TokHaving) {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	// ORDER BY (optional)
	if p.is(TokOrder) {
		p.advance()
		if err := p.expect(TokBy); err != nil {
			return nil, err
		}
		clauses, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = clauses
	}

	// LIMIT (optional)
	if p.is(TokLimit) {
		p.advance()
		if !p.is(TokNumber) {
			return nil, p.errorf("expected number after LIMIT")
		}
		n, _ := strconv.Atoi(p.advance().Literal)
		stmt.Limit = n
	}

	// OFFSET (optional)
	if p.is(TokOffset) {
		p.advance()
		if !p.is(TokNumber) {
			return nil, p.errorf("expected number after OFFSET")
		}
		n, _ := strconv.Atoi(p.advance().Literal)
		stmt.Offset = n
	}

	return stmt, nil
}

func (p *Parser) parseSelectColumns() ([]SelectColumn, error) {
	// SELECT *
	if p.is(TokStar) {
		p.advance()
		return []SelectColumn{{Star: true, Expr: &StarExpr{}}}, nil
	}

	var cols []SelectColumn
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if !p.is(TokComma) {
			break
		}
		p.advance() // skip comma
	}
	return cols, nil
}

func (p *Parser) parseSelectColumn() (SelectColumn, error) {
	// Aggregate function?
	if p.isAggFunc() {
		agg, err := p.parseAggExpr()
		if err != nil {
			return SelectColumn{}, err
		}
		col := SelectColumn{
			Expr:     agg,
			IsAgg:    true,
			AggFunc:  agg.Func,
			Distinct: agg.Distinct,
		}
		// AS alias?
		if p.is(TokAs) {
			p.advance()
			if !p.is(TokIdent) {
				return col, p.errorf("expected alias after AS")
			}
			col.Alias = p.advance().Literal
		}
		return col, nil
	}

	// Regular expression (field name)
	expr, err := p.parsePrimary()
	if err != nil {
		return SelectColumn{}, err
	}
	col := SelectColumn{Expr: expr}
	if p.is(TokAs) {
		p.advance()
		if !p.is(TokIdent) {
			return col, p.errorf("expected alias after AS")
		}
		col.Alias = p.advance().Literal
	}
	return col, nil
}

func (p *Parser) parseAggExpr() (*AggExpr, error) {
	funcName := p.advance().Literal // COUNT, SUM, etc.
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}

	agg := &AggExpr{Func: funcName}

	// COUNT(*)
	if p.is(TokStar) {
		p.advance()
		agg.Star = true
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return agg, nil
	}

	// DISTINCT?
	if p.is(TokDistinct) {
		p.advance()
		agg.Distinct = true
	}

	arg, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	agg.Arg = arg

	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return agg, nil
}

// --- Expression Parsing (precedence climbing) ---

func (p *Parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.is(TokOr) {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "OR", Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.is(TokAnd) {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "AND", Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (Expr, error) {
	if p.is(TokNot) {
		p.advance()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	// Comparison operators: =, !=, <, >, <=, >=
	switch {
	case p.is(TokEq):
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "=", Right: right}, nil

	case p.is(TokNeq):
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "!=", Right: right}, nil

	case p.is(TokLt):
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "<", Right: right}, nil

	case p.is(TokGt):
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: ">", Right: right}, nil

	case p.is(TokLte):
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "<=", Right: right}, nil

	case p.is(TokGte):
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: ">=", Right: right}, nil

	// LIKE
	case p.is(TokLike):
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "LIKE", Right: right}, nil

	// REGEX / RLIKE
	case p.is(TokRegex):
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: "REGEX", Right: right}, nil

	// NOT LIKE / NOT REGEX / NOT IN / NOT BETWEEN
	case p.is(TokNot):
		p.advance()
		switch {
		case p.is(TokLike):
			p.advance()
			right, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{Op: "NOT", Expr: &BinaryExpr{Left: left, Op: "LIKE", Right: right}}, nil
		case p.is(TokRegex):
			p.advance()
			right, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{Op: "NOT", Expr: &BinaryExpr{Left: left, Op: "REGEX", Right: right}}, nil
		case p.is(TokIn):
			p.advance()
			vals, err := p.parseInList()
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{Op: "NOT", Expr: &InExpr{Expr: left, Values: vals}}, nil
		case p.is(TokBetween):
			p.advance()
			low, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			if err := p.expect(TokAnd); err != nil {
				return nil, err
			}
			high, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{Op: "NOT", Expr: &BetweenExpr{Expr: left, Low: low, High: high}}, nil
		default:
			return nil, p.errorf("expected LIKE, REGEX, IN, or BETWEEN after NOT")
		}

	// IN
	case p.is(TokIn):
		p.advance()
		vals, err := p.parseInList()
		if err != nil {
			return nil, err
		}
		return &InExpr{Expr: left, Values: vals}, nil

	// BETWEEN
	case p.is(TokBetween):
		p.advance()
		low, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TokAnd); err != nil {
			return nil, err
		}
		high, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: left, Low: low, High: high}, nil

	// IS [NOT] NULL
	case p.is(TokIs):
		p.advance()
		not := false
		if p.is(TokNot) {
			p.advance()
			not = true
		}
		if err := p.expect(TokNull); err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: left, Not: not}, nil
	}

	return left, nil
}

func (p *Parser) parsePrimary() (Expr, error) {
	tok := p.cur()

	switch tok.Type {
	case TokIdent:
		p.advance()
		return &IdentExpr{Name: tok.Literal}, nil

	case TokString:
		p.advance()
		return &LiteralExpr{Value: tok.Literal, Raw: "'" + tok.Literal + "'"}, nil

	case TokNumber:
		p.advance()
		f, _ := strconv.ParseFloat(tok.Literal, 64)
		return &LiteralExpr{Value: f, Raw: tok.Literal}, nil

	case TokTrue:
		p.advance()
		return &LiteralExpr{Value: true, Raw: "TRUE"}, nil

	case TokFalse:
		p.advance()
		return &LiteralExpr{Value: false, Raw: "FALSE"}, nil

	case TokNull:
		p.advance()
		return &LiteralExpr{Value: nil, Raw: "NULL"}, nil

	case TokStar:
		p.advance()
		return &StarExpr{}, nil

	case TokLParen:
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TokRParen); err != nil {
			return nil, err
		}
		return expr, nil

	// Aggregate in WHERE / HAVING
	case TokCount, TokSum, TokAvg, TokMin, TokMax:
		return p.parseAggExpr()

	default:
		return nil, p.errorf("unexpected token %q", tok.Literal)
	}
}

func (p *Parser) parseInList() ([]Expr, error) {
	if err := p.expect(TokLParen); err != nil {
		return nil, err
	}
	var vals []Expr
	for {
		if p.is(TokRParen) {
			break
		}
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		vals = append(vals, expr)
		if !p.is(TokComma) {
			break
		}
		p.advance()
	}
	if err := p.expect(TokRParen); err != nil {
		return nil, err
	}
	return vals, nil
}

func (p *Parser) parseIdentList() ([]string, error) {
	var ids []string
	if !p.is(TokIdent) {
		return nil, p.errorf("expected field name")
	}
	ids = append(ids, p.advance().Literal)
	for p.is(TokComma) {
		p.advance()
		if !p.is(TokIdent) {
			return nil, p.errorf("expected field name after comma")
		}
		ids = append(ids, p.advance().Literal)
	}
	return ids, nil
}

func (p *Parser) parseOrderBy() ([]OrderClause, error) {
	var clauses []OrderClause
	for {
		var expr Expr
		var err error
		if p.isAggFunc() {
			expr, err = p.parseAggExpr()
		} else {
			expr, err = p.parsePrimary()
		}
		if err != nil {
			return nil, err
		}
		desc := false
		if p.is(TokDesc) {
			p.advance()
			desc = true
		} else if p.is(TokAsc) {
			p.advance()
		}
		clauses = append(clauses, OrderClause{Expr: expr, Desc: desc})
		if !p.is(TokComma) {
			break
		}
		p.advance()
	}
	return clauses, nil
}

// --- Helpers ---

func (p *Parser) cur() Token {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return Token{Type: TokEOF}
}

func (p *Parser) is(tt TokenType) bool {
	return p.cur().Type == tt
}

func (p *Parser) advance() Token {
	tok := p.cur()
	p.pos++
	return tok
}

func (p *Parser) expect(tt TokenType) error {
	if !p.is(tt) {
		return p.errorf("expected %v but got %q", tokenName(tt), p.cur().Literal)
	}
	p.advance()
	return nil
}

func (p *Parser) isAggFunc() bool {
	switch p.cur().Type {
	case TokCount, TokSum, TokAvg, TokMin, TokMax:
		return true
	}
	return false
}

func (p *Parser) errorf(format string, args ...any) error {
	return &ParseError{Pos: p.cur().Pos, Msg: fmt.Sprintf(format, args...)}
}

func tokenName(tt TokenType) string {
	names := map[TokenType]string{
		TokSelect: "SELECT", TokFrom: "FROM", TokWhere: "WHERE",
		TokAnd: "AND", TokOr: "OR", TokBy: "BY", TokNull: "NULL",
		TokLParen: "(", TokRParen: ")", TokComma: ",",
		TokEOF: "EOF", TokIdent: "identifier", TokNumber: "number",
	}
	if s, ok := names[tt]; ok {
		return s
	}
	return "token"
}
