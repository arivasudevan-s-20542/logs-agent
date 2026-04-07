package query

import "fmt"

// TokenType represents a SQL token category.
type TokenType int

const (
	TokIdent TokenType = iota
	TokString
	TokNumber

	TokSelect
	TokFrom
	TokWhere
	TokAnd
	TokOr
	TokNot
	TokGroup
	TokBy
	TokHaving
	TokOrder
	TokAsc
	TokDesc
	TokLimit
	TokOffset
	TokLike
	TokRegex
	TokIn
	TokBetween
	TokAs
	TokIs
	TokNull
	TokTrue
	TokFalse
	TokDistinct

	TokCount
	TokSum
	TokAvg
	TokMin
	TokMax

	TokEq
	TokNeq
	TokLt
	TokGt
	TokLte
	TokGte

	TokLParen
	TokRParen
	TokComma
	TokStar
	TokDot

	TokEOF
)

type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}

var keywords = map[string]TokenType{
	"SELECT":   TokSelect,
	"FROM":     TokFrom,
	"WHERE":    TokWhere,
	"AND":      TokAnd,
	"OR":       TokOr,
	"NOT":      TokNot,
	"GROUP":    TokGroup,
	"BY":       TokBy,
	"HAVING":   TokHaving,
	"ORDER":    TokOrder,
	"ASC":      TokAsc,
	"DESC":     TokDesc,
	"LIMIT":    TokLimit,
	"OFFSET":   TokOffset,
	"LIKE":     TokLike,
	"REGEX":    TokRegex,
	"RLIKE":    TokRegex,
	"IN":       TokIn,
	"BETWEEN":  TokBetween,
	"AS":       TokAs,
	"IS":       TokIs,
	"NULL":     TokNull,
	"TRUE":     TokTrue,
	"FALSE":    TokFalse,
	"DISTINCT": TokDistinct,
	"COUNT":    TokCount,
	"SUM":      TokSum,
	"AVG":      TokAvg,
	"MIN":      TokMin,
	"MAX":      TokMax,
}

// Lexer tokenizes a SQL string.
type Lexer struct {
	input  string
	pos    int
	tokens []Token
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

func (l *Lexer) Tokenize() ([]Token, error) {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]

		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.pos++
			continue
		}

		switch {
		case ch == '\'', ch == '"':
			l.readString(ch)
		case ch >= '0' && ch <= '9':
			l.readNumber()
		case isIdentStart(ch):
			l.readIdent()
		case ch == '=':
			l.emit(TokEq, "=")
			l.pos++
		case ch == '!':
			if l.peek() == '=' {
				l.emit(TokNeq, "!=")
				l.pos += 2
			} else {
				return nil, l.errorf("unexpected character '!'")
			}
		case ch == '<':
			if l.peek() == '=' {
				l.emit(TokLte, "<=")
				l.pos += 2
			} else if l.peek() == '>' {
				l.emit(TokNeq, "<>")
				l.pos += 2
			} else {
				l.emit(TokLt, "<")
				l.pos++
			}
		case ch == '>':
			if l.peek() == '=' {
				l.emit(TokGte, ">=")
				l.pos += 2
			} else {
				l.emit(TokGt, ">")
				l.pos++
			}
		case ch == '(':
			l.emit(TokLParen, "(")
			l.pos++
		case ch == ')':
			l.emit(TokRParen, ")")
			l.pos++
		case ch == ',':
			l.emit(TokComma, ",")
			l.pos++
		case ch == '*':
			l.emit(TokStar, "*")
			l.pos++
		case ch == '.':
			l.emit(TokDot, ".")
			l.pos++
		default:
			return nil, l.errorf("unexpected character %q", ch)
		}
	}

	l.emit(TokEOF, "")
	return l.tokens, nil
}

func (l *Lexer) readString(quote byte) {
	start := l.pos
	l.pos++
	var sb []byte
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' && l.pos+1 < len(l.input) {
			l.pos++
			sb = append(sb, l.input[l.pos])
			l.pos++
			continue
		}
		if ch == quote {
			l.pos++
			l.tokens = append(l.tokens, Token{Type: TokString, Literal: string(sb), Pos: start})
			return
		}
		sb = append(sb, ch)
		l.pos++
	}
	l.tokens = append(l.tokens, Token{Type: TokString, Literal: string(sb), Pos: start})
}

func (l *Lexer) readNumber() {
	start := l.pos
	for l.pos < len(l.input) && (l.input[l.pos] >= '0' && l.input[l.pos] <= '9' || l.input[l.pos] == '.') {
		l.pos++
	}
	l.tokens = append(l.tokens, Token{Type: TokNumber, Literal: l.input[start:l.pos], Pos: start})
}

func (l *Lexer) readIdent() {
	start := l.pos
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	word := l.input[start:l.pos]
	upper := toUpper(word)
	if tt, ok := keywords[upper]; ok {
		l.tokens = append(l.tokens, Token{Type: tt, Literal: upper, Pos: start})
	} else {
		l.tokens = append(l.tokens, Token{Type: TokIdent, Literal: word, Pos: start})
	}
}

func (l *Lexer) peek() byte {
	if l.pos+1 < len(l.input) {
		return l.input[l.pos+1]
	}
	return 0
}

func (l *Lexer) emit(tt TokenType, lit string) {
	l.tokens = append(l.tokens, Token{Type: tt, Literal: lit, Pos: l.pos})
}

func (l *Lexer) errorf(format string, args ...any) error {
	return &ParseError{Pos: l.pos, Msg: fmt.Sprintf(format, args...)}
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func toUpper(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		} else {
			b[i] = c
		}
	}
	return string(b)
}
