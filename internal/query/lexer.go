package query

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int

const (
	TokenTerm    TokenType = iota
	TokenPhrase
	TokenField
	TokenAnd
	TokenOr
	TokenNot
	TokenLParen
	TokenRParen
	TokenPrefix
	TokenEOF
)

func (t TokenType) String() string {
	switch t {
	case TokenTerm:
		return "TERM"
	case TokenPhrase:
		return "PHRASE"
	case TokenField:
		return "FIELD"
	case TokenAnd:
		return "AND"
	case TokenOr:
		return "OR"
	case TokenNot:
		return "NOT"
	case TokenLParen:
		return "LPAREN"
	case TokenRParen:
		return "RPAREN"
	case TokenPrefix:
		return "PREFIX"
	case TokenEOF:
		return "EOF"
	default:
		return "UNKNOWN"
	}
}

// Token represents a lexical token.
type Token struct {
	Type  TokenType
	Value string
}

func (t Token) String() string {
	if t.Value != "" {
		return fmt.Sprintf("%s(%s)", t.Type, t.Value)
	}
	return t.Type.String()
}

// Lexer tokenizes a query string.
type Lexer struct {
	input string
	pos   int
}

// NewLexer creates a new lexer.
func NewLexer(input string) *Lexer {
	return &Lexer{input: input, pos: 0}
}

// Tokenize tokenizes a query string into tokens.
func Tokenize(query string) ([]Token, error) {
	lexer := NewLexer(query)
	return lexer.TokenizeAll()
}

// TokenizeAll returns all tokens from the input.
func (l *Lexer) TokenizeAll() ([]Token, error) {
	var tokens []Token
	for {
		token, err := l.NextToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
		if token.Type == TokenEOF {
			break
		}
	}
	return tokens, nil
}

// NextToken returns the next token.
func (l *Lexer) NextToken() (Token, error) {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF}, nil
	}

	ch := l.input[l.pos]

	switch ch {
	case '(':
		l.pos++
		return Token{Type: TokenLParen, Value: "("}, nil
	case ')':
		l.pos++
		return Token{Type: TokenRParen, Value: ")"}, nil
	case '-':
		if l.pos+1 < len(l.input) && !unicode.IsSpace(rune(l.input[l.pos+1])) {
			l.pos++
			return Token{Type: TokenNot, Value: "-"}, nil
		}
		return l.readTerm()
	case '"':
		return l.readPhrase()
	}

	return l.readWord()
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *Lexer) readPhrase() (Token, error) {
	l.pos++
	start := l.pos

	for l.pos < len(l.input) && l.input[l.pos] != '"' {
		if l.input[l.pos] == '\\' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '"' {
			l.pos += 2
			continue
		}
		l.pos++
	}

	if l.pos >= len(l.input) {
		return Token{}, fmt.Errorf("unterminated phrase at position %d", start-1)
	}

	value := l.input[start:l.pos]
	value = strings.ReplaceAll(value, `\"`, `"`)
	l.pos++

	return Token{Type: TokenPhrase, Value: value}, nil
}

func (l *Lexer) readWord() (Token, error) {
	start := l.pos

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsSpace(rune(ch)) || ch == '(' || ch == ')' || ch == '"' {
			break
		}
		l.pos++
	}

	word := l.input[start:l.pos]
	if word == "" {
		return Token{}, fmt.Errorf("unexpected character at position %d", l.pos)
	}

	switch word {
	case "AND":
		return Token{Type: TokenAnd, Value: word}, nil
	case "OR":
		return Token{Type: TokenOr, Value: word}, nil
	case "NOT":
		return Token{Type: TokenNot, Value: word}, nil
	}

	if colonIdx := strings.Index(word, ":"); colonIdx > 0 {
		field := word[:colonIdx]
		if colonIdx < len(word)-1 {
			l.pos = start + colonIdx + 1
			return Token{Type: TokenField, Value: field}, nil
		}
		return Token{Type: TokenField, Value: field}, nil
	}

	if strings.HasSuffix(word, "*") {
		prefix := strings.TrimSuffix(word, "*")
		return Token{Type: TokenPrefix, Value: prefix}, nil
	}

	return Token{Type: TokenTerm, Value: word}, nil
}

func (l *Lexer) readTerm() (Token, error) {
	start := l.pos

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsSpace(rune(ch)) || ch == '(' || ch == ')' || ch == '"' {
			break
		}
		l.pos++
	}

	word := l.input[start:l.pos]

	if strings.HasSuffix(word, "*") {
		prefix := strings.TrimSuffix(word, "*")
		return Token{Type: TokenPrefix, Value: prefix}, nil
	}

	return Token{Type: TokenTerm, Value: word}, nil
}
