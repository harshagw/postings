package query

import (
	"fmt"
)

// Parser parses tokens into a Query AST.
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser creates a new parser.
func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

// Parse parses tokens into a Query AST.
func Parse(tokens []Token) (Query, error) {
	parser := NewParser(tokens)
	return parser.Parse()
}

// Parse parses the tokens into a Query AST.
func (p *Parser) Parse() (Query, error) {
	if len(p.tokens) == 0 || (len(p.tokens) == 1 && p.tokens[0].Type == TokenEOF) {
		return &MatchAllQuery{}, nil
	}

	query, err := p.parseOrExpr()
	if err != nil {
		return nil, err
	}

	if p.current().Type != TokenEOF {
		return nil, fmt.Errorf("unexpected token at position %d: %s", p.pos, p.current())
	}

	return query, nil
}

func (p *Parser) current() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	token := p.current()
	p.pos++
	return token
}

func (p *Parser) peek() Token {
	return p.current()
}

func (p *Parser) parseOrExpr() (Query, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	var orClauses []Query
	orClauses = append(orClauses, left)

	for p.peek().Type == TokenOr {
		p.advance()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		orClauses = append(orClauses, right)
	}

	if len(orClauses) == 1 {
		return orClauses[0], nil
	}

	return &BoolQuery{Should: orClauses}, nil
}

func (p *Parser) parseAndExpr() (Query, error) {
	left, err := p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	var andClauses []Query
	andClauses = append(andClauses, left)

	for {
		if p.peek().Type == TokenAnd {
			p.advance()
			right, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}
			andClauses = append(andClauses, right)
			continue
		}

		next := p.peek()
		if next.Type == TokenTerm || next.Type == TokenPhrase || next.Type == TokenField ||
			next.Type == TokenPrefix || next.Type == TokenLParen || next.Type == TokenNot {
			right, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}
			andClauses = append(andClauses, right)
			continue
		}

		break
	}

	if len(andClauses) == 1 {
		return andClauses[0], nil
	}

	return &BoolQuery{Must: andClauses}, nil
}

func (p *Parser) parseUnaryExpr() (Query, error) {
	if p.peek().Type == TokenNot {
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BoolQuery{MustNot: []Query{expr}}, nil
	}

	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (Query, error) {
	token := p.peek()

	switch token.Type {
	case TokenLParen:
		return p.parseGrouped()
	case TokenField:
		return p.parseFieldExpr()
	case TokenPhrase:
		p.advance()
		return &PhraseQuery{Phrase: token.Value}, nil
	case TokenPrefix:
		p.advance()
		return &PrefixQuery{Prefix: token.Value}, nil
	case TokenTerm:
		p.advance()
		return &TermQuery{Term: token.Value}, nil
	case TokenEOF:
		return nil, fmt.Errorf("unexpected end of query")
	default:
		return nil, fmt.Errorf("unexpected token: %s", token)
	}
}

func (p *Parser) parseGrouped() (Query, error) {
	p.advance()

	expr, err := p.parseOrExpr()
	if err != nil {
		return nil, err
	}

	if p.peek().Type != TokenRParen {
		return nil, fmt.Errorf("expected ')' at position %d, got %s", p.pos, p.peek())
	}
	p.advance()

	return expr, nil
}

func (p *Parser) parseFieldExpr() (Query, error) {
	fieldToken := p.advance()
	field := fieldToken.Value

	valueToken := p.peek()

	switch valueToken.Type {
	case TokenPhrase:
		p.advance()
		return &PhraseQuery{Field: field, Phrase: valueToken.Value}, nil
	case TokenPrefix:
		p.advance()
		return &PrefixQuery{Field: field, Prefix: valueToken.Value}, nil
	case TokenTerm:
		p.advance()
		return &TermQuery{Field: field, Term: valueToken.Value}, nil
	case TokenEOF, TokenRParen, TokenAnd, TokenOr:
		return nil, fmt.Errorf("expected term after field '%s:'", field)
	default:
		return nil, fmt.Errorf("expected term after field '%s:', got %s", field, valueToken)
	}
}
