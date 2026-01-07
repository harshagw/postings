package analysis

import (
	"strings"
	"unicode"
)

type TokenPosition struct {
	Token    string
	Position uint64
}

// Analyzer defines the interface for text analysis.
type Analyzer interface {
	Analyze(text string) []TokenPosition
}

// Simple performs basic tokenization: lowercasing and splitting on non-alphanumeric.
type Simple struct{}

func NewSimple() *Simple {
	return &Simple{}
}

// Analyze tokenizes text into tokens with positions.
func (a *Simple) Analyze(text string) []TokenPosition {
	var tokens []TokenPosition
	var currentToken strings.Builder
	var position uint64

	text = strings.ToLower(text)

	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			currentToken.WriteRune(r)
		} else {
			if currentToken.Len() > 0 {
				tokens = append(tokens, TokenPosition{
					Token:    currentToken.String(),
					Position: position,
				})
				position++
				currentToken.Reset()
			}
		}
	}

	if currentToken.Len() > 0 {
		tokens = append(tokens, TokenPosition{
			Token:    currentToken.String(),
			Position: position,
		})
	}

	return tokens
}
