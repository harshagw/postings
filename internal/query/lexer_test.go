package query

import (
	"testing"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "single term",
			input: "hello",
			expected: []Token{
				{Type: TokenTerm, Value: "hello"},
				{Type: TokenEOF},
			},
		},
		{
			name:  "multiple terms",
			input: "hello world",
			expected: []Token{
				{Type: TokenTerm, Value: "hello"},
				{Type: TokenTerm, Value: "world"},
				{Type: TokenEOF},
			},
		},
		{
			name:  "AND operator",
			input: "hello AND world",
			expected: []Token{
				{Type: TokenTerm, Value: "hello"},
				{Type: TokenAnd, Value: "AND"},
				{Type: TokenTerm, Value: "world"},
				{Type: TokenEOF},
			},
		},
		{
			name:  "OR operator",
			input: "hello OR world",
			expected: []Token{
				{Type: TokenTerm, Value: "hello"},
				{Type: TokenOr, Value: "OR"},
				{Type: TokenTerm, Value: "world"},
				{Type: TokenEOF},
			},
		},
		{
			name:  "NOT operator",
			input: "NOT hello",
			expected: []Token{
				{Type: TokenNot, Value: "NOT"},
				{Type: TokenTerm, Value: "hello"},
				{Type: TokenEOF},
			},
		},
		{
			name:  "minus as NOT",
			input: "-hello",
			expected: []Token{
				{Type: TokenNot, Value: "-"},
				{Type: TokenTerm, Value: "hello"},
				{Type: TokenEOF},
			},
		},
		{
			name: "Prefix",
			input: "hel*",
			expected: []Token{
				{Type: TokenPrefix, Value: "hel"},
				{Type: TokenEOF},
			},
		},
		{
			name: "Regex",
			input: "/hel.*/",
			expected: []Token{
				{Type: TokenRegex, Value: "hel.*"},
				{Type: TokenEOF},
			},
		},
		{
			name: "Fuzzy",
			input: "hello~2",
			expected: []Token{
				{Type: TokenFuzzy, Value: "hello~2"},
				{Type: TokenEOF},
			},
		},
		{
			name: "Field",
			input: "title:hello",
			expected: []Token{
				{Type: TokenField, Value: "title"},
				{Type: TokenTerm, Value: "hello"},
				{Type: TokenEOF},
			},
		},
		{
			name: "Parentheses",
			input: "(hello OR world)",
			expected: []Token{
				{Type: TokenLParen, Value: "("},
				{Type: TokenTerm, Value: "hello"},
				{Type: TokenOr, Value: "OR"},
				{Type: TokenTerm, Value: "world"},
				{Type: TokenRParen, Value: ")"},
				{Type: TokenEOF},
			},
		},
		{
			name: "Escapted Quote",
			input: `"hello \"world\""`,
			expected: []Token{
				{Type: TokenPhrase, Value: `hello "world"`},
				{Type: TokenEOF},
			},
		},
		{
			name: "Complex Query",
			input: `title:"go programming" AND (body:test* OR tags:lang*) -deprecated`,
			expected: []Token{
				{Type: TokenField, Value: "title"},
				{Type: TokenPhrase, Value: "go programming"},
				{Type: TokenAnd, Value: "AND"},
				{Type: TokenLParen, Value: "("},
				{Type: TokenField, Value: "body"},
				{Type: TokenPrefix, Value: "test"},
				{Type: TokenOr, Value: "OR"},
				{Type: TokenField, Value: "tags"},
				{Type: TokenPrefix, Value: "lang"},
				{Type: TokenRParen, Value: ")"},
				{Type: TokenNot, Value: "-"},
				{Type: TokenTerm, Value: "deprecated"},
				{Type: TokenEOF},
			},
		},
		}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := Tokenize(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tokens) != len(tt.expected) {
				t.Fatalf("expected %d tokens, got %d: %v", len(tt.expected), len(tokens), tokens)
			}
			for i, tok := range tokens {
				if tok.Type != tt.expected[i].Type {
					t.Errorf("token %d: expected type %v, got %v", i, tt.expected[i].Type, tok.Type)
				}
				if tok.Value != tt.expected[i].Value {
					t.Errorf("token %d: expected value %q, got %q", i, tt.expected[i].Value, tok.Value)
				}
			}
		})
	}
}

func TestTokenize_UnterminatedPhrase(t *testing.T) {
	_, err := Tokenize(`"hello world`)
	if err == nil {
		t.Error("expected error for unterminated phrase")
	}
}

func TestTokenize_UnterminatedRegex(t *testing.T) {
	_, err := Tokenize(`/hello.*`)
	if err == nil {
		t.Error("expected error for unterminated regex")
	}
}

