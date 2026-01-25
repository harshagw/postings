package search

import (
	"fmt"
	"harshagw/postings/internal/index"
	"harshagw/postings/internal/query"
)

// Result represents a search hit with score.
type Result struct {
	DocID        string
	Score        float64
	Doc          map[string]any
	MatchedTerms []string
}

// Searcher performs searches on an index snapshot.
type Searcher struct {
	snapshot *index.IndexSnapshot
}

// New creates a new searcher for a snapshot.
func New(snapshot *index.IndexSnapshot) *Searcher {
	return &Searcher{snapshot: snapshot}
}

// Close releases searcher resources.
func (s *Searcher) Close() error {
	return nil
}

// RunQueryString parses and executes a query string.
func (s *Searcher) RunQueryString(queryString string) ([]Result, error) {
	tokens, err := query.Tokenize(queryString)
	if err != nil {
		return nil, err
	}

	ast, err := query.Parse(tokens)
	if err != nil {
		return nil, err
	}

	return s.execute(ast)
}

// RunQuery executes a pre-parsed query AST.
func (s *Searcher) RunQuery(q query.Query) ([]Result, error) {
	return s.execute(q)
}

// execute executes a query AST and returns the results.
func (s *Searcher) execute(q query.Query) ([]Result, error) {
	if q == nil {
		return nil, nil
	}
	switch v := q.(type) {
	case *query.TermQuery:
		return s.termSearch(v.Term, v.Field)
	case *query.PhraseQuery:
		return s.phraseSearch(v.Phrase, v.Field)
	case *query.PrefixQuery:
		return s.prefixSearch(v.Prefix, v.Field)
	case *query.RegexQuery:
		return s.regexSearch(v.Pattern, v.Field)
	case *query.FuzzyQuery:
		return s.fuzzySearch(v.Term, v.Fuzziness, v.Field)
	case *query.BoolQuery:
		return s.boolSearch(v)
	default:
		return nil, fmt.Errorf("unknown query type: %T", q)
	}
}
