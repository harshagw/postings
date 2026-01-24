package search

import (
	"harshagw/postings/internal/query"
)

// searcherAdapter wraps Searcher to implement query.SearchBackend.
type searcherAdapter struct {
	s *Searcher
}

func (a *searcherAdapter) Search(term, field string) ([]query.Result, error) {
	results, err := a.s.Search(term, field)
	if err != nil {
		return nil, err
	}
	return convertResults(results), nil
}

func (a *searcherAdapter) PhraseSearch(phrase, field string) ([]query.Result, error) {
	results, err := a.s.PhraseSearch(phrase, field)
	if err != nil {
		return nil, err
	}
	return convertResults(results), nil
}

func (a *searcherAdapter) PrefixSearch(prefix, field string) ([]query.Result, error) {
	results, err := a.s.PrefixSearch(prefix, field)
	if err != nil {
		return nil, err
	}
	return convertResults(results), nil
}

func convertResults(results []Result) []query.Result {
	qResults := make([]query.Result, len(results))
	for i, r := range results {
		qResults[i] = query.Result{
			DocID:        r.DocID,
			Score:        r.Score,
			Doc:          r.Doc,
			MatchedTerms: r.MatchedTerms,
		}
	}
	return qResults
}

func convertQueryResults(results []query.Result) []Result {
	sResults := make([]Result, len(results))
	for i, r := range results {
		sResults[i] = Result{
			DocID:        r.DocID,
			Score:        r.Score,
			Doc:          r.Doc,
			MatchedTerms: r.MatchedTerms,
		}
	}
	return sResults
}

// Query parses and executes a query string.
func (s *Searcher) Query(queryString string) ([]Result, error) {
	tokens, err := query.Tokenize(queryString)
	if err != nil {
		return nil, err
	}

	ast, err := query.Parse(tokens)
	if err != nil {
		return nil, err
	}

	adapter := &searcherAdapter{s: s}
	executor := query.NewExecutor(adapter)
	qResults, err := executor.Execute(ast)
	if err != nil {
		return nil, err
	}

	return convertQueryResults(qResults), nil
}
