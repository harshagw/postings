package query

import (
	"fmt"
)

// Result represents a search hit with score.
type Result struct {
	DocID        string
	Score        float64
	Doc          map[string]any
	MatchedTerms []string
}

// SearchBackend defines the interface for primitive search operations.
type SearchBackend interface {
	Search(term, field string) ([]Result, error)
	PhraseSearch(phrase, field string) ([]Result, error)
	PrefixSearch(prefix, field string) ([]Result, error)
}

// Executor executes a Query AST against a SearchBackend.
type Executor struct {
	backend SearchBackend
}

// NewExecutor creates a new executor.
func NewExecutor(backend SearchBackend) *Executor {
	return &Executor{backend: backend}
}

// Execute executes a query and returns the results.
func (e *Executor) Execute(q Query) ([]Result, error) {
	switch v := q.(type) {
	case *TermQuery:
		return e.backend.Search(v.Term, v.Field)
	case *PhraseQuery:
		return e.backend.PhraseSearch(v.Phrase, v.Field)
	case *PrefixQuery:
		return e.backend.PrefixSearch(v.Prefix, v.Field)
	case *BoolQuery:
		return e.executeBool(v)
	case *MatchAllQuery:
		return nil, nil
	case *MatchNoneQuery:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown query type: %T", q)
	}
}

func (e *Executor) executeBool(q *BoolQuery) ([]Result, error) {
	flattenedMust := make([]Query, 0, len(q.Must))
	flattenedMustNot := append([]Query{}, q.MustNot...)

	for _, m := range q.Must {
		if bq, ok := m.(*BoolQuery); ok && len(bq.Must) == 0 && len(bq.Should) == 0 && len(bq.MustNot) > 0 {
			flattenedMustNot = append(flattenedMustNot, bq.MustNot...)
		} else {
			flattenedMust = append(flattenedMust, m)
		}
	}

	flattenedShould := make([]Query, 0, len(q.Should))
	for _, s := range q.Should {
		if bq, ok := s.(*BoolQuery); ok && len(bq.Must) == 0 && len(bq.Should) == 0 && len(bq.MustNot) > 0 {
			flattenedMustNot = append(flattenedMustNot, bq.MustNot...)
		} else {
			flattenedShould = append(flattenedShould, s)
		}
	}

	must := flattenedMust
	should := flattenedShould
	mustNot := flattenedMustNot

	if len(must) == 0 && len(should) == 0 && len(mustNot) > 0 {
		return nil, fmt.Errorf("NOT queries require a positive clause")
	}

	if len(must) == 0 && len(should) > 0 && len(mustNot) == 0 {
		return e.executeOr(should)
	}

	if len(must) > 0 && len(should) == 0 && len(mustNot) == 0 {
		return e.executeAnd(must)
	}

	if len(must) > 0 && len(should) == 0 && len(mustNot) > 0 {
		return e.executeAndNot(must, mustNot)
	}

	if len(must) == 0 && len(should) > 0 && len(mustNot) > 0 {
		return e.executeOrNot(should, mustNot)
	}

	if len(must) > 0 && len(should) > 0 {
		return e.executeComplex(must, should, mustNot)
	}

	return nil, nil
}

func (e *Executor) executeOr(queries []Query) ([]Result, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	if len(queries) == 1 {
		return e.Execute(queries[0])
	}

	docScores := make(map[string]Result)

	for _, q := range queries {
		results, err := e.Execute(q)
		if err != nil {
			return nil, err
		}
		for _, r := range results {
			if existing, ok := docScores[r.DocID]; ok {
				existing.Score += r.Score
				if r.MatchedTerms != nil {
					existing.MatchedTerms = append(existing.MatchedTerms, r.MatchedTerms...)
				}
				docScores[r.DocID] = existing
			} else {
				docScores[r.DocID] = r
			}
		}
	}

	results := make([]Result, 0, len(docScores))
	for _, r := range docScores {
		results = append(results, r)
	}

	sortByScore(results)
	return results, nil
}

func (e *Executor) executeAnd(queries []Query) ([]Result, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	if len(queries) == 1 {
		return e.Execute(queries[0])
	}

	firstResults, err := e.Execute(queries[0])
	if err != nil {
		return nil, err
	}
	if len(firstResults) == 0 {
		return nil, nil
	}

	candidates := make(map[string]Result)
	for _, r := range firstResults {
		candidates[r.DocID] = r
	}

	for _, q := range queries[1:] {
		results, err := e.Execute(q)
		if err != nil {
			return nil, err
		}

		resultSet := make(map[string]Result)
		for _, r := range results {
			resultSet[r.DocID] = r
		}

		for docID, r := range candidates {
			if other, ok := resultSet[docID]; ok {
				r.Score += other.Score
				candidates[docID] = r
			} else {
				delete(candidates, docID)
			}
		}

		if len(candidates) == 0 {
			return nil, nil
		}
	}

	results := make([]Result, 0, len(candidates))
	for _, r := range candidates {
		results = append(results, r)
	}

	sortByScore(results)
	return results, nil
}

func (e *Executor) executeAndNot(must []Query, mustNot []Query) ([]Result, error) {
	positiveResults, err := e.executeAnd(must)
	if err != nil {
		return nil, err
	}
	if len(positiveResults) == 0 {
		return nil, nil
	}

	excludeSet := make(map[string]bool)
	for _, q := range mustNot {
		results, err := e.Execute(q)
		if err != nil {
			return nil, err
		}
		for _, r := range results {
			excludeSet[r.DocID] = true
		}
	}

	var filtered []Result
	for _, r := range positiveResults {
		if !excludeSet[r.DocID] {
			filtered = append(filtered, r)
		}
	}

	return filtered, nil
}

func (e *Executor) executeOrNot(should []Query, mustNot []Query) ([]Result, error) {
	positiveResults, err := e.executeOr(should)
	if err != nil {
		return nil, err
	}
	if len(positiveResults) == 0 {
		return nil, nil
	}

	excludeSet := make(map[string]bool)
	for _, q := range mustNot {
		results, err := e.Execute(q)
		if err != nil {
			return nil, err
		}
		for _, r := range results {
			excludeSet[r.DocID] = true
		}
	}

	var filtered []Result
	for _, r := range positiveResults {
		if !excludeSet[r.DocID] {
			filtered = append(filtered, r)
		}
	}

	return filtered, nil
}

func (e *Executor) executeComplex(must []Query, should []Query, mustNot []Query) ([]Result, error) {
	andResults, err := e.executeAnd(must)
	if err != nil {
		return nil, err
	}
	if len(andResults) == 0 {
		return nil, nil
	}

	candidates := make(map[string]Result)
	for _, r := range andResults {
		candidates[r.DocID] = r
	}

	orResults, err := e.executeOr(should)
	if err != nil {
		return nil, err
	}

	orSet := make(map[string]Result)
	for _, r := range orResults {
		orSet[r.DocID] = r
	}

	for docID, r := range candidates {
		if other, ok := orSet[docID]; ok {
			r.Score += other.Score
			candidates[docID] = r
		} else {
			delete(candidates, docID)
		}
	}

	if len(mustNot) > 0 {
		excludeSet := make(map[string]bool)
		for _, q := range mustNot {
			results, err := e.Execute(q)
			if err != nil {
				return nil, err
			}
			for _, r := range results {
				excludeSet[r.DocID] = true
			}
		}

		for docID := range candidates {
			if excludeSet[docID] {
				delete(candidates, docID)
			}
		}
	}

	results := make([]Result, 0, len(candidates))
	for _, r := range candidates {
		results = append(results, r)
	}

	sortByScore(results)
	return results, nil
}

func sortByScore(results []Result) {
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Score > results[i].Score {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}
