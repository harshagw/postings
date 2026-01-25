package search

import (
	"fmt"
	"slices"

	"harshagw/postings/internal/query"
)

func (s *Searcher) boolSearch(q *query.BoolQuery) ([]Result, error) {
	// Flatten the query: extract nested MustNot clauses from Must
	// This handles cases like "A AND NOT B" which parses as:
	// BoolQuery{Must: [A, BoolQuery{MustNot: [B]}]}
	must, mustNot, should := flattenBoolQuery(q)

	if len(must) == 0 && len(should) == 0 && len(mustNot) > 0 {
		return nil, fmt.Errorf("NOT queries require a positive clause")
	}

	if len(must) == 0 && len(should) > 0 && len(mustNot) == 0 {
		return s.executeOr(should)
	}

	if len(must) > 0 && len(should) == 0 && len(mustNot) == 0 {
		return s.executeAnd(must)
	}

	if len(must) > 0 && len(should) == 0 && len(mustNot) > 0 {
		return s.executeAndNot(must, mustNot)
	}

	if len(must) == 0 && len(should) > 0 && len(mustNot) > 0 {
		return s.executeOrNot(should, mustNot)
	}

	if len(must) > 0 && len(should) > 0 {
		return s.executeComplex(must, should, mustNot)
	}

	return nil, nil
}

// flattenBoolQuery extracts nested MustNot clauses from Must/Should.
// For example, "A AND NOT B" parses as BoolQuery{Must: [A, BoolQuery{MustNot: [B]}]}
// This flattens it to Must: [A], MustNot: [B]
func flattenBoolQuery(q *query.BoolQuery) (must, mustNot, should []query.Query) {
	mustNot = append(mustNot, q.MustNot...)
	should = append(should, q.Should...)

	for _, m := range q.Must {
		if bq, ok := m.(*query.BoolQuery); ok {
			// Check if this is a pure MustNot query (no Must/Should)
			if len(bq.Must) == 0 && len(bq.Should) == 0 && len(bq.MustNot) > 0 {
				// Hoist the MustNot clauses to the parent
				mustNot = append(mustNot, bq.MustNot...)
				continue
			}
		}
		must = append(must, m)
	}

	return must, mustNot, should
}

func (s *Searcher) executeOr(queries []query.Query) ([]Result, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	if len(queries) == 1 {
		return s.execute(queries[0])
	}

	// Get docSets for all queries
	var sets []*docSet
	for _, q := range queries {
		ds, err := s.executeQueryToDocSet(q)
		if err != nil {
			return nil, err
		}
		if !ds.IsEmpty() {
			sets = append(sets, ds)
		}
	}

	if len(sets) == 0 {
		return nil, nil
	}

	result := unionAll(sets)
	if result == nil || result.IsEmpty() {
		return nil, nil
	}

	return s.materializeResults(result, ""), nil
}

func (s *Searcher) executeAnd(queries []query.Query) ([]Result, error) {
	if len(queries) == 0 {
		return nil, nil
	}
	if len(queries) == 1 {
		return s.execute(queries[0])
	}

	// Get docSets for all queries
	var sets []*docSet
	for _, q := range queries {
		ds, err := s.executeQueryToDocSet(q)
		if err != nil {
			return nil, err
		}
		if ds.IsEmpty() {
			return nil, nil // AND with empty = empty
		}
		sets = append(sets, ds)
	}

	// Sort by count (smallest first) for optimal intersection
	slices.SortFunc(sets, func(a, b *docSet) int {
		return int(a.Count()) - int(b.Count())
	})

	result := intersectAll(sets)
	if result == nil || result.IsEmpty() {
		return nil, nil
	}

	return s.materializeResults(result, ""), nil
}

// collectDocSets executes queries and collects their docSets.
// If requireNonEmpty is true, returns nil on first empty set (for AND semantics).
func (s *Searcher) collectDocSets(queries []query.Query, requireNonEmpty bool) ([]*docSet, error) {
	var sets []*docSet
	for _, q := range queries {
		ds, err := s.executeQueryToDocSet(q)
		if err != nil {
			return nil, err
		}
		if requireNonEmpty && ds.IsEmpty() {
			return nil, nil // AND with empty = empty
		}
		if !ds.IsEmpty() {
			sets = append(sets, ds)
		}
	}
	return sets, nil
}

// subtractNot subtracts mustNot docs from result.
func (s *Searcher) subtractNot(result *docSet, mustNot []query.Query) (*docSet, error) {
	if len(mustNot) == 0 {
		return result, nil
	}
	notSets, err := s.collectDocSets(mustNot, false)
	if err != nil {
		return nil, err
	}
	if len(notSets) > 0 {
		return result.Subtract(unionAll(notSets)), nil
	}
	return result, nil
}

func (s *Searcher) executeAndNot(must []query.Query, mustNot []query.Query) ([]Result, error) {
	mustSets, err := s.collectDocSets(must, true)
	if err != nil || mustSets == nil {
		return nil, err
	}

	result := intersectAll(mustSets)
	if result == nil || result.IsEmpty() {
		return nil, nil
	}

	result, err = s.subtractNot(result, mustNot)
	if err != nil || result.IsEmpty() {
		return nil, err
	}

	return s.materializeResults(result, ""), nil
}

func (s *Searcher) executeOrNot(should []query.Query, mustNot []query.Query) ([]Result, error) {
	shouldSets, err := s.collectDocSets(should, false)
	if err != nil || len(shouldSets) == 0 {
		return nil, err
	}

	result := unionAll(shouldSets)
	if result == nil || result.IsEmpty() {
		return nil, nil
	}

	result, err = s.subtractNot(result, mustNot)
	if err != nil || result.IsEmpty() {
		return nil, err
	}

	return s.materializeResults(result, ""), nil
}

func (s *Searcher) executeComplex(must []query.Query, should []query.Query, mustNot []query.Query) ([]Result, error) {
	mustSets, err := s.collectDocSets(must, true)
	if err != nil || mustSets == nil {
		return nil, err
	}

	result := intersectAll(mustSets)
	if result == nil || result.IsEmpty() {
		return nil, nil
	}

	// Intersect with OR of should queries
	shouldSets, err := s.collectDocSets(should, false)
	if err != nil {
		return nil, err
	}
	if len(shouldSets) > 0 {
		result = result.Intersect(unionAll(shouldSets))
	}
	if result.IsEmpty() {
		return nil, nil
	}

	result, err = s.subtractNot(result, mustNot)
	if err != nil || result.IsEmpty() {
		return nil, err
	}

	return s.materializeResults(result, ""), nil
}

// executeQueryToDocSet executes a query and returns results as a docSet.
// This allows set-based boolean operations on any query type.
func (s *Searcher) executeQueryToDocSet(q query.Query) (*docSet, error) {
	if q == nil {
		return newDocSet(s.snapshot), nil
	}
	switch v := q.(type) {
	case *query.TermQuery:
		return s.termDocSet(v.Term, v.Field), nil
	case *query.PhraseQuery, *query.PrefixQuery, *query.RegexQuery, *query.FuzzyQuery, *query.BoolQuery:
		// Execute query, convert results to docSet
		results, err := s.execute(q)
		if err != nil {
			return nil, err
		}
		return s.resultsToDocSet(results), nil
	default:
		return nil, fmt.Errorf("unknown query type: %T", q)
	}
}

// resultsToDocSet converts a slice of Results to a docSet.
func (s *Searcher) resultsToDocSet(results []Result) *docSet {
	ds := newDocSet(s.snapshot)
	if len(results) == 0 {
		return ds
	}

	// Build docID -> docNum mapping for each segment and builder
	for _, r := range results {
		// Check each segment for this docID
		for i, segSnap := range s.snapshot.Segments() {
			seg := segSnap.Segment()
			if docNum, ok := seg.DocNum(r.DocID); ok {
				if segSnap.Deleted() == nil || !segSnap.Deleted().Contains(uint32(docNum)) {
					ds.segmentDocs[i].docs.Add(uint32(docNum))
					break // Found in this segment, no need to check others
				}
			}
		}
		// Check builder
		if builder := s.snapshot.Builder(); builder != nil {
			for docNum, docID := range builder.DocIDs {
				if docID == r.DocID && !builder.IsDeleted(uint64(docNum)) {
					ds.builderDocs.Add(uint32(docNum))
					break
				}
			}
		}
	}

	return ds
}
