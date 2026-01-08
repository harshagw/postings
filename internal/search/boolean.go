package search

// AndSearch returns documents that contain ALL of the given terms.
// If field is empty, searches all fields.
func (s *Searcher) AndSearch(terms []string, field string) ([]Result, error) {
	if len(terms) == 0 {
		return nil, nil
	}

	if len(terms) == 1 {
		return s.Search(terms[0], field)
	}

	firstResults, err := s.Search(terms[0], field)
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

	for _, term := range terms[1:] {
		termResults, err := s.Search(term, field)
		if err != nil {
			return nil, err
		}

		termDocIDs := make(map[string]Result)
		for _, r := range termResults {
			termDocIDs[r.DocID] = r
		}

		for docID, r := range candidates {
			if termResult, ok := termDocIDs[docID]; ok {
				// Sum scores from both terms
				r.Score += termResult.Score
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

// OrSearch returns documents that contain ANY of the given terms.
// If field is empty, searches all fields.
func (s *Searcher) OrSearch(terms []string, field string) ([]Result, error) {
	if len(terms) == 0 {
		return nil, nil
	}

	docScores := make(map[string]Result)

	for _, term := range terms {
		termResults, err := s.Search(term, field)
		if err != nil {
			return nil, err
		}

		for _, r := range termResults {
			if existing, ok := docScores[r.DocID]; ok {
				existing.Score += r.Score
				existing.MatchedTerms = append(existing.MatchedTerms, term)
				docScores[r.DocID] = existing
			} else {
				r.MatchedTerms = []string{term}
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
