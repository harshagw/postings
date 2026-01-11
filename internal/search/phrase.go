package search

import (
	"harshagw/postings/internal/index"
	"harshagw/postings/internal/segment"
	"slices"
)

// PhraseSearch searches for an exact phrase in a field.
// If field is empty, searches all fields.
func (s *Searcher) PhraseSearch(phrase, field string) ([]Result, error) {
	tokens := s.snapshot.Analyzer().Analyze(phrase)

	if len(tokens) == 0 {
		return nil, nil
	}

	terms := make([]string, len(tokens))
	for i, t := range tokens {
		terms[i] = t.Token
	}

	if len(terms) == 1 {
		return s.Search(terms[0], field)
	}

	var matches []searchMatch
	seen := make(map[string]bool)

	fields := s.getFieldsToSearch(field)

	for _, f := range fields {
		segments := s.snapshot.Segments()
		for i := len(segments) - 1; i >= 0; i-- {
			segSnap := segments[i]
			segMatches := s.phraseMatchInSegment(segSnap, terms, f, seen)
			matches = append(matches, segMatches...)
		}

		if s.snapshot.Builder() != nil {
			builderMatches := s.phraseMatchInBuilder(terms, f, seen)
			matches = append(matches, builderMatches...)
		}
	}

	return s.scoreAndSort(matches, field), nil
}

func (s *Searcher) phraseMatchInSegment(segSnap *index.SegmentSnapshot, terms []string, field string, seen map[string]bool) []searchMatch {
	var matches []searchMatch
	seg := segSnap.Segment()

	termPostings := make([][]segment.Posting, len(terms))
	for i, term := range terms {
		postings, err := segSnap.Search(term, field)
		if err != nil || len(postings) == 0 {
			return matches
		}
		termPostings[i] = postings
	}

	docPositions := make(map[uint64][][]uint64)
	for _, p := range termPostings[0] {
		docPositions[p.DocNum] = make([][]uint64, len(terms))
	}

	for termIdx, postings := range termPostings {
		for _, p := range postings {
			if _, ok := docPositions[p.DocNum]; ok {
				docPositions[p.DocNum][termIdx] = p.Positions
			}
		}
	}

	for docNum, positions := range docPositions {
		valid := true
		for _, pos := range positions {
			if len(pos) == 0 {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}

		if phraseMatch(positions) {
			extID, ok := seg.ExternalID(docNum)
			if !ok || seen[extID] {
				continue
			}
			seen[extID] = true
			matches = append(matches, searchMatch{
				docID:       extID,
				tf:          1.0,
				fieldLength: seg.FieldLength(field, docNum),
				field:       field,
				segmentIdx:  -1,
			})
		}
	}

	return matches
}

func phraseMatch(positions [][]uint64) bool {
	if len(positions) == 0 {
		return false
	}

	for _, start := range positions[0] {
		ok := true
		for i := 1; i < len(positions); i++ {
			expectedPos := start + uint64(i)
			if !slices.Contains(positions[i], expectedPos) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func (s *Searcher) phraseMatchInBuilder(terms []string, field string, seen map[string]bool) []searchMatch {
	var matches []searchMatch
	builder := s.snapshot.Builder()

	fieldTerms, ok := builder.Fields[field]
	if !ok {
		return matches
	}

	termPostings := make([][]segment.Posting, len(terms))
	for i, term := range terms {
		postings, ok := fieldTerms[term]
		if !ok || len(postings) == 0 {
			return matches
		}
		termPostings[i] = postings
	}

	docPositions := make(map[uint64][][]uint64)
	for _, p := range termPostings[0] {
		if !builder.IsDeleted(p.DocNum) {
			docPositions[p.DocNum] = make([][]uint64, len(terms))
		}
	}

	for termIdx, postings := range termPostings {
		for _, p := range postings {
			if _, ok := docPositions[p.DocNum]; ok {
				docPositions[p.DocNum][termIdx] = p.Positions
			}
		}
	}

	for docNum, positions := range docPositions {
		valid := true
		for _, pos := range positions {
			if len(pos) == 0 {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}

		if phraseMatch(positions) {
			if docNum < uint64(len(builder.DocIDs)) {
				extID := builder.DocIDs[docNum]
				if !seen[extID] {
					seen[extID] = true
					matches = append(matches, searchMatch{
						docID:       extID,
						tf:          1.0,
						fieldLength: builder.FieldLength(field, docNum),
						field:       field,
						segmentIdx:  -1,
					})
				}
			}
		}
	}

	return matches
}
