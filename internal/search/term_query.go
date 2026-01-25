package search

import (
	"harshagw/postings/internal/index"
	"harshagw/postings/internal/segment"
)

type searchMatch struct {
	docID       string
	tf          float64
	fieldLength uint64
	field       string
	segmentIdx  int
}

// search searches for a term, optionally in a specific field.
func (s *Searcher) termSearch(term, field string) ([]Result, error) {
	seen := make(map[string]bool)
	var matches []searchMatch

	segments := s.snapshot.Segments()
	for i := len(segments) - 1; i >= 0; i-- {
		segSnap := segments[i]
		segMatches := s.searchSegment(segSnap, term, field, i, seen)
		matches = append(matches, segMatches...)
	}

	if s.snapshot.Builder() != nil {
		builderMatches := s.searchBuilder(term, field, seen)
		matches = append(matches, builderMatches...)
	}

	return s.scoreAndSort(matches, field), nil
}

func (s *Searcher) searchSegment(segSnap *index.SegmentSnapshot, term, field string, segIdx int, seen map[string]bool) []searchMatch {
	var matches []searchMatch
	seg := segSnap.Segment()

	fields := []string{field}
	if field == "" {
		fields = seg.Fields()
	}

	for _, f := range fields {
		fieldMatches := s.searchSegmentField(segSnap, seg, term, f, segIdx, seen)
		matches = append(matches, fieldMatches...)
	}

	return matches
}

func (s *Searcher) searchSegmentField(segSnap *index.SegmentSnapshot, seg *segment.Segment, term, field string, segIdx int, seen map[string]bool) []searchMatch {
	var matches []searchMatch

	postings, err := segSnap.Search(term, field)
	if err != nil || len(postings) == 0 {
		return matches
	}

	for _, p := range postings {
		extID, ok := seg.ExternalID(p.DocNum)
		if !ok || seen[extID] {
			continue
		}
		seen[extID] = true
		matches = append(matches, searchMatch{
			docID:       extID,
			tf:          float64(p.Frequency),
			fieldLength: seg.FieldLength(field, p.DocNum),
			field:       field,
			segmentIdx:  segIdx,
		})
	}

	return matches
}

func (s *Searcher) searchBuilder(term, field string, seen map[string]bool) []searchMatch {
	var matches []searchMatch
	builder := s.snapshot.Builder()

	if field != "" {
		matches = s.searchBuilderField(builder, term, field, seen)
	} else {
		for fieldName := range builder.Fields {
			fieldMatches := s.searchBuilderField(builder, term, fieldName, seen)
			matches = append(matches, fieldMatches...)
		}
	}

	return matches
}

func (s *Searcher) searchBuilderField(builder *segment.Builder, term, field string, seen map[string]bool) []searchMatch {
	var matches []searchMatch

	fieldTerms, ok := builder.Fields[field]
	if !ok {
		return matches
	}

	postings, ok := fieldTerms[term]
	if !ok {
		return matches
	}

	for _, p := range postings {
		if builder.IsDeleted(p.DocNum) {
			continue
		}
		if p.DocNum < uint64(len(builder.DocIDs)) {
			extID := builder.DocIDs[p.DocNum]
			if !seen[extID] {
				seen[extID] = true
				matches = append(matches, searchMatch{
					docID:       extID,
					tf:          float64(p.Frequency),
					fieldLength: builder.FieldLength(field, p.DocNum),
					field:       field,
					segmentIdx:  -1,
				})
			}
		}
	}

	return matches
}
