package search

import (
	"math"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/segment"
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

type searchMatch struct {
	docID       string
	tf          float64
	fieldLength uint64
	field       string
	segmentIdx  int
}

// Search searches for a term, optionally in a specific field.
func (s *Searcher) Search(term, field string) ([]Result, error) {
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

func (s *Searcher) scoreAndSort(matches []searchMatch, field string) []Result {
	totalDocs := s.snapshot.TotalDocs()
	df := uint64(len(matches))

	results := make([]Result, len(matches))

	if s.snapshot.ScoringMode() == index.ScoringBM25 {
		// Cache avg field lengths per field for multi-field searches
		avgFieldLengthCache := make(map[string]float64)
		getAvgFieldLength := func(f string) float64 {
			if avg, ok := avgFieldLengthCache[f]; ok {
				return avg
			}
			avg := s.snapshot.AvgFieldLength(f)
			if avg == 0 {
				avg = 1
			}
			avgFieldLengthCache[f] = avg
			return avg
		}

		idf := math.Log(1 + (float64(totalDocs)-float64(df)+0.5)/(float64(df)+0.5))

		for i, m := range matches {
			// Use the field from the match, fall back to provided field
			matchField := m.field
			if matchField == "" {
				matchField = field
			}
			avgFieldLength := getAvgFieldLength(matchField)

			fieldLen := float64(m.fieldLength)
			if fieldLen == 0 {
				fieldLen = avgFieldLength
			}
			tf := m.tf
			score := idf * (tf * (BM25_k1 + 1)) / (tf + BM25_k1*(1-BM25_b+BM25_b*fieldLen/avgFieldLength))
			results[i] = Result{
				DocID: m.docID,
				Score: score,
			}
		}
	} else {
		idf := math.Log(float64(totalDocs+1)/float64(df+1)) + 1.0
		for i, m := range matches {
			var tf float64
			if m.tf > 0 {
				tf = 1.0 + math.Log(m.tf)
			}
			score := tf * idf
			results[i] = Result{
				DocID: m.docID,
				Score: score,
			}
		}
	}

	sortByScore(results)
	return results
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

// getFieldsToSearch returns fields to search.
func (s *Searcher) getFieldsToSearch(field string) []string {
	if field != "" {
		return []string{field}
	}

	fieldSet := make(map[string]bool)

	for _, segSnap := range s.snapshot.Segments() {
		for _, f := range segSnap.Segment().Fields() {
			if f != "_id" {
				fieldSet[f] = true
			}
		}
	}

	if s.snapshot.Builder() != nil {
		for f := range s.snapshot.Builder().Fields {
			if f != "_id" {
				fieldSet[f] = true
			}
		}
	}

	fields := make([]string, 0, len(fieldSet))
	for f := range fieldSet {
		fields = append(fields, f)
	}
	return fields
}
