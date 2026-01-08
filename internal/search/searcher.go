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
	docID      string
	tf         float64
	segmentIdx int
}

// Search searches for a term, optionally in a specific field.
func (s *Searcher) Search(term, field string) ([]Result, error) {
	seen := make(map[string]bool)
	var matches []searchMatch

	// Search persisted segments (newest first)
	segments := s.snapshot.Segments()
	for i := len(segments) - 1; i >= 0; i-- {
		segSnap := segments[i]
		segMatches := s.searchSegment(segSnap, term, field, i, seen)
		matches = append(matches, segMatches...)
	}

	// Search in-memory builder
	if s.snapshot.Builder() != nil {
		builderMatches := s.searchBuilder(term, field, seen)
		matches = append(matches, builderMatches...)
	}

	return s.scoreAndSort(matches), nil
}

// scoreAndSort calculates TF-IDF scores and sorts results.
func (s *Searcher) scoreAndSort(matches []searchMatch) []Result {
	totalDocs := s.snapshot.TotalDocs()
	df := uint64(len(matches))
	idf := math.Log(float64(totalDocs) / float64(df+1))

	results := make([]Result, len(matches))
	for i, m := range matches {
		score := m.tf * idf
		results[i] = Result{
			DocID: m.docID,
			Score: score,
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
			docID:      extID,
			tf:         float64(p.Frequency),
			segmentIdx: segIdx,
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
					docID:      extID,
					tf:         float64(p.Frequency),
					segmentIdx: -1,
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
