package search

import (
	"regexp"

	"harshagw/postings/internal/segment"
)

// regexSearch searches for documents containing terms that match the regex pattern.
func (s *Searcher) regexSearch(pattern, field string) ([]Result, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return s.automatonSearch(field,
		func(seg *segment.Segment, f string) ([]string, error) {
			return seg.MatchingTerms(pattern, f)
		},
		func(term string) bool {
			return re.MatchString(term)
		},
	)
}

// fuzzySearch searches for documents containing terms within edit distance of the query.
func (s *Searcher) fuzzySearch(term string, fuzziness uint8, field string) ([]Result, error) {
	return s.automatonSearch(field,
		func(seg *segment.Segment, f string) ([]string, error) {
			return seg.FuzzyTerms(term, fuzziness, f)
		},
		func(candidate string) bool {
			return levenshteinDistance(term, candidate) <= int(fuzziness)
		},
	)
}

// termMatcher is a function that checks if a term matches a pattern.
type termMatcher func(term string) bool

// segmentTermFinder extracts matching terms from a segment for a given field.
type segmentTermFinder func(seg *segment.Segment, field string) ([]string, error)

// termSearch is a generic search that finds terms using segment and builder matchers.
func (s *Searcher) automatonSearch(field string, segFinder segmentTermFinder, builderMatcher termMatcher) ([]Result, error) {
	matchingTerms := make(map[string]bool)
	fields := s.getFieldsToSearch(field)

	// Search persisted segments
	for _, segSnap := range s.snapshot.Segments() {
		seg := segSnap.Segment()
		for _, f := range fields {
			terms, err := segFinder(seg, f)
			if err != nil {
				continue
			}
			for _, term := range terms {
				matchingTerms[term] = true
			}
		}
	}

	// Search in-memory builder
	if builder := s.snapshot.Builder(); builder != nil {
		for _, f := range fields {
			if fieldTerms, ok := builder.Fields[f]; ok {
				for term := range fieldTerms {
					if builderMatcher(term) {
						matchingTerms[term] = true
					}
				}
			}
		}
	}

	if len(matchingTerms) == 0 {
		return nil, nil
	}

	terms := make([]string, 0, len(matchingTerms))
	for term := range matchingTerms {
		terms = append(terms, term)
	}

	return s.multiTermSearch(terms, field), nil
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

// termDocSet searches for a term and returns a docSet.
func (s *Searcher) termDocSet(term, field string) *docSet {
	ds := newDocSet(s.snapshot)
	fields := s.getFieldsToSearch(field)

	// Search persisted segments
	for i, segSnap := range s.snapshot.Segments() {
		seg := segSnap.Segment()
		for _, f := range fields {
			bm, err := seg.SearchBitmap(term, f, segSnap.Deleted())
			if err != nil || bm.IsEmpty() {
				continue
			}
			ds.segmentDocs[i].docs.Or(bm)
		}
	}

	// Search in-memory builder
	if builder := s.snapshot.Builder(); builder != nil {
		for _, f := range fields {
			if fieldTerms, ok := builder.Fields[f]; ok {
				if postings, ok := fieldTerms[term]; ok {
					for _, p := range postings {
						if !builder.IsDeleted(p.DocNum) {
							ds.builderDocs.Add(uint32(p.DocNum))
						}
					}
				}
			}
		}
	}

	return ds
}

// multiTermSearch searches for multiple terms and returns results as OR of all.
func (s *Searcher) multiTermSearch(terms []string, field string) []Result {
	if len(terms) == 0 {
		return nil
	}
	if len(terms) == 1 {
		results, _ := s.termSearch(terms[0], field)
		return results
	}

	// Get docSets for all terms
	var sets []*docSet
	for _, term := range terms {
		ds := s.termDocSet(term, field)
		if !ds.IsEmpty() {
			sets = append(sets, ds)
		}
	}

	if len(sets) == 0 {
		return nil
	}

	result := unionAll(sets)
	if result == nil || result.IsEmpty() {
		return nil
	}

	return s.materializeResults(result, field)
}
