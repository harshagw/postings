package search

import (
	"regexp"
	"strings"

	"harshagw/postings/internal/segment"
)

// termMatcher is a function that checks if a term matches a pattern.
type termMatcher func(term string) bool

// segmentTermFinder extracts matching terms from a segment for a given field.
type segmentTermFinder func(seg *segment.Segment, field string) ([]string, error)

// termSearch is a generic search that finds terms using segment and builder matchers.
func (s *Searcher) termSearch(field string, segFinder segmentTermFinder, builderMatcher termMatcher) ([]Result, error) {
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

	return s.OrSearch(terms, field)
}

// RegexSearch searches for documents containing terms that match the regex pattern.
func (s *Searcher) RegexSearch(pattern, field string) ([]Result, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return s.termSearch(field,
		func(seg *segment.Segment, f string) ([]string, error) {
			return seg.MatchingTerms(pattern, f)
		},
		func(term string) bool {
			return re.MatchString(term)
		},
	)
}

// PrefixSearch searches for documents containing terms that start with the given prefix.
func (s *Searcher) PrefixSearch(prefix, field string) ([]Result, error) {
	return s.termSearch(field,
		func(seg *segment.Segment, f string) ([]string, error) {
			return seg.PrefixTerms(prefix, f)
		},
		func(term string) bool {
			return strings.HasPrefix(term, prefix)
		},
	)
}

// FuzzySearch searches for documents containing terms within edit distance of the query.
func (s *Searcher) FuzzySearch(term string, fuzziness uint8, field string) ([]Result, error) {
	return s.termSearch(field,
		func(seg *segment.Segment, f string) ([]string, error) {
			return seg.FuzzyTerms(term, fuzziness, f)
		},
		func(candidate string) bool {
			return levenshteinDistance(term, candidate) <= int(fuzziness)
		},
	)
}
