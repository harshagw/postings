package search

import "strings"

// prefixSearch searches for documents containing terms that start with the given prefix.
func (s *Searcher) prefixSearch(prefix, field string) ([]Result, error) {
	seen := make(map[string]bool)
	var matches []searchMatch
	fields := s.getFieldsToSearch(field)

	// Search persisted segments - collect postings directly
	for i, segSnap := range s.snapshot.Segments() {
		seg := segSnap.Segment()
		for _, f := range fields {
			postings, err := seg.PrefixPostings(prefix, f, segSnap.Deleted())
			if err != nil {
				continue
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
					fieldLength: seg.FieldLength(f, p.DocNum),
					field:       f,
					segmentIdx:  i,
				})
			}
		}
	}

	// Search in-memory builder
	if builder := s.snapshot.Builder(); builder != nil {
		for _, f := range fields {
			if fieldTerms, ok := builder.Fields[f]; ok {
				for term, postings := range fieldTerms {
					if !strings.HasPrefix(term, prefix) {
						continue
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
									fieldLength: builder.FieldLength(f, p.DocNum),
									field:       f,
									segmentIdx:  -1,
								})
							}
						}
					}
				}
			}
		}
	}

	return s.scoreAndSort(matches, field), nil
}
