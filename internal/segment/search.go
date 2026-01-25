package segment

import (
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/couchbase/vellum"
	"github.com/couchbase/vellum/levenshtein"
	"github.com/couchbase/vellum/regexp"
)

// getFieldMeta returns metadata for a field using O(1) map lookup.
func (s *Segment) getFieldMeta(fieldName string) *FieldMeta {
	return s.fieldMetaByName[fieldName]
}

// getFST returns the FST for a field, loading it lazily.
func (s *Segment) getFST(fieldName string) (*vellum.FST, error) {
	s.fstsMu.RLock()
	fst, ok := s.fsts[fieldName]
	s.fstsMu.RUnlock()
	if ok {
		return fst, nil
	}

	s.fstsMu.Lock()
	defer s.fstsMu.Unlock()

	// Double-check after acquiring write lock
	if fst, ok := s.fsts[fieldName]; ok {
		return fst, nil
	}

	meta := s.getFieldMeta(fieldName)
	if meta == nil {
		return nil, fmt.Errorf("field not found: %s", fieldName)
	}

	// FST data starts after the 8-byte size prefix
	fstOffset := meta.DictOffset
	fstSize := binary.BigEndian.Uint64(s.data[fstOffset:])
	fstData := s.data[fstOffset+8 : fstOffset+8+fstSize]

	fst, err := vellum.Load(fstData)
	if err != nil {
		return nil, fmt.Errorf("failed to load FST for field %s: %w", fieldName, err)
	}

	s.fsts[fieldName] = fst
	return fst, nil
}

// Search searches for a term in a specific field.
func (s *Segment) Search(term, fieldName string, deleted *roaring.Bitmap) ([]Posting, error) {
	fst, err := s.getFST(fieldName)
	if err != nil {
		return nil, err
	}

	val, exists, err := fst.Get([]byte(term))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	meta := s.getFieldMeta(fieldName)
	postingsOffset := meta.PostingsOffset + val
	postings, err := decodePostings(s.data[postingsOffset:])
	if err != nil {
		return nil, err
	}

	// Filter deleted documents
	if deleted != nil && !deleted.IsEmpty() {
		filtered := make([]Posting, 0, len(postings))
		for _, p := range postings {
			if !deleted.Contains(uint32(p.DocNum)) {
				filtered = append(filtered, p)
			}
		}
		return filtered, nil
	}

	return postings, nil
}

// SearchBitmap returns just the docNum bitmap for a term (no freq/positions).
func (s *Segment) SearchBitmap(term, fieldName string, deleted *roaring.Bitmap) (*roaring.Bitmap, error) {
	fst, err := s.getFST(fieldName)
	if err != nil {
		return nil, err
	}

	val, exists, err := fst.Get([]byte(term))
	if err != nil {
		return nil, err
	}
	if !exists {
		return roaring.New(), nil
	}

	meta := s.getFieldMeta(fieldName)
	postingsOffset := meta.PostingsOffset + val
	return DecodePostingsBitmap(s.data[postingsOffset:], deleted)
}

// decodePostings decodes a posting list from segment data.
func decodePostings(data []byte) ([]Posting, error) {
	return DecodePostings(data)
}

// searchWithAutomaton is a helper that searches FST using any vellum automaton.
func (s *Segment) searchWithAutomaton(fieldName string, aut vellum.Automaton) ([]string, error) {
	fst, err := s.getFST(fieldName)
	if err != nil {
		return nil, err
	}

	iter, err := fst.Search(aut, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to search FST: %w", err)
	}

	var terms []string
	for err == nil {
		key, _ := iter.Current()
		terms = append(terms, string(key))
		err = iter.Next()
	}

	if err != vellum.ErrIteratorDone {
		return nil, err
	}

	return terms, nil
}

// MatchingTerms returns all terms in a field that match the given regex pattern.
func (s *Segment) MatchingTerms(pattern, fieldName string) ([]string, error) {
	aut, err := regexp.New(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}
	return s.searchWithAutomaton(fieldName, aut)
}

// FuzzyTerms returns all terms in a field within edit distance of the query.
func (s *Segment) FuzzyTerms(term string, fuzziness uint8, fieldName string) ([]string, error) {
	builder, err := levenshtein.NewLevenshteinAutomatonBuilder(fuzziness, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create levenshtein builder: %w", err)
	}

	aut, err := builder.BuildDfa(term, fuzziness)
	if err != nil {
		return nil, fmt.Errorf("failed to build fuzzy automaton: %w", err)
	}

	return s.searchWithAutomaton(fieldName, aut)
}

// PrefixPostings returns all postings for terms matching the prefix.
// More efficient than PrefixTerms + multiple Search calls.
func (s *Segment) PrefixPostings(prefix, fieldName string, deleted *roaring.Bitmap) ([]Posting, error) {
	fst, err := s.getFST(fieldName)
	if err != nil {
		return nil, err
	}

	meta := s.getFieldMeta(fieldName)
	if meta == nil {
		return nil, fmt.Errorf("field not found: %s", fieldName)
	}

	start := []byte(prefix)
	end := prefixSuccessor(start)

	iter, err := fst.Iterator(start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}

	docPostings := make(map[uint64]Posting)

	for err == nil {
		_, val := iter.Current()

		postingsOffset := meta.PostingsOffset + val
		postings, decodeErr := decodePostings(s.data[postingsOffset:])
		if decodeErr == nil {
			for _, p := range postings {
				if deleted != nil && deleted.Contains(uint32(p.DocNum)) {
					continue
				}
				if existing, ok := docPostings[p.DocNum]; ok {
					existing.Frequency += p.Frequency
					docPostings[p.DocNum] = existing
				} else {
					docPostings[p.DocNum] = p
				}
			}
		}

		err = iter.Next()
	}

	if err != vellum.ErrIteratorDone {
		return nil, err
	}

	// Convert map to slice
	result := make([]Posting, 0, len(docPostings))
	for _, p := range docPostings {
		result = append(result, p)
	}

	return result, nil
}
