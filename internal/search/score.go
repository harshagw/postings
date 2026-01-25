package search

import (
	"harshagw/postings/internal/index"
	"math"
)

// BM25 scoring constants.
const (
	BM25_k1 = 1.2
	BM25_b  = 0.75
)

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
