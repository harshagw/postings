package index

import (
	"github.com/RoaringBitmap/roaring"

	"harshagw/postings/internal/analysis"
	"harshagw/postings/internal/segment"
)

// SegmentSnapshot represents a segment with its deletion bitmap.
type SegmentSnapshot struct {
	seg     *segment.Segment
	deleted *roaring.Bitmap
}

// Segment returns the underlying segment.
func (s *SegmentSnapshot) Segment() *segment.Segment { return s.seg }

// Deleted returns the deletion bitmap.
func (s *SegmentSnapshot) Deleted() *roaring.Bitmap { return s.deleted }

// ID returns the segment ID.
func (s *SegmentSnapshot) ID() string { return s.seg.ID() }

// Search searches for a term in a field.
func (s *SegmentSnapshot) Search(term, field string) ([]segment.Posting, error) {
	return s.seg.Search(term, field, s.deleted)
}

// IndexSnapshot represents a point-in-time view of the index for searching.
type IndexSnapshot struct {
	segments    []*SegmentSnapshot
	builder     *segment.Builder
	epoch       uint64
	analyzer    analysis.Analyzer
	scoringMode ScoringMode
}

// Segments returns the segment snapshots.
func (s *IndexSnapshot) Segments() []*SegmentSnapshot { return s.segments }

// Builder returns the in-memory segment builder (may be nil).
func (s *IndexSnapshot) Builder() *segment.Builder { return s.builder }

// Analyzer returns the index's analyzer.
func (s *IndexSnapshot) Analyzer() analysis.Analyzer { return s.analyzer }

// ScoringMode returns the scoring mode for this snapshot.
func (s *IndexSnapshot) ScoringMode() ScoringMode { return s.scoringMode }

// TotalDocs returns the total number of documents across all segments.
func (s *IndexSnapshot) TotalDocs() uint64 {
	var total uint64
	for _, seg := range s.segments {
		total += seg.seg.NumDocs()
		if seg.deleted != nil {
			total -= seg.deleted.GetCardinality()
		}
	}
	if s.builder != nil {
		total += s.builder.NumDocs()
	}
	return total
}

// AvgFieldLength returns the average length of a field across all segments.
func (s *IndexSnapshot) AvgFieldLength(field string) float64 {
	var totalTokens uint64
	var docCount uint64

	for _, seg := range s.segments {
		avg := seg.seg.AvgFieldLength(field)
		if avg > 0 {
			numDocs := seg.seg.NumDocs()
			if seg.deleted != nil {
				numDocs -= seg.deleted.GetCardinality()
			}
			totalTokens += uint64(avg * float64(numDocs))
			docCount += numDocs
		}
	}

	if s.builder != nil {
		avg := s.builder.AvgFieldLength(field)
		if avg > 0 {
			totalTokens += uint64(avg * float64(s.builder.NumDocs()))
			docCount += s.builder.NumDocs()
		}
	}

	if docCount == 0 {
		return 0
	}
	return float64(totalTokens) / float64(docCount)
}

// Close releases snapshot resources.
func (s *IndexSnapshot) Close() error {
	return nil
}
