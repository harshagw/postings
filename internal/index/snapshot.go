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
	segments []*SegmentSnapshot
	builder  *segment.Builder
	epoch    uint64
	analyzer analysis.Analyzer
}

// Segments returns the segment snapshots.
func (s *IndexSnapshot) Segments() []*SegmentSnapshot { return s.segments }

// Builder returns the in-memory segment builder (may be nil).
func (s *IndexSnapshot) Builder() *segment.Builder { return s.builder }

// Analyzer returns the index's analyzer.
func (s *IndexSnapshot) Analyzer() analysis.Analyzer { return s.analyzer }

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

// Close releases snapshot resources.
func (s *IndexSnapshot) Close() error {
	// Snapshots don't own the segments, so nothing to close
	return nil
}
