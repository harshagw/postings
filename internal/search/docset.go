package search

import (
	"github.com/RoaringBitmap/roaring"

	"harshagw/postings/internal/index"
)

// segmentDocSet holds matching documents for a single segment.
type segmentDocSet struct {
	docs    *roaring.Bitmap
	segSnap *index.SegmentSnapshot
}

// docSet holds matching documents across all segments and the builder.
// Each bitmap contains docNums local to that segment/builder.
type docSet struct {
	segmentDocs []segmentDocSet
	builderDocs *roaring.Bitmap
	snapshot    *index.IndexSnapshot
}

// newDocSet creates an empty docSet for the given snapshot.
func newDocSet(snapshot *index.IndexSnapshot) *docSet {
	segments := snapshot.Segments()
	ds := &docSet{
		segmentDocs: make([]segmentDocSet, len(segments)),
		builderDocs: roaring.New(),
		snapshot:    snapshot,
	}
	for i, segSnap := range segments {
		ds.segmentDocs[i] = segmentDocSet{
			docs:    roaring.New(),
			segSnap: segSnap,
		}
	}
	return ds
}

// IsEmpty returns true if all bitmaps are empty.
func (ds *docSet) IsEmpty() bool {
	for _, sd := range ds.segmentDocs {
		if !sd.docs.IsEmpty() {
			return false
		}
	}
	return ds.builderDocs.IsEmpty()
}

// Count returns the total number of documents across all segments.
func (ds *docSet) Count() uint64 {
	var count uint64
	for _, sd := range ds.segmentDocs {
		count += sd.docs.GetCardinality()
	}
	count += ds.builderDocs.GetCardinality()
	return count
}

// Clone creates a deep copy of the docSet.
func (ds *docSet) Clone() *docSet {
	result := &docSet{
		segmentDocs: make([]segmentDocSet, len(ds.segmentDocs)),
		builderDocs: ds.builderDocs.Clone(),
		snapshot:    ds.snapshot,
	}
	for i, sd := range ds.segmentDocs {
		result.segmentDocs[i] = segmentDocSet{
			docs:    sd.docs.Clone(),
			segSnap: sd.segSnap,
		}
	}
	return result
}

// Intersect performs intersection with another docSet.
func (ds *docSet) Intersect(other *docSet) *docSet {
	result := &docSet{
		segmentDocs: make([]segmentDocSet, len(ds.segmentDocs)),
		builderDocs: roaring.And(ds.builderDocs, other.builderDocs),
		snapshot:    ds.snapshot,
	}
	for i := range ds.segmentDocs {
		result.segmentDocs[i] = segmentDocSet{
			docs:    roaring.And(ds.segmentDocs[i].docs, other.segmentDocs[i].docs),
			segSnap: ds.segmentDocs[i].segSnap,
		}
	}
	return result
}

// Subtract removes documents that are in the other docSet.
func (ds *docSet) Subtract(other *docSet) *docSet {
	result := &docSet{
		segmentDocs: make([]segmentDocSet, len(ds.segmentDocs)),
		builderDocs: roaring.AndNot(ds.builderDocs, other.builderDocs),
		snapshot:    ds.snapshot,
	}
	for i := range ds.segmentDocs {
		result.segmentDocs[i] = segmentDocSet{
			docs:    roaring.AndNot(ds.segmentDocs[i].docs, other.segmentDocs[i].docs),
			segSnap: ds.segmentDocs[i].segSnap,
		}
	}
	return result
}

// materializeResults converts a docSet to a slice of Results.
// It retrieves the external document IDs and calculates scores.
func (s *Searcher) materializeResults(ds *docSet, field string) []Result {
	if ds == nil || ds.IsEmpty() {
		return nil
	}

	var matches []searchMatch
	seen := make(map[string]bool)

	// Materialize from segments (newest to oldest for proper deduplication)
	segments := s.snapshot.Segments()
	for i := len(segments) - 1; i >= 0; i-- {
		segSnap := segments[i]
		seg := segSnap.Segment()
		docs := ds.segmentDocs[i].docs

		iter := docs.Iterator()
		for iter.HasNext() {
			docNum := uint64(iter.Next())
			extID, ok := seg.ExternalID(docNum)
			if !ok || seen[extID] {
				continue
			}
			seen[extID] = true

			// Get field length for scoring
			fieldLen := uint64(0)
			if field != "" {
				fieldLen = seg.FieldLength(field, docNum)
			}

			matches = append(matches, searchMatch{
				docID:       extID,
				tf:          1.0, // Default TF for docSet results
				fieldLength: fieldLen,
				field:       field,
				segmentIdx:  i,
			})
		}
	}

	// Materialize from builder
	if builder := s.snapshot.Builder(); builder != nil {
		iter := ds.builderDocs.Iterator()
		for iter.HasNext() {
			docNum := uint64(iter.Next())
			if builder.IsDeleted(docNum) {
				continue
			}
			if docNum < uint64(len(builder.DocIDs)) {
				extID := builder.DocIDs[docNum]
				if !seen[extID] {
					seen[extID] = true

					fieldLen := uint64(0)
					if field != "" {
						fieldLen = builder.FieldLength(field, docNum)
					}

					matches = append(matches, searchMatch{
						docID:       extID,
						tf:          1.0,
						fieldLength: fieldLen,
						field:       field,
						segmentIdx:  -1,
					})
				}
			}
		}
	}

	return s.scoreAndSort(matches, field)
}

// unionAll performs fast union of multiple docSets.
// Uses roaring.FastOr for better performance with many sets.
func unionAll(sets []*docSet) *docSet {
	if len(sets) == 0 {
		return nil
	}
	if len(sets) == 1 {
		return sets[0]
	}

	result := &docSet{
		segmentDocs: make([]segmentDocSet, len(sets[0].segmentDocs)),
		snapshot:    sets[0].snapshot,
	}

	// Collect builder bitmaps
	builderBitmaps := make([]*roaring.Bitmap, 0, len(sets))
	for _, ds := range sets {
		if !ds.builderDocs.IsEmpty() {
			builderBitmaps = append(builderBitmaps, ds.builderDocs)
		}
	}
	if len(builderBitmaps) > 0 {
		result.builderDocs = roaring.FastOr(builderBitmaps...)
	} else {
		result.builderDocs = roaring.New()
	}

	// Union each segment's docs
	for i := range result.segmentDocs {
		segBitmaps := make([]*roaring.Bitmap, 0, len(sets))
		for _, ds := range sets {
			if !ds.segmentDocs[i].docs.IsEmpty() {
				segBitmaps = append(segBitmaps, ds.segmentDocs[i].docs)
			}
		}
		if len(segBitmaps) > 0 {
			result.segmentDocs[i] = segmentDocSet{
				docs:    roaring.FastOr(segBitmaps...),
				segSnap: sets[0].segmentDocs[i].segSnap,
			}
		} else {
			result.segmentDocs[i] = segmentDocSet{
				docs:    roaring.New(),
				segSnap: sets[0].segmentDocs[i].segSnap,
			}
		}
	}

	return result
}

// intersectAll performs fast intersection of multiple docSets.
// Assumes sets are sorted by count (smallest first) for optimal performance.
func intersectAll(sets []*docSet) *docSet {
	if len(sets) == 0 {
		return nil
	}
	if len(sets) == 1 {
		return sets[0]
	}

	// Start with smallest set and iteratively intersect
	result := sets[0].Clone()
	for i := 1; i < len(sets); i++ {
		result = result.Intersect(sets[i])
		// Early exit if empty
		if result.IsEmpty() {
			return result
		}
	}

	return result
}
