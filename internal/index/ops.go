package index

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/RoaringBitmap/roaring"

	"harshagw/postings/internal/segment"
	"harshagw/postings/internal/store"
)

func (idx *Index) getDeletions(segID string) (*roaring.Bitmap, error) {
	persisted, err := idx.meta.GetDeletions(segID)
	if err != nil {
		return nil, err
	}
	if pending := idx.pendingDeletions[segID]; pending != nil {
		persisted.Or(pending)
	}
	return persisted, nil
}

func (idx *Index) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	return idx.flushInternal()
}

// flushInternal performs flush without locking.
func (idx *Index) flushInternal() error {
	if idx.builder.NumDocs() == 0 {
		return nil
	}

	currentSegmentIDs, err := idx.meta.GetSegments()
	if err != nil {
		return err
	}

	currentEpoch, err := idx.meta.GetEpoch()
	if err != nil {
		return err
	}
	segmentID := fmt.Sprintf("%012d", currentEpoch+1)

	segPath, err := idx.builder.Build(idx.dir, segmentID)
	if err != nil {
		return err
	}

	var epoch uint64
	err = idx.meta.Update(func(tx *store.Tx) error {
		epoch, err = tx.IncrementEpoch()
		if err != nil {
			return err
		}

		for segID, pending := range idx.pendingDeletions {
			if pending == nil || pending.IsEmpty() {
				continue
			}
			existing, err := tx.GetDeletions(segID)
			if err != nil {
				return err
			}
			existing.Or(pending)
			if err := tx.SetDeletions(segID, existing); err != nil {
				return err
			}
		}

		if !idx.builder.Deleted.IsEmpty() {
			if err := tx.SetDeletions(segmentID, idx.builder.Deleted); err != nil {
				return err
			}
		}

		return tx.SetSegments(append(currentSegmentIDs, segmentID))
	})
	if err != nil {
		os.Remove(segPath)
		return err
	}

	seg, err := segment.Open(segPath, segmentID)
	if err != nil {
		return err
	}

	idx.segments = append(idx.segments, seg)
	idx.epoch = epoch
	idx.pendingDeletions = make(map[string]*roaring.Bitmap)
	idx.builder = segment.NewBuilder(idx.analyzer)

	return nil
}

// Snapshot returns a point-in-time snapshot for searching.
func (idx *Index) Snapshot() (*IndexSnapshot, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	snapshots := make([]*SegmentSnapshot, len(idx.segments))
	for i, seg := range idx.segments {
		deleted, err := idx.getDeletions(seg.ID())
		if err != nil {
			return nil, err
		}
		snapshots[i] = &SegmentSnapshot{seg: seg, deleted: deleted}
	}

	return &IndexSnapshot{
		segments: snapshots,
		builder:  idx.builder,
		epoch:    idx.epoch,
		analyzer: idx.analyzer,
	}, nil
}

// Close closes the index and releases resources.
func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	idx.closed = true
	idx.pendingDeletions = nil
	idx.builder = nil

	for _, seg := range idx.segments {
		seg.Close()
	}
	idx.segments = nil

	if idx.meta != nil {
		idx.meta.Close()
	}

	return nil
}

// NumSegments returns the number of segments.
func (idx *Index) NumSegments() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.segments)
}

// SegmentInfo holds info about a segment.
type SegmentInfo struct {
	ID      string
	Path    string
	NumDocs uint64
}

// Segments returns info about all segments.
func (idx *Index) Segments() []SegmentInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	info := make([]SegmentInfo, len(idx.segments))
	for i, seg := range idx.segments {
		info[i] = SegmentInfo{
			ID:      seg.ID(),
			Path:    filepath.Join(idx.dir, seg.ID()+".seg"),
			NumDocs: seg.NumDocs(),
		}
	}
	return info
}

// SegmentStats holds detailed stats for a segment.
type SegmentStats struct {
	NumDocs    uint64
	NumDeleted uint64
	Fields     []string
}

// SegmentStats returns detailed stats for a segment.
func (idx *Index) SegmentStats(segID string) (*SegmentStats, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for _, seg := range idx.segments {
		if seg.ID() == segID {
			deleted, err := idx.getDeletions(segID)
			if err != nil {
				return nil, err
			}
			return &SegmentStats{
				NumDocs:    seg.NumDocs(),
				NumDeleted: deleted.GetCardinality(),
				Fields:     seg.Fields(),
			}, nil
		}
	}
	return nil, fmt.Errorf("segment not found: %s", segID)
}

// LoadDoc loads a document from a segment by docNum.
func (idx *Index) LoadDoc(segID string, docNum uint64) (map[string]any, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for _, seg := range idx.segments {
		if seg.ID() == segID {
			return seg.LoadDoc(docNum)
		}
	}
	return nil, fmt.Errorf("segment not found: %s", segID)
}

type PostingEntry struct {
	SegmentID string
	DocNum    uint64
	Freq      uint64
	Positions []uint64
}

// DumpPostings returns raw postings for a field:term across all segments.
func (idx *Index) DumpPostings(field, term string) ([]PostingEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []PostingEntry
	for _, seg := range idx.segments {
		postings, err := seg.Search(term, field, nil)
		if err != nil {
			continue
		}
		for _, p := range postings {
			results = append(results, PostingEntry{
				SegmentID: seg.ID(),
				DocNum:    p.DocNum,
				Freq:      p.Frequency,
				Positions: p.Positions,
			})
		}
	}
	return results, nil
}

// DumpDeletions returns the deleted docNums for a segment.
func (idx *Index) DumpDeletions(segID string) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	deleted, err := idx.getDeletions(segID)
	if err != nil {
		return nil, err
	}
	return deleted.ToArray(), nil
}

// ForceMerge merges all segments into one.
func (idx *Index) ForceMerge() error {
	idx.mu.RLock()
	segmentIDs := make([]string, len(idx.segments))
	for i, seg := range idx.segments {
		segmentIDs[i] = seg.ID()
	}
	idx.mu.RUnlock()

	if len(segmentIDs) < 2 {
		return nil
	}

	return idx.Merge(segmentIDs)
}
