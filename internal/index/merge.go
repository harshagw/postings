package index

import (
	"fmt"
	"os"

	"harshagw/postings/internal/segment"
	"harshagw/postings/internal/store"
)

// Merge merges multiple segments into one.
func (idx *Index) Merge(segmentIDs []string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	if len(segmentIDs) < 2 {
		return fmt.Errorf("need at least 2 segments to merge")
	}

	idSet := make(map[string]bool)
	for _, id := range segmentIDs {
		idSet[id] = true
	}

	var segsToMerge []*SegmentSnapshot
	for _, seg := range idx.segments {
		if idSet[seg.ID()] {
			deleted, err := idx.meta.GetDeletions(seg.ID())
			if err != nil {
				return err
			}
			segsToMerge = append(segsToMerge, &SegmentSnapshot{seg: seg, deleted: deleted})
		}
	}

	if len(segsToMerge) != len(segmentIDs) {
		return fmt.Errorf("some segments not found")
	}

	builder := segment.NewBuilder(idx.analyzer)

	for _, ss := range segsToMerge {
		seg := ss.Segment()
		deleted := ss.Deleted()
		for docNum := uint64(0); docNum < seg.NumDocs(); docNum++ {
			if deleted != nil && deleted.Contains(uint32(docNum)) {
				continue
			}

			doc, err := seg.LoadDoc(docNum)
			if err != nil {
				continue
			}

			extID, ok := seg.ExternalID(docNum)
			if !ok {
				continue
			}

			builder.Add(extID, doc)
		}
	}

	currentEpoch, err := idx.meta.GetEpoch()
	if err != nil {
		return err
	}
	newSegmentID := fmt.Sprintf("%012d", currentEpoch+1)

	segPath, err := builder.Build(idx.dir, newSegmentID)
	if err != nil {
		return err
	}

	newSeg, err := segment.Open(segPath, newSegmentID)
	if err != nil {
		return err
	}

	newSegments := make([]*segment.Segment, 0, len(idx.segments)-len(segmentIDs)+1)
	removedPaths := make([]string, 0, len(segmentIDs))

	for _, seg := range idx.segments {
		if idSet[seg.ID()] {
			removedPaths = append(removedPaths, seg.Path())
			seg.Close()
		} else {
			newSegments = append(newSegments, seg)
		}
	}
	newSegments = append(newSegments, newSeg)

	var epoch uint64
	err = idx.meta.Update(func(tx *store.Tx) error {
		epoch, err = tx.IncrementEpoch()
		if err != nil {
			return err
		}

		for docNum, externalID := range builder.DocIDs {
			if err := tx.SetDocMapping(externalID, newSegmentID, uint64(docNum)); err != nil {
				return err
			}
		}

		for _, segID := range segmentIDs {
			if err := tx.DeleteDeletions(segID); err != nil {
				return err
			}
		}

		segmentIDList := make([]string, len(newSegments))
		for i, seg := range newSegments {
			segmentIDList[i] = seg.ID()
		}
		return tx.SetSegments(segmentIDList)
	})
	if err != nil {
		newSeg.Close()
		return err
	}

	idx.segments = newSegments
	idx.epoch = epoch

	for _, path := range removedPaths {
		os.Remove(path)
	}

	return nil
}
