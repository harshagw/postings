package index

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/RoaringBitmap/roaring"

	"harshagw/postings/internal/analysis"
	"harshagw/postings/internal/segment"
	"harshagw/postings/internal/store"
)

type ScoringMode int

const (
	ScoringTFIDF ScoringMode = iota
	ScoringBM25
)

type Index struct {
	mu sync.RWMutex

	dir              string
	meta             *store.Metadata
	segments         []*segment.Segment
	builder          *segment.Builder
	epoch            uint64
	pendingDeletions map[string]*roaring.Bitmap

	analyzer       analysis.Analyzer
	flushThreshold int
	scoringMode    ScoringMode

	closed bool
}

type Config struct {
	Dir            string
	FlushThreshold int
	Analyzer       analysis.Analyzer
	ScoringMode    ScoringMode
}

func DefaultConfig(dir string) Config {
	return Config{
		Dir:            dir,
		FlushThreshold: 1000,
		Analyzer:       analysis.NewSimple(),
		ScoringMode:    ScoringBM25,
	}
}

// New creates or opens an index at the given directory.
func New(config Config) (*Index, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}

	meta, err := store.NewMetadata(config.Dir)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata store: %w", err)
	}

	idx := &Index{
		dir:              config.Dir,
		meta:             meta,
		segments:         make([]*segment.Segment, 0),
		pendingDeletions: make(map[string]*roaring.Bitmap),
		analyzer:         config.Analyzer,
		flushThreshold:   config.FlushThreshold,
		scoringMode:      config.ScoringMode,
	}

	idx.builder = segment.NewBuilder(idx.analyzer)

	if err := idx.loadSegments(); err != nil {
		meta.Close()
		return nil, fmt.Errorf("failed to load segments: %w", err)
	}

	idx.epoch, _ = meta.GetEpoch()

	return idx, nil
}

// loadSegments loads all segments from the metadata store.
func (idx *Index) loadSegments() error {
	segmentIDs, err := idx.meta.GetSegments()
	if err != nil {
		return err
	}

	for _, segID := range segmentIDs {
		segPath := filepath.Join(idx.dir, segID+".seg")
		seg, err := segment.Open(segPath, segID)
		if err != nil {
			return fmt.Errorf("failed to open segment %s: %w", segID, err)
		}
		idx.segments = append(idx.segments, seg)
	}

	return nil
}

// Index indexes a document.
func (idx *Index) Index(docID string, doc map[string]any) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	idx.builder.Delete(docID)
	idx.markObsoletes([]string{docID})
	idx.builder.Add(docID, doc)

	if idx.builder.NumDocs() >= uint64(idx.flushThreshold) {
		return idx.flushInternal()
	}

	return nil
}

func (idx *Index) Delete(docID string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	idx.builder.Delete(docID)
	idx.markObsoletes([]string{docID})
	return nil
}

// markObsoletes updates deletion bitmaps for docs in persisted segments.
func (idx *Index) markObsoletes(docIDs []string) {
	for _, seg := range idx.segments {
		obsoletes := seg.DocNumbers(docIDs)
		if obsoletes.IsEmpty() {
			continue
		}
		segID := seg.ID()
		if idx.pendingDeletions[segID] == nil {
			idx.pendingDeletions[segID] = roaring.New()
		}
		idx.pendingDeletions[segID].Or(obsoletes)
	}
}
