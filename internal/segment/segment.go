package segment

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/couchbase/vellum"
	"github.com/edsrzf/mmap-go"
	"github.com/golang/snappy"
)

// Segment represents an immutable, mmap'd segment.
type Segment struct {
	id     string
	path   string
	file   *os.File
	data   mmap.MMap
	footer Footer

	fieldMetaByName map[string]*FieldMeta

	fsts   map[string]*vellum.FST
	fstsMu sync.RWMutex
}

// Open opens an existing segment file with mmap.
func Open(path, segmentID string) (*Segment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %s: %w", path, err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if stat.Size() < int64(len(SegmentMagic)+4+8+16) {
		file.Close()
		return nil, fmt.Errorf("segment file too small: %s", path)
	}

	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap segment %s: %w", path, err)
	}

	// Verify magic
	if string(data[:len(SegmentMagic)]) != SegmentMagic {
		data.Unmap()
		file.Close()
		return nil, fmt.Errorf("invalid segment magic: %s", path)
	}

	// Read footer offset and size from end of file
	footerOffset := binary.BigEndian.Uint64(data[len(data)-16 : len(data)-8])
	footerSize := binary.BigEndian.Uint64(data[len(data)-8:])

	// Parse footer
	var footer Footer
	footerData := data[footerOffset : footerOffset+footerSize]
	if err := json.Unmarshal(footerData, &footer); err != nil {
		data.Unmap()
		file.Close()
		return nil, fmt.Errorf("failed to parse segment footer: %w", err)
	}

	// Build O(1) field metadata lookup map
	fieldMetaByName := make(map[string]*FieldMeta, len(footer.FieldsMeta))
	for i := range footer.FieldsMeta {
		fieldMetaByName[footer.FieldsMeta[i].Name] = &footer.FieldsMeta[i]
	}

	return &Segment{
		id:              segmentID,
		path:            path,
		file:            file,
		data:            data,
		footer:          footer,
		fieldMetaByName: fieldMetaByName,
		fsts:            make(map[string]*vellum.FST),
	}, nil
}

// ID returns the segment ID.
func (s *Segment) ID() string { return s.id }

// Path returns the segment file path.
func (s *Segment) Path() string { return s.path }

// NumDocs returns the total number of documents.
func (s *Segment) NumDocs() uint64 { return s.footer.NumDocs }

// ExternalID returns the external ID for a given docNum.
func (s *Segment) ExternalID(docNum uint64) (string, bool) {
	if docNum >= s.footer.NumDocs {
		return "", false
	}
	return s.footer.DocIDs[docNum], true
}

// DocNumbers returns a bitmap of docNums for the given external IDs.
// Uses FST lookup on the _id field for each ID.
func (s *Segment) DocNumbers(externalIDs []string) *roaring.Bitmap {
	bm := roaring.New()

	fst, err := s.getFST(IDField)
	if err != nil {
		return bm
	}

	for _, id := range externalIDs {
		val, exists, err := fst.Get([]byte(id))
		if err != nil || !exists {
			continue
		}

		// _id terms are always 1-hit encoded
		if IsOneHit(val) {
			bm.Add(uint32(DecodeOneHit(val)))
		}
	}
	return bm
}

// Fields returns the list of indexed field names.
func (s *Segment) Fields() []string {
	fields := make([]string, len(s.footer.FieldsMeta))
	for i, fm := range s.footer.FieldsMeta {
		fields[i] = fm.Name
	}
	return fields
}

// FieldLength returns the length of a field in a document.
func (s *Segment) FieldLength(field string, docNum uint64) uint64 {
	if s.footer.FieldLengths == nil {
		return 0
	}
	if lengths, ok := s.footer.FieldLengths[field]; ok && docNum < uint64(len(lengths)) {
		return lengths[docNum]
	}
	return 0
}

// AvgFieldLength returns the average length of a field.
func (s *Segment) AvgFieldLength(field string) float64 {
	meta, ok := s.fieldMetaByName[field]
	if !ok || meta.DocCount == 0 {
		return 0
	}
	return float64(meta.TotalTokens) / float64(meta.DocCount)
}

// LoadDoc loads a document by docNum from stored fields.
func (s *Segment) LoadDoc(docNum uint64) (map[string]any, error) {
	if docNum >= s.footer.NumDocs {
		return nil, fmt.Errorf("docNum %d out of range", docNum)
	}

	// Find the chunk containing this document
	chunkIdx := docNum / ChunkSize
	if int(chunkIdx) >= len(s.footer.ChunkOffsets) {
		return nil, fmt.Errorf("chunk index out of range")
	}

	offset := s.footer.ChunkOffsets[chunkIdx]

	// Read chunk length
	chunkLen := binary.BigEndian.Uint32(s.data[offset:])
	compressedData := s.data[offset+4 : offset+4+uint64(chunkLen)]

	// Decompress
	decompressed, err := snappy.Decode(nil, compressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress chunk: %w", err)
	}

	// Parse chunk
	var chunk []map[string]any
	if err := json.Unmarshal(decompressed, &chunk); err != nil {
		return nil, fmt.Errorf("failed to parse chunk: %w", err)
	}

	// Return the specific document
	docInChunk := docNum % ChunkSize
	if int(docInChunk) >= len(chunk) {
		return nil, fmt.Errorf("document index out of range in chunk")
	}

	return chunk[docInChunk], nil
}

// Close releases segment resources.
func (s *Segment) Close() error {
	s.fstsMu.Lock()
	defer s.fstsMu.Unlock()

	for _, fst := range s.fsts {
		fst.Close()
	}
	s.fsts = nil

	if s.data != nil {
		s.data.Unmap()
	}
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
