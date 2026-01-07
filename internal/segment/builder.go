package segment

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/RoaringBitmap/roaring"

	"harshagw/postings/internal/analysis"
)

// Builder accumulates documents before flushing to an immutable segment.
type Builder struct {
	Fields   map[string]map[string][]Posting // field -> term -> postings
	Docs     []map[string]any                // stored documents
	DocIDs   []string                        // external IDs by docNum
	Deleted  *roaring.Bitmap                 // deleted docNums
	numDocs  uint64
	analyzer analysis.Analyzer
}

// NewBuilder creates a new segment builder.
func NewBuilder(analyzer analysis.Analyzer) *Builder {
	return &Builder{
		Fields:   make(map[string]map[string][]Posting),
		Docs:     make([]map[string]any, 0),
		DocIDs:   make([]string, 0),
		Deleted:  roaring.New(),
		numDocs:  0,
		analyzer: analyzer,
	}
}

// IDField is the special field name used to store document IDs for lookup.
const IDField = "_id"

// Add adds a document to the builder and returns its docNum.
func (b *Builder) Add(externalID string, doc map[string]any) uint64 {
	docNum := b.numDocs
	b.numDocs++

	b.Docs = append(b.Docs, doc)
	b.DocIDs = append(b.DocIDs, externalID)

	// Index _id field for DocNumbers lookup via FST
	if b.Fields[IDField] == nil {
		b.Fields[IDField] = make(map[string][]Posting)
	}
	b.Fields[IDField][externalID] = []Posting{{
		DocNum:    docNum,
		Frequency: 1,
		Positions: []uint64{1},
	}}

	// Index each user field
	for fieldName, value := range doc {
		text, ok := value.(string)
		if !ok {
			continue
		}

		if b.Fields[fieldName] == nil {
			b.Fields[fieldName] = make(map[string][]Posting)
		}

		tokens := b.analyzer.Analyze(text)

		// Group by term
		termPositions := make(map[string][]uint64)
		for _, tp := range tokens {
			termPositions[tp.Token] = append(termPositions[tp.Token], tp.Position)
		}

		for term, positions := range termPositions {
			b.Fields[fieldName][term] = append(b.Fields[fieldName][term], Posting{
				DocNum:    docNum,
				Frequency: uint64(len(positions)),
				Positions: positions,
			})
		}
	}

	return docNum
}

// Delete marks a document as deleted. Returns true if found.
func (b *Builder) Delete(externalID string) bool {
	for i, id := range b.DocIDs {
		if id == externalID && !b.Deleted.Contains(uint32(i)) {
			b.Deleted.Add(uint32(i))
			return true
		}
	}
	return false
}

// IsDeleted checks if a docNum is deleted.
func (b *Builder) IsDeleted(docNum uint64) bool {
	return b.Deleted.Contains(uint32(docNum))
}

// NumDocs returns the number of documents in the builder.
func (b *Builder) NumDocs() uint64 { return b.numDocs }

// Build writes the segment to disk and returns the segment path.
func (b *Builder) Build(dir, segmentID string) (string, error) {
	segPath := filepath.Join(dir, segmentID+".seg")
	tmpPath := segPath + ".tmp"

	file, err := os.Create(tmpPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Write header
	if _, err := file.WriteString(SegmentMagic); err != nil {
		return "", err
	}
	if err := binary.Write(file, binary.BigEndian, SegmentVersion); err != nil {
		return "", err
	}
	if err := binary.Write(file, binary.BigEndian, b.numDocs); err != nil {
		return "", err
	}

	// Reserve space for offsets
	offsetsPos, _ := file.Seek(0, 1)
	placeholder := make([]byte, 16)
	file.Write(placeholder)

	// Write stored fields
	storedFieldsOffset, _ := file.Seek(0, 1)
	chunkOffsets, err := b.writeStoredFields(file)
	if err != nil {
		return "", err
	}

	// Write fields index
	fieldsIndexOffset, _ := file.Seek(0, 1)
	fieldsMeta, err := b.writeFieldsIndex(file)
	if err != nil {
		return "", err
	}

	// Write footer with metadata
	footerOffset, _ := file.Seek(0, 1)
	footer := Footer{
		StoredFieldsOffset: uint64(storedFieldsOffset),
		FieldsIndexOffset:  uint64(fieldsIndexOffset),
		ChunkOffsets:       chunkOffsets,
		FieldsMeta:         fieldsMeta,
		DocIDs:             b.DocIDs,
		NumDocs:            b.numDocs,
	}
	footerData, err := json.Marshal(footer)
	if err != nil {
		return "", err
	}
	if _, err := file.Write(footerData); err != nil {
		return "", err
	}

	binary.Write(file, binary.BigEndian, uint64(footerOffset))
	binary.Write(file, binary.BigEndian, uint64(len(footerData)))

	// Go back and write the actual offsets
	file.Seek(offsetsPos, 0)
	binary.Write(file, binary.BigEndian, uint64(storedFieldsOffset))
	binary.Write(file, binary.BigEndian, uint64(fieldsIndexOffset))

	file.Close()

	if err := os.Rename(tmpPath, segPath); err != nil {
		return "", err
	}

	return segPath, nil
}
