package segment

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"sort"

	"github.com/couchbase/vellum"
	"github.com/golang/snappy"
)

// writeStoredFields writes chunked, compressed stored documents.
func (b *Builder) writeStoredFields(file *os.File) ([]uint64, error) {
	var chunkOffsets []uint64

	for i := 0; i < len(b.Docs); i += ChunkSize {
		end := i + ChunkSize
		if end > len(b.Docs) {
			end = len(b.Docs)
		}
		chunk := b.Docs[i:end]

		// Serialize chunk
		chunkData, err := json.Marshal(chunk)
		if err != nil {
			return nil, err
		}

		// Compress with snappy
		compressed := snappy.Encode(nil, chunkData)

		// Record offset
		offset, err := file.Seek(0, 1)
		if err != nil {
			return nil, err
		}
		chunkOffsets = append(chunkOffsets, uint64(offset))

		// Write length + compressed data
		if err := binary.Write(file, binary.BigEndian, uint32(len(compressed))); err != nil {
			return nil, err
		}
		if _, err := file.Write(compressed); err != nil {
			return nil, err
		}
	}

	return chunkOffsets, nil
}

// writeFieldsIndex writes the FST dictionary and postings for each field.
func (b *Builder) writeFieldsIndex(file *os.File) ([]FieldMeta, error) {
	var fieldsMeta []FieldMeta

	// Get sorted field names
	fieldNames := make([]string, 0, len(b.Fields))
	for name := range b.Fields {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	for _, fieldName := range fieldNames {
		terms := b.Fields[fieldName]
		meta, err := b.writeFieldIndex(file, fieldName, terms)
		if err != nil {
			return nil, err
		}
		fieldsMeta = append(fieldsMeta, meta)
	}

	return fieldsMeta, nil
}

// writeFieldIndex writes FST and postings for a single field.
func (b *Builder) writeFieldIndex(file *os.File, fieldName string, terms map[string][]Posting) (FieldMeta, error) {
	meta := FieldMeta{Name: fieldName}

	// Get sorted terms
	termList := make([]string, 0, len(terms))
	for term := range terms {
		termList = append(termList, term)
	}
	sort.Strings(termList)

	// Write postings first, collect offsets
	postingsStart, _ := file.Seek(0, 1)
	meta.PostingsOffset = uint64(postingsStart)

	termOffsets := make(map[string]uint64)
	for _, term := range termList {
		postings := terms[term]

		// Sort postings by docNum
		sort.Slice(postings, func(i, j int) bool {
			return postings[i].DocNum < postings[j].DocNum
		})

		offset, _ := file.Seek(0, 1)
		relOffset := uint64(offset) - meta.PostingsOffset

		termOffsets[term] = relOffset
		encoded := EncodePostings(postings)
		if _, err := file.Write(encoded); err != nil {
			return meta, err
		}
	}

	postingsEnd, _ := file.Seek(0, 1)
	meta.PostingsSize = uint64(postingsEnd) - meta.PostingsOffset

	// Write FST dictionary
	dictStart, _ := file.Seek(0, 1)
	meta.DictOffset = uint64(dictStart)

	var fstBuf bytes.Buffer
	fstBuilder, err := vellum.New(&fstBuf, nil)
	if err != nil {
		return meta, err
	}

	for _, term := range termList {
		if err := fstBuilder.Insert([]byte(term), termOffsets[term]); err != nil {
			return meta, err
		}
	}
	if err := fstBuilder.Close(); err != nil {
		return meta, err
	}

	// Write FST size and data
	binary.Write(file, binary.BigEndian, uint64(fstBuf.Len()))
	file.Write(fstBuf.Bytes())

	dictEnd, _ := file.Seek(0, 1)
	meta.DictSize = uint64(dictEnd) - meta.DictOffset

	return meta, nil
}
