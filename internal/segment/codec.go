package segment

import (
	"bytes"
	"encoding/binary"

	"github.com/RoaringBitmap/roaring"
)

// Segment file format constants
const (
	SegmentMagic   = "ZAP\x00"
	SegmentVersion = uint32(1)
	ChunkSize      = 1024 // Documents per chunk for stored fields
)

type Posting struct {
	DocNum    uint64
	Frequency uint64
	Positions []uint64
}

type Footer struct {
	StoredFieldsOffset uint64              `json:"stored_offset"`
	FieldsIndexOffset  uint64              `json:"fields_offset"`
	ChunkOffsets       []uint64            `json:"chunks"`
	FieldsMeta         []FieldMeta         `json:"fields"`
	DocIDs             []string            `json:"doc_ids"`
	NumDocs            uint64              `json:"num_docs"`
	FieldLengths       map[string][]uint64 `json:"field_lengths,omitempty"`
}

type FieldMeta struct {
	Name           string `json:"name"`
	DictOffset     uint64 `json:"dict_offset"`
	DictSize       uint64 `json:"dict_size"`
	PostingsOffset uint64 `json:"postings_offset"`
	PostingsSize   uint64 `json:"postings_size"`
	TotalTokens    uint64 `json:"total_tokens,omitempty"`
	DocCount       uint64 `json:"doc_count,omitempty"`
}

// EncodePostings encodes a posting list with delta encoding.
func EncodePostings(postings []Posting) []byte {
	buf := make([]byte, 0, len(postings)*32)
	tmp := make([]byte, binary.MaxVarintLen64)

	n := binary.PutUvarint(tmp, uint64(len(postings)))
	buf = append(buf, tmp[:n]...)

	var prevDocNum uint64
	for _, p := range postings {
		delta := p.DocNum - prevDocNum
		n = binary.PutUvarint(tmp, delta)
		buf = append(buf, tmp[:n]...)
		prevDocNum = p.DocNum
	}

	for _, p := range postings {
		n = binary.PutUvarint(tmp, p.Frequency)
		buf = append(buf, tmp[:n]...)
	}

	for _, p := range postings {
		n = binary.PutUvarint(tmp, uint64(len(p.Positions)))
		buf = append(buf, tmp[:n]...)

		var prevPos uint64
		for _, pos := range p.Positions {
			delta := pos - prevPos
			n = binary.PutUvarint(tmp, delta)
			buf = append(buf, tmp[:n]...)
			prevPos = pos
		}
	}

	return buf
}

// DecodePostings decodes a posting list.
func DecodePostings(data []byte) ([]Posting, error) {
	r := bytes.NewReader(data)

	count, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	postings := make([]Posting, count)

	var prevDocNum uint64
	for i := uint64(0); i < count; i++ {
		delta, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		postings[i].DocNum = prevDocNum + delta
		prevDocNum = postings[i].DocNum
	}

	for i := uint64(0); i < count; i++ {
		freq, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		postings[i].Frequency = freq
	}

	for i := uint64(0); i < count; i++ {
		posCount, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		postings[i].Positions = make([]uint64, posCount)

		var prevPos uint64
		for j := uint64(0); j < posCount; j++ {
			delta, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, err
			}
			postings[i].Positions[j] = prevPos + delta
			prevPos = postings[i].Positions[j]
		}
	}

	return postings, nil
}

// DecodePostingsBitmap decodes only the docNums from a posting list into a bitmap.
// This is faster than full decoding when freq/positions aren't needed.
func DecodePostingsBitmap(data []byte, deleted *roaring.Bitmap) (*roaring.Bitmap, error) {
	r := newByteReader(data)

	count, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}

	bm := roaring.New()
	var prevDocNum uint64
	for i := uint64(0); i < count; i++ {
		delta, err := r.ReadUvarint()
		if err != nil {
			return nil, err
		}
		docNum := prevDocNum + delta
		prevDocNum = docNum

		if deleted == nil || !deleted.Contains(uint32(docNum)) {
			bm.Add(uint32(docNum))
		}
	}

	return bm, nil
}
