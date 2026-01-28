package segment

import (
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func TestEncodeDecodePostings_Empty(t *testing.T) {
	encoded := EncodePostings([]Posting{})
	decoded, err := DecodePostings(encoded)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("expected empty, got %d postings", len(decoded))
	}
}

func TestEncodeDecodePostings_DeltaEncoding(t *testing.T) {
	// DocNums 1000, 1001, 2000 - tests delta encoding with large gaps
	postings := []Posting{
		{DocNum: 1000, Frequency: 1, Positions: []uint64{0}},
		{DocNum: 1001, Frequency: 1, Positions: []uint64{0}},
		{DocNum: 2000, Frequency: 1, Positions: []uint64{0}},
	}
	encoded := EncodePostings(postings)
	decoded, err := DecodePostings(encoded)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if decoded[0].DocNum != 1000 || decoded[1].DocNum != 1001 || decoded[2].DocNum != 2000 {
		t.Errorf("docNums mismatch: got %d, %d, %d", decoded[0].DocNum, decoded[1].DocNum, decoded[2].DocNum)
	}
}

func TestEncodeDecodePostings_FrequencyAndPositions(t *testing.T) {
	postings := []Posting{
		{DocNum: 0, Frequency: 3, Positions: []uint64{0, 5, 10}},
	}
	encoded := EncodePostings(postings)
	decoded, err := DecodePostings(encoded)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if decoded[0].Frequency != 3 {
		t.Errorf("frequency: got %d, want 3", decoded[0].Frequency)
	}
	if !reflect.DeepEqual(decoded[0].Positions, []uint64{0, 5, 10}) {
		t.Errorf("positions: got %v, want [0 5 10]", decoded[0].Positions)
	}
}

func TestDecodePostingsBitmap_AllDocs(t *testing.T) {
	postings := []Posting{
		{DocNum: 1, Frequency: 1, Positions: []uint64{0}},
		{DocNum: 5, Frequency: 1, Positions: []uint64{0}},
		{DocNum: 10, Frequency: 1, Positions: []uint64{0}},
	}
	encoded := EncodePostings(postings)

	bm, err := DecodePostingsBitmap(encoded, nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !bm.Contains(1) || !bm.Contains(5) || !bm.Contains(10) {
		t.Error("bitmap missing expected doc numbers")
	}
	if bm.GetCardinality() != 3 {
		t.Errorf("cardinality: got %d, want 3", bm.GetCardinality())
	}
}

func TestDecodePostingsBitmap_ExcludesDeleted(t *testing.T) {
	postings := []Posting{
		{DocNum: 1, Frequency: 1, Positions: []uint64{0}},
		{DocNum: 5, Frequency: 1, Positions: []uint64{0}},
		{DocNum: 10, Frequency: 1, Positions: []uint64{0}},
	}
	encoded := EncodePostings(postings)
	deleted := newTestBitmap(5)

	bm, err := DecodePostingsBitmap(encoded, deleted)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if bm.Contains(5) {
		t.Error("deleted doc 5 should be excluded")
	}
	if bm.GetCardinality() != 2 {
		t.Errorf("cardinality: got %d, want 2", bm.GetCardinality())
	}
}

func newTestBitmap(vals ...uint32) *roaring.Bitmap {
	bm := roaring.New()
	for _, v := range vals {
		bm.Add(v)
	}
	return bm
}
