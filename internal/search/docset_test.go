package search

import (
	"testing"

	"harshagw/postings/internal/index"

	"github.com/RoaringBitmap/roaring"
)

// createTestDocSets creates two test docSets with overlapping documents
func createTestDocSets(t *testing.T) (*docSet, *docSet, *index.IndexSnapshot) {
	t.Helper()
	snapshot := createTestSnapshot(t)

	// Create first docSet with doc1, doc2, doc3
	ds1 := newDocSet(snapshot)
	ds1.builderDocs.Add(0) // doc1
	ds1.builderDocs.Add(1) // doc2
	ds1.builderDocs.Add(2) // doc3

	// Create second docSet with doc2, doc3, doc4
	ds2 := newDocSet(snapshot)
	ds2.builderDocs.Add(1) // doc2
	ds2.builderDocs.Add(2) // doc3
	ds2.builderDocs.Add(3) // doc4

	return ds1, ds2, snapshot
}

func TestNewDocSet(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds := newDocSet(snapshot)
	if ds == nil {
		t.Fatal("newDocSet returned nil")
	}

	if ds.builderDocs == nil {
		t.Error("builderDocs not initialized")
	}

	if !ds.IsEmpty() {
		t.Error("new docSet should be empty")
	}
}

func TestDocSet_IsEmpty(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds := newDocSet(snapshot)

	if !ds.IsEmpty() {
		t.Error("new docSet should be empty")
	}

	ds.builderDocs.Add(1)
	if ds.IsEmpty() {
		t.Error("docSet with document should not be empty")
	}
}

func TestDocSet_Count(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds := newDocSet(snapshot)

	if ds.Count() != 0 {
		t.Errorf("expected count 0, got %d", ds.Count())
	}

	ds.builderDocs.Add(0)
	ds.builderDocs.Add(1)
	ds.builderDocs.Add(2)

	if ds.Count() != 3 {
		t.Errorf("expected count 3, got %d", ds.Count())
	}
}

func TestDocSet_Clone(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds := newDocSet(snapshot)
	ds.builderDocs.Add(0)
	ds.builderDocs.Add(1)

	clone := ds.Clone()

	if clone.Count() != ds.Count() {
		t.Error("clone should have same count")
	}

	// Modify original, clone should not be affected
	ds.builderDocs.Add(2)
	if clone.builderDocs.Contains(2) {
		t.Error("clone should not be affected by original modification")
	}
}

func TestDocSet_Intersect(t *testing.T) {
	ds1, ds2, snapshot := createTestDocSets(t)
	defer snapshot.Close()

	// ds1: {0, 1, 2}, ds2: {1, 2, 3}
	result := ds1.Intersect(ds2)

	// Intersection: {1, 2}
	if result.Count() != 2 {
		t.Errorf("expected count 2, got %d", result.Count())
	}

	if !result.builderDocs.Contains(1) || !result.builderDocs.Contains(2) {
		t.Error("intersection should contain 1 and 2")
	}

	if result.builderDocs.Contains(0) || result.builderDocs.Contains(3) {
		t.Error("intersection should not contain 0 or 3")
	}
}

func TestDocSet_Subtract(t *testing.T) {
	ds1, ds2, snapshot := createTestDocSets(t)
	defer snapshot.Close()

	// ds1: {0, 1, 2}, ds2: {1, 2, 3}
	result := ds1.Subtract(ds2)

	// ds1 - ds2 = {0}
	if result.Count() != 1 {
		t.Errorf("expected count 1, got %d", result.Count())
	}

	if !result.builderDocs.Contains(0) {
		t.Error("subtraction result should contain 0")
	}

	if result.builderDocs.Contains(1) || result.builderDocs.Contains(2) {
		t.Error("subtraction should remove common elements")
	}
}

func TestUnionAll(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds1 := newDocSet(snapshot)
	ds1.builderDocs.Add(0)

	ds2 := newDocSet(snapshot)
	ds2.builderDocs.Add(1)

	ds3 := newDocSet(snapshot)
	ds3.builderDocs.Add(2)

	result := unionAll([]*docSet{ds1, ds2, ds3})

	if result.Count() != 3 {
		t.Errorf("expected count 3, got %d", result.Count())
	}
}

func TestUnionAll_Empty(t *testing.T) {
	result := unionAll([]*docSet{})
	if result != nil {
		t.Error("unionAll of empty slice should return nil")
	}
}

func TestUnionAll_Single(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds := newDocSet(snapshot)
	ds.builderDocs.Add(0)

	result := unionAll([]*docSet{ds})
	if result != ds {
		t.Error("unionAll of single set should return that set")
	}
}

func TestUnionAll_WithOverlap(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds1 := newDocSet(snapshot)
	ds1.builderDocs.Add(0)
	ds1.builderDocs.Add(1)

	ds2 := newDocSet(snapshot)
	ds2.builderDocs.Add(1)
	ds2.builderDocs.Add(2)

	result := unionAll([]*docSet{ds1, ds2})

	// Union should have {0, 1, 2}
	if result.Count() != 3 {
		t.Errorf("expected count 3, got %d", result.Count())
	}
}

func TestIntersectAll(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds1 := newDocSet(snapshot)
	ds1.builderDocs.Add(0)
	ds1.builderDocs.Add(1)
	ds1.builderDocs.Add(2)

	ds2 := newDocSet(snapshot)
	ds2.builderDocs.Add(1)
	ds2.builderDocs.Add(2)

	ds3 := newDocSet(snapshot)
	ds3.builderDocs.Add(2)

	result := intersectAll([]*docSet{ds1, ds2, ds3})

	// Intersection should be {2}
	if result.Count() != 1 {
		t.Errorf("expected count 1, got %d", result.Count())
	}

	if !result.builderDocs.Contains(2) {
		t.Error("intersection should contain 2")
	}
}

func TestIntersectAll_Empty(t *testing.T) {
	result := intersectAll([]*docSet{})
	if result != nil {
		t.Error("intersectAll of empty slice should return nil")
	}
}

func TestIntersectAll_NoOverlap(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	ds1 := newDocSet(snapshot)
	ds1.builderDocs.Add(0)

	ds2 := newDocSet(snapshot)
	ds2.builderDocs.Add(1)

	result := intersectAll([]*docSet{ds1, ds2})

	if !result.IsEmpty() {
		t.Error("intersection with no overlap should be empty")
	}
}

// Mark unused import as used
var _ = roaring.New
