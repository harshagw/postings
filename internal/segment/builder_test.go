package segment

import (
	"os"
	"path/filepath"
	"testing"

	"harshagw/postings/internal/analysis"
)

func TestBuilder_Add_ReturnsDocNum(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())

	docNum0 := b.Add("doc1", map[string]any{"title": "first"})
	docNum1 := b.Add("doc2", map[string]any{"title": "second"})

	if docNum0 != 0 || docNum1 != 1 {
		t.Errorf("expected docNums 0,1 got %d,%d", docNum0, docNum1)
	}
	if b.NumDocs() != 2 {
		t.Errorf("expected 2 docs, got %d", b.NumDocs())
	}
}

func TestBuilder_Add_IndexesTerms(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "Hello World"})

	titleTerms := b.Fields["title"]
	if titleTerms == nil {
		t.Fatal("title field not indexed")
	}
	if _, ok := titleTerms["hello"]; !ok {
		t.Error("'hello' not indexed")
	}
	if _, ok := titleTerms["world"]; !ok {
		t.Error("'world' not indexed")
	}
}

func TestBuilder_Add_TracksPositions(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "go go go"})

	posting := b.Fields["title"]["go"][0]
	if posting.Frequency != 3 {
		t.Errorf("frequency: got %d, want 3", posting.Frequency)
	}
	if len(posting.Positions) != 3 {
		t.Errorf("positions count: got %d, want 3", len(posting.Positions))
	}
	// Positions should be 0, 1, 2
	for i, pos := range posting.Positions {
		if pos != uint64(i) {
			t.Errorf("position %d: got %d, want %d", i, pos, i)
		}
	}
}

func TestBuilder_Delete_MarksDeleted(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "hello"})
	b.Add("doc2", map[string]any{"title": "world"})

	if !b.Delete("doc1") {
		t.Error("Delete should return true")
	}
	if !b.IsDeleted(0) {
		t.Error("doc1 (docNum 0) should be marked deleted")
	}
	if b.IsDeleted(1) {
		t.Error("doc2 (docNum 1) should not be deleted")
	}
}

func TestBuilder_Delete_NonExistent(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "hello"})

	if b.Delete("nonexistent") {
		t.Error("Delete should return false for non-existent doc")
	}
}

func TestBuilder_Delete_CountBehavior(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "hello"})
	b.Add("doc2", map[string]any{"title": "world"})

	b.Delete("doc1")

	if b.NumDocs() != 1 {
		t.Errorf("NumDocs: got %d, want 1", b.NumDocs())
	}
	if b.TotalDocs() != 2 {
		t.Errorf("TotalDocs: got %d, want 2", b.TotalDocs())
	}
}

func TestBuilder_FieldLength_ReturnsTokenCount(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "one two three"}) // 3 tokens

	if fl := b.FieldLength("title", 0); fl != 3 {
		t.Errorf("FieldLength: got %d, want 3", fl)
	}
}

func TestBuilder_FieldLength_NonExistent(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "hello"})

	if fl := b.FieldLength("nonexistent", 0); fl != 0 {
		t.Errorf("FieldLength for non-existent: got %d, want 0", fl)
	}
}

func TestBuilder_AvgFieldLength(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "a b"})     // 2 tokens
	b.Add("doc2", map[string]any{"title": "a b c d"}) // 4 tokens

	avg := b.AvgFieldLength("title")
	if avg != 3.0 {
		t.Errorf("AvgFieldLength: got %.2f, want 3.0", avg)
	}
}

func TestBuilder_AvgFieldLength_ExcludesDeleted(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "a b"})     // 2 tokens
	b.Add("doc2", map[string]any{"title": "a b c d"}) // 4 tokens
	b.Delete("doc2")

	avg := b.AvgFieldLength("title")
	if avg != 2.0 {
		t.Errorf("AvgFieldLength after delete: got %.2f, want 2.0", avg)
	}
}

func TestBuilder_Build_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "hello"})

	segPath, err := b.Build(dir, "test")
	if err != nil {
		t.Fatalf("Build error: %v", err)
	}

	expected := filepath.Join(dir, "test.seg")
	if segPath != expected {
		t.Errorf("path: got %s, want %s", segPath, expected)
	}
	if _, err := os.Stat(segPath); os.IsNotExist(err) {
		t.Error("segment file not created")
	}
}

func TestBuilder_Build_SegmentOpenable(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{"title": "hello"})
	b.Add("doc2", map[string]any{"title": "world"})

	segPath, _ := b.Build(dir, "test")
	seg, err := Open(segPath, "test")
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer seg.Close()

	if seg.NumDocs() != 2 {
		t.Errorf("NumDocs: got %d, want 2", seg.NumDocs())
	}
}

func TestBuilder_Build_ExternalIDMapping(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(analysis.NewSimple())
	b.Add("my-doc-1", map[string]any{"title": "hello"})

	segPath, _ := b.Build(dir, "test")
	seg, _ := Open(segPath, "test")
	defer seg.Close()

	id, ok := seg.ExternalID(0)
	if !ok || id != "my-doc-1" {
		t.Errorf("ExternalID(0): got %q, want my-doc-1", id)
	}
}

// Builder ignores non-string fields
func TestBuilder_IgnoresNonStringFields(t *testing.T) {
	b := NewBuilder(analysis.NewSimple())
	b.Add("doc1", map[string]any{
		"title": "Hello",
		"count": 42,
		"score": 3.14,
	})

	// Only title and _id should be indexed
	if len(b.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(b.Fields))
	}
}
