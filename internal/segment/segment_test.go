package segment

import (
	"slices"
	"testing"

	"harshagw/postings/internal/analysis"
)

// Helper to create a test segment with known data
func makeSegment(t *testing.T, docs map[string]map[string]any) *Segment {
	t.Helper()
	dir := t.TempDir()
	b := NewBuilder(analysis.NewSimple())
	for id, doc := range docs {
		b.Add(id, doc)
	}
	segPath, err := b.Build(dir, "test")
	if err != nil {
		t.Fatalf("Build error: %v", err)
	}
	seg, err := Open(segPath, "test")
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	return seg
}

func TestSegment_Search_FindsTerm(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello world"},
		"doc2": {"title": "hello there"},
	})
	defer seg.Close()

	postings, err := seg.Search("hello", "title", nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(postings) != 2 {
		t.Errorf("expected 2 postings, got %d", len(postings))
	}
}

func TestSegment_Search_NonExistentTerm(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello"},
	})
	defer seg.Close()

	postings, err := seg.Search("missing", "title", nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(postings) != 0 {
		t.Errorf("expected 0 postings, got %d", len(postings))
	}
}

func TestSegment_Search_ExcludesDeleted(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello"},
		"doc2": {"title": "hello"},
	})
	defer seg.Close()

	deleted := newTestBitmap(0) // delete doc1
	postings, err := seg.Search("hello", "title", deleted)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(postings) != 1 {
		t.Errorf("expected 1 posting after deletion, got %d", len(postings))
	}
	if postings[0].DocNum != 1 {
		t.Errorf("expected docNum 1, got %d", postings[0].DocNum)
	}
}

func TestSegment_PrefixPostings_MatchesPrefix(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "program"},
		"doc2": {"title": "programming"},
		"doc3": {"title": "programmer"},
	})
	defer seg.Close()

	postings, err := seg.PrefixPostings("prog", "title", nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(postings) != 3 {
		t.Errorf("expected 3 docs matching 'prog*', got %d", len(postings))
	}
}

func TestSegment_PrefixPostings_ExcludesDeleted(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello"},
		"doc2": {"title": "help"},
	})
	defer seg.Close()

	deleted := newTestBitmap(0)
	postings, err := seg.PrefixPostings("hel", "title", deleted)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(postings) != 1 {
		t.Errorf("expected 1 after deletion, got %d", len(postings))
	}
}

func TestSegment_FuzzyTerms_MatchesWithinDistance(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello"},
	})
	defer seg.Close()

	terms, err := seg.FuzzyTerms("hallo", 1, "title") // 1 edit: a->e
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !slices.Contains(terms, "hello") {
		t.Errorf("expected 'hello' for 'hallo' with fuzziness 1, got %v", terms)
	}
}

func TestSegment_FuzzyTerms_RespectsFuzziness(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello"},
	})
	defer seg.Close()

	// fuzziness 0 = exact match only
	terms, err := seg.FuzzyTerms("hello", 0, "title")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !slices.Contains(terms, "hello") {
		t.Error("exact term should match with fuzziness 0")
	}

	// "hallo" requires 1 edit, won't match with fuzziness 0
	terms, _ = seg.FuzzyTerms("hallo", 0, "title")
	if slices.Contains(terms, "hello") {
		t.Error("'hallo' should NOT match 'hello' with fuzziness 0")
	}
}

func TestSegment_MatchingTerms_ReturnsMatches(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "go"},
		"doc2": {"title": "world"},
	})
	defer seg.Close()

	terms, err := seg.MatchingTerms("go|world", "title")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !slices.Contains(terms, "go") || !slices.Contains(terms, "world") {
		t.Errorf("expected [go, world], got %v", terms)
	}
}

func TestSegment_MatchingTerms_InvalidRegex(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello"},
	})
	defer seg.Close()

	_, err := seg.MatchingTerms("[invalid", "title")
	if err == nil {
		t.Error("expected error for invalid regex")
	}
}

func TestSegment_LoadDoc_ReturnsFields(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello", "body": "world"},
	})
	defer seg.Close()

	doc, err := seg.LoadDoc(0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if doc["title"] != "hello" || doc["body"] != "world" {
		t.Errorf("unexpected doc: %v", doc)
	}
}

func TestSegment_LoadDoc_PreservesTypes(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello", "count": float64(42), "flag": true},
	})
	defer seg.Close()

	doc, err := seg.LoadDoc(0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if doc["count"] != float64(42) {
		t.Errorf("count: got %v (type %T)", doc["count"], doc["count"])
	}
	if doc["flag"] != true {
		t.Errorf("flag: got %v", doc["flag"])
	}
}

func TestSegment_ExternalID_Mapping(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"my-doc-1": {"title": "hello"},
	})
	defer seg.Close()

	id, ok := seg.ExternalID(0)
	if !ok || id != "my-doc-1" {
		t.Errorf("ExternalID(0): got %q, ok=%v", id, ok)
	}
}

func TestSegment_DocNum_Mapping(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"my-doc-1": {"title": "hello"},
	})
	defer seg.Close()

	docNum, ok := seg.DocNum("my-doc-1")
	if !ok || docNum != 0 {
		t.Errorf("DocNum('my-doc-1'): got %d, ok=%v", docNum, ok)
	}

	_, ok = seg.DocNum("nonexistent")
	if ok {
		t.Error("expected ok=false for nonexistent doc")
	}
}

func TestSegment_FieldLength_ReturnsTokenCount(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "one two three"},
	})
	defer seg.Close()

	if fl := seg.FieldLength("title", 0); fl != 3 {
		t.Errorf("FieldLength: got %d, want 3", fl)
	}
}

func TestSegment_AvgFieldLength_ComputesAverage(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "a b"},     // 2 tokens
		"doc2": {"title": "a b c d"}, // 4 tokens
	})
	defer seg.Close()

	avg := seg.AvgFieldLength("title")
	if avg != 3.0 {
		t.Errorf("AvgFieldLength: got %.2f, want 3.0", avg)
	}
}

func TestSegment_Search_FrequencyAndPositions(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "go is go and go"}, // "go" at positions 0, 2, 4
	})
	defer seg.Close()

	postings, err := seg.Search("go", "title", nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(postings) != 1 {
		t.Fatalf("expected 1 posting, got %d", len(postings))
	}

	p := postings[0]
	if p.Frequency != 3 {
		t.Errorf("frequency: got %d, want 3", p.Frequency)
	}
	if len(p.Positions) != 3 {
		t.Errorf("positions: got %d, want 3", len(p.Positions))
	}
}

func TestSegment_Size_ReturnsPositive(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello"},
	})
	defer seg.Close()

	if seg.Size() <= 0 {
		t.Errorf("Size: got %d, want > 0", seg.Size())
	}
}

func TestSegment_Open_NonExistentFile(t *testing.T) {
	_, err := Open("/nonexistent/path.seg", "test")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestSegment_Search_NonExistentField(t *testing.T) {
	seg := makeSegment(t, map[string]map[string]any{
		"doc1": {"title": "hello"},
	})
	defer seg.Close()

	_, err := seg.Search("hello", "nonexistent", nil)
	if err == nil {
		t.Error("expected error for non-existent field")
	}
}
