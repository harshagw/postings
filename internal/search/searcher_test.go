package search

import (
	"testing"

	"harshagw/postings/internal/index"
)

// createTestSnapshot creates a test index snapshot with sample documents.
func createTestSnapshot(t *testing.T) *index.IndexSnapshot {
	t.Helper()
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	t.Cleanup(func() { idx.Close() })

	docs := []struct {
		id  string
		doc map[string]any
	}{
		{"doc1", map[string]any{"title": "Hello World", "body": "This is a test document."}},
		{"doc2", map[string]any{"title": "Go Programming", "body": "Learning Go programming."}},
		{"doc3", map[string]any{"title": "Hello Go", "body": "Hello from Go world."}},
	}

	for _, d := range docs {
		if err := idx.Index(d.id, d.doc); err != nil {
			t.Fatalf("Index error: %v", err)
		}
	}

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	return snapshot
}

// ============ Term Query E2E Tests ============

func TestE2E_TermQuery(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("hello")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for 'hello', got %d", len(results))
	}
}

func TestE2E_TermQuery_FieldSpecific(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("title:go")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for title:go, got %d", len(results))
	}
}

func TestE2E_TermQuery_NoMatch(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("nonexistent")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for nonexistent term, got %d", len(results))
	}
}

// ============ Phrase Query E2E Tests ============

func TestE2E_PhraseQuery(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString(`"hello world"`)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for phrase query, got %d", len(results))
	}
}

func TestE2E_PhraseQuery_NonAdjacent(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString(`"hello programming"`)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for non-adjacent phrase, got %d", len(results))
	}
}

// ============ Prefix Query E2E Tests ============

func TestE2E_PrefixQuery(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("prog*")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) < 1 {
		t.Errorf("expected at least 1 result for prog*, got %d", len(results))
	}
}

func TestE2E_PrefixQuery_FieldSpecific(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("title:hel*")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for title:hel*, got %d", len(results))
	}
}

// ============ Regex Query E2E Tests ============

func TestE2E_RegexQuery(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("/go|hello/")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) < 2 {
		t.Errorf("expected at least 2 results for /go|hello/, got %d", len(results))
	}
}

func TestE2E_RegexQuery_InvalidPattern(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	_, err := s.RunQueryString("/[invalid/")
	if err == nil {
		t.Error("expected error for invalid regex")
	}
}

// ============ Fuzzy Query E2E Tests ============

func TestE2E_FuzzyQuery(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	// "hallo" is 1 edit from "hello"
	results, err := s.RunQueryString("hallo~1")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) < 1 {
		t.Errorf("expected at least 1 result for hallo~1, got %d", len(results))
	}
}

func TestE2E_FuzzyQuery_FieldSpecific(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("title:worlf~1")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	found := false
	for _, r := range results {
		if r.DocID == "doc1" {
			found = true
		}
	}
	if !found {
		t.Error("expected doc1 with fuzzy match")
	}
}

// ============ Bool Query E2E Tests ============

func TestE2E_BoolQuery_And(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("hello AND go")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for AND, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc3" {
		t.Errorf("expected doc3, got %s", results[0].DocID)
	}
}

func TestE2E_BoolQuery_Or(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("programming OR test")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) < 2 {
		t.Errorf("expected at least 2 results for OR, got %d", len(results))
	}
}

func TestE2E_BoolQuery_Not(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("go AND NOT hello")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	for _, r := range results {
		if r.DocID == "doc3" {
			t.Error("doc3 should be excluded (has hello)")
		}
	}
}

func TestE2E_BoolQuery_NotOnlyError(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	_, err := s.RunQueryString("NOT hello")
	if err == nil {
		t.Error("expected error for NOT-only query")
	}
}

func TestE2E_BoolQuery_ImplicitAnd(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	// space between terms = implicit AND
	results, err := s.RunQueryString("hello go")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for implicit AND, got %d", len(results))
	}
}

// ============ Edge Cases E2E Tests ============

func TestE2E_EmptyQuery(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty query, got %d", len(results))
	}
}

func TestE2E_WhitespaceOnlyQuery(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("   ")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for whitespace-only query, got %d", len(results))
	}
}

// ============ Scoring E2E Tests ============

func TestE2E_Scoring_SortedDescending(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	results, err := s.RunQueryString("go")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Error("results not sorted by score descending")
		}
	}
}

func TestE2E_Scoring_TermFrequency(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	idx, _ := index.New(config)
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "test"})
	idx.Index("doc2", map[string]any{"body": "test test test"})

	snapshot, _ := idx.Snapshot()
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	results, _ := s.RunQueryString("test")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	// doc2 has higher TF, should rank first
	if results[0].DocID != "doc2" {
		t.Errorf("expected doc2 first (higher TF), got %s", results[0].DocID)
	}
}

// ============ Complex Query E2E Tests ============

func TestE2E_ComplexQuery_NestedBool(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	// Complex query with grouping
	results, err := s.RunQueryString("(hello OR go) AND world")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	// doc1: "Hello World" (hello + world) -> match
	// doc3: "Hello Go" / "Hello from Go world." (hello + go + world) -> match
	if len(results) < 1 {
		t.Errorf("expected at least 1 result, got %d", len(results))
	}
}

func TestE2E_ComplexQuery_MixedTypes(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	// Mix term query with field-specific
	results, err := s.RunQueryString("title:hello AND body:test")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	// doc1: title="Hello World", body="This is a test document." -> match
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc1" {
		t.Errorf("expected doc1, got %s", results[0].DocID)
	}
}

// ============ Searcher Lifecycle Tests ============

func TestSearcher_Close(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()

	s := New(snapshot)

	// Should not panic
	err := s.Close()
	if err != nil {
		t.Errorf("Close error: %v", err)
	}
}

func TestSearcher_MultipleQueries(t *testing.T) {
	snapshot := createTestSnapshot(t)
	defer snapshot.Close()
	s := New(snapshot)
	defer s.Close()

	// Run multiple queries on same searcher
	queries := []string{"hello", "go", "programming", "world"}
	for _, q := range queries {
		_, err := s.RunQueryString(q)
		if err != nil {
			t.Errorf("query %q error: %v", q, err)
		}
	}
}
