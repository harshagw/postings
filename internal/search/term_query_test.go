package search

import (
	"testing"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/query"
)

// createTestIndex creates a test index with sample documents for testing.
func createTestIndex(t *testing.T) (*index.Index, func()) {
	t.Helper()
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}

	docs := []struct {
		id  string
		doc map[string]any
	}{
		{"doc1", map[string]any{"title": "Hello World", "body": "This is a test document."}},
		{"doc2", map[string]any{"title": "Go Programming", "body": "Learning Go programming language."}},
		{"doc3", map[string]any{"title": "Hello Go", "body": "Hello from Go world."}},
		{"doc4", map[string]any{"title": "Python Basics", "body": "Introduction to Python programming."}},
		{"doc5", map[string]any{"title": "Database Design", "body": "SQL and NoSQL databases explained."}},
	}

	for _, d := range docs {
		if err := idx.Index(d.id, d.doc); err != nil {
			t.Fatalf("Index error: %v", err)
		}
	}

	cleanup := func() { idx.Close() }
	return idx, cleanup
}

// createSearcher creates a searcher from the given index.
func createSearcher(t *testing.T, idx *index.Index) (*Searcher, func()) {
	t.Helper()
	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	s := New(snapshot)
	cleanup := func() {
		s.Close()
		snapshot.Close()
	}
	return s, cleanup
}

func TestTermQuery_FindsDocsByTerm(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Search for "hello" - should find doc1 and doc3
	q := &query.TermQuery{Term: "hello"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results for 'hello', got %d", len(results))
	}

	docIDs := make(map[string]bool)
	for _, r := range results {
		docIDs[r.DocID] = true
	}
	if !docIDs["doc1"] || !docIDs["doc3"] {
		t.Errorf("expected doc1 and doc3, got %v", docIDs)
	}
}

func TestTermQuery_NoMatchReturnsEmpty(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.TermQuery{Term: "nonexistent"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for nonexistent term, got %d", len(results))
	}
}

func TestTermQuery_FieldSpecificSearch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Search for "go" in title field only
	q := &query.TermQuery{Field: "title", Term: "go"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc2 "Go Programming" and doc3 "Hello Go" have "go" in title
	if len(results) != 2 {
		t.Errorf("expected 2 results for title:go, got %d", len(results))
	}
}

func TestTermQuery_FieldSpecificExcludesOtherFields(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "programming" appears in body of doc2 and doc4, and title of doc2
	// Search only in title field
	q := &query.TermQuery{Field: "title", Term: "programming"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Only doc2 has "programming" in title
	if len(results) != 1 {
		t.Errorf("expected 1 result for title:programming, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc2" {
		t.Errorf("expected doc2, got %s", results[0].DocID)
	}
}

func TestTermQuery_SearchAllFieldsWhenNoFieldSpecified(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "programming" appears in multiple docs across fields
	q := &query.TermQuery{Term: "programming"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc2 has it in title and body, doc4 has it in body
	if len(results) < 2 {
		t.Errorf("expected at least 2 results for 'programming', got %d", len(results))
	}
}

func TestTermQuery_CaseInsensitive(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "Hello" in title should match when searching lowercase "hello"
	q := &query.TermQuery{Term: "hello"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results (case insensitive), got %d", len(results))
	}
}

func TestTermQuery_ResultsHaveScores(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.TermQuery{Term: "go"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	for _, r := range results {
		if r.Score <= 0 {
			t.Errorf("expected positive score for %s, got %f", r.DocID, r.Score)
		}
	}
}

func TestTermQuery_ResultsSortedByScoreDescending(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.TermQuery{Term: "go"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted by score descending: %f > %f at index %d",
				results[i].Score, results[i-1].Score, i)
		}
	}
}

func TestTermQuery_WithDeletedDocuments(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// Index documents
	idx.Index("doc1", map[string]any{"body": "unique term here"})
	idx.Index("doc2", map[string]any{"body": "unique term there"})

	// Delete doc1
	if err := idx.Delete("doc1"); err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "unique"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Should only find doc2, doc1 is deleted
	if len(results) != 1 {
		t.Errorf("expected 1 result (doc1 deleted), got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc2" {
		t.Errorf("expected doc2, got %s", results[0].DocID)
	}
}

func TestTermQuery_EmptyTermReturnsEmpty(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.TermQuery{Term: ""}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for empty term, got %d", len(results))
	}
}
