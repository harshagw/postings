package search

import (
	"testing"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/query"
)

func TestPhraseQuery_MatchesAdjacentTerms(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "Hello World" appears in doc1's title
	q := &query.PhraseQuery{Phrase: "Hello World"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for 'Hello World', got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc1" {
		t.Errorf("expected doc1, got %s", results[0].DocID)
	}
}

func TestPhraseQuery_RejectsNonAdjacentTerms(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "hello" and "programming" both exist but are never adjacent
	q := &query.PhraseQuery{Phrase: "hello programming"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for non-adjacent phrase, got %d", len(results))
	}
}

func TestPhraseQuery_MatchesMultiWordPhrase(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "Go Programming" appears in doc2's title
	q := &query.PhraseQuery{Phrase: "Go Programming"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for 'Go Programming', got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc2" {
		t.Errorf("expected doc2, got %s", results[0].DocID)
	}
}

func TestPhraseQuery_FieldSpecificSearch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "Hello World" is in doc1's title, search only in title
	q := &query.PhraseQuery{Field: "title", Phrase: "Hello World"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for title:'Hello World', got %d", len(results))
	}
}

func TestPhraseQuery_FieldSpecificExcludesOtherFields(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "test document" appears in doc1's body, not title
	q := &query.PhraseQuery{Field: "title", Phrase: "test document"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for title:'test document', got %d", len(results))
	}

	// But it should be found in body
	q2 := &query.PhraseQuery{Field: "body", Phrase: "test document"}
	results2, err := s.RunQuery(q2)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results2) != 1 {
		t.Errorf("expected 1 result for body:'test document', got %d", len(results2))
	}
}

func TestPhraseQuery_SingleTermFallsBackToTermSearch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Single term phrase should work like term search
	q := &query.PhraseQuery{Phrase: "hello"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Should find doc1 and doc3
	if len(results) != 2 {
		t.Errorf("expected 2 results for single-term phrase 'hello', got %d", len(results))
	}
}

func TestPhraseQuery_EmptyPhraseReturnsEmpty(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.PhraseQuery{Phrase: ""}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected nil/empty results for empty phrase, got %d", len(results))
	}
}

func TestPhraseQuery_CaseInsensitive(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "hello world" lowercase should match "Hello World"
	q := &query.PhraseQuery{Phrase: "hello world"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result (case insensitive), got %d", len(results))
	}
}

func TestPhraseQuery_ThreeWordPhrase(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// Index documents with 3-word phrases
	idx.Index("doc1", map[string]any{"body": "the quick brown fox jumps"})
	idx.Index("doc2", map[string]any{"body": "quick brown dog"})
	idx.Index("doc3", map[string]any{"body": "the lazy brown dog"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// "quick brown fox" should only match doc1
	q := &query.PhraseQuery{Phrase: "quick brown fox"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for 'quick brown fox', got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc1" {
		t.Errorf("expected doc1, got %s", results[0].DocID)
	}
}

func TestPhraseQuery_WithDeletedDocuments(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "exact phrase match"})
	idx.Index("doc2", map[string]any{"body": "exact phrase match"})

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

	q := &query.PhraseQuery{Phrase: "exact phrase match"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Should only find doc2
	if len(results) != 1 {
		t.Errorf("expected 1 result (doc1 deleted), got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc2" {
		t.Errorf("expected doc2, got %s", results[0].DocID)
	}
}

func TestPhraseQuery_RejectsWrongOrder(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "World Hello" is not in any document (reversed order)
	q := &query.PhraseQuery{Phrase: "World Hello"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for wrong order phrase, got %d", len(results))
	}
}

func TestPhraseQuery_RejectsPartialOverlap(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// "hello" and "world" exist but with a word in between
	idx.Index("doc1", map[string]any{"body": "hello beautiful world"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// "hello world" should NOT match "hello beautiful world"
	q := &query.PhraseQuery{Phrase: "hello world"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for partial overlap, got %d", len(results))
	}
}
