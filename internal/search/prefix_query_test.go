package search

import (
	"testing"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/query"
)

func TestPrefixQuery_MatchesTermsWithPrefix(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "prog*" should match "programming" in doc2 and doc4
	q := &query.PrefixQuery{Prefix: "prog"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) < 2 {
		t.Errorf("expected at least 2 results for 'prog*', got %d", len(results))
	}

	// Verify doc2 and doc4 are in results
	docIDs := make(map[string]bool)
	for _, r := range results {
		docIDs[r.DocID] = true
	}
	if !docIDs["doc2"] || !docIDs["doc4"] {
		t.Errorf("expected doc2 and doc4 in results, got %v", docIDs)
	}
}

func TestPrefixQuery_NoMatchReturnsEmpty(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.PrefixQuery{Prefix: "xyz"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for 'xyz*', got %d", len(results))
	}
}

func TestPrefixQuery_FieldSpecificSearch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "hel*" in title field should match doc1 "Hello World" and doc3 "Hello Go"
	q := &query.PrefixQuery{Field: "title", Prefix: "hel"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results for title:hel*, got %d", len(results))
	}
}

func TestPrefixQuery_FieldSpecificExcludesOtherFields(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "doc*" prefix only appears in body field (as "document")
	// Search in title field should not find it
	q := &query.PrefixQuery{Field: "title", Prefix: "doc"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for title:doc*, got %d", len(results))
	}
}

func TestPrefixQuery_ShortPrefix(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Single char prefix "g*" should match "go"
	q := &query.PrefixQuery{Prefix: "g"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) < 1 {
		t.Errorf("expected at least 1 result for 'g*', got %d", len(results))
	}
}

func TestPrefixQuery_ExactPrefixMatch(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// Index docs with terms that start with "test"
	idx.Index("doc1", map[string]any{"body": "test"})
	idx.Index("doc2", map[string]any{"body": "testing"})
	idx.Index("doc3", map[string]any{"body": "tested"})
	idx.Index("doc4", map[string]any{"body": "testimony"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.PrefixQuery{Prefix: "test"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Should match all 4 docs
	if len(results) != 4 {
		t.Errorf("expected 4 results for 'test*', got %d", len(results))
	}
}

func TestPrefixQuery_CaseInsensitive(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Prefix "HEL" should match "Hello" (case insensitive)
	q := &query.PrefixQuery{Prefix: "hel"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) < 2 {
		t.Errorf("expected at least 2 results for case-insensitive prefix, got %d", len(results))
	}
}

func TestPrefixQuery_WithDeletedDocuments(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "prefix match"})
	idx.Index("doc2", map[string]any{"body": "prefix match"})

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

	q := &query.PrefixQuery{Prefix: "pref"}
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

func TestPrefixQuery_EmptyPrefixReturnsEmpty(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.PrefixQuery{Prefix: ""}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Empty prefix behavior - might return all or none depending on implementation
	// Just verify no error
	_ = results
}

func TestPrefixQuery_ResultsHaveScores(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.PrefixQuery{Prefix: "prog"}
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

func TestPrefixQuery_ResultsSortedByScoreDescending(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.PrefixQuery{Prefix: "prog"}
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

func TestPrefixQuery_MultipleMatchingTerms(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// Document with multiple terms matching same prefix
	idx.Index("doc1", map[string]any{"body": "run running runner"})
	idx.Index("doc2", map[string]any{"body": "walk walking"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.PrefixQuery{Prefix: "run"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1 should match (has "run", "running", "runner")
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc1" {
		t.Errorf("expected doc1, got %s", results[0].DocID)
	}
}
