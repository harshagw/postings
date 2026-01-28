package search

import (
	"testing"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/query"
)

// ============ RegexQuery Tests ============

func TestRegexQuery_SimplePatternMatches(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Pattern "go" should match docs containing "go"
	q := &query.RegexQuery{Pattern: "go"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) < 2 {
		t.Errorf("expected at least 2 results for /go/, got %d", len(results))
	}
}

func TestRegexQuery_OrPatternMatches(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Pattern "go|hello" should match docs with either term
	q := &query.RegexQuery{Pattern: "go|hello"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1, doc2, doc3 have "go" or "hello"
	if len(results) < 3 {
		t.Errorf("expected at least 3 results for /go|hello/, got %d", len(results))
	}
}

func TestRegexQuery_InvalidPatternReturnsError(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.RegexQuery{Pattern: "[invalid"}
	_, err := s.RunQuery(q)
	if err == nil {
		t.Error("expected error for invalid regex pattern")
	}
}

func TestRegexQuery_NoMatchReturnsEmpty(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.RegexQuery{Pattern: "xyz123"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for /xyz123/, got %d", len(results))
	}
}

func TestRegexQuery_FieldSpecificSearch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Search for pattern in title field only
	q := &query.RegexQuery{Field: "title", Pattern: "go"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc2 "Go Programming" and doc3 "Hello Go" have "go" in title
	if len(results) != 2 {
		t.Errorf("expected 2 results for title:/go/, got %d", len(results))
	}
}

func TestRegexQuery_WildcardPattern(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Pattern "prog.*" should match "programming"
	q := &query.RegexQuery{Pattern: "prog.*"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) < 2 {
		t.Errorf("expected at least 2 results for /prog.*/, got %d", len(results))
	}
}

func TestRegexQuery_AnchorPattern(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "testing"})
	idx.Index("doc2", map[string]any{"body": "tested"})
	idx.Index("doc3", map[string]any{"body": "contest"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// Pattern "^test" should only match terms starting with "test"
	q := &query.RegexQuery{Pattern: "^test"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Should match "testing" and "tested", not "contest"
	if len(results) != 2 {
		t.Errorf("expected 2 results for /^test/, got %d", len(results))
	}

	docIDs := make(map[string]bool)
	for _, r := range results {
		docIDs[r.DocID] = true
	}
	if docIDs["doc3"] {
		t.Error("doc3 with 'contest' should not match /^test/")
	}
}

func TestRegexQuery_CharacterClass(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "cat"})
	idx.Index("doc2", map[string]any{"body": "bat"})
	idx.Index("doc3", map[string]any{"body": "rat"})
	idx.Index("doc4", map[string]any{"body": "hat"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// Pattern "[cbr]at" should match cat, bat, rat but not hat
	q := &query.RegexQuery{Pattern: "[cbr]at"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 results for /[cbr]at/, got %d", len(results))
	}
}

func TestRegexQuery_WithDeletedDocuments(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "pattern match"})
	idx.Index("doc2", map[string]any{"body": "pattern match"})

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

	q := &query.RegexQuery{Pattern: "pattern"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result (doc1 deleted), got %d", len(results))
	}
}

// ============ FuzzyQuery Tests ============

func TestFuzzyQuery_MatchesWithinEditDistance(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "hallo" is 1 edit from "hello"
	q := &query.FuzzyQuery{Term: "hallo", Fuzziness: 1}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) < 1 {
		t.Errorf("expected at least 1 result for hallo~1, got %d", len(results))
	}
}

func TestFuzzyQuery_NoMatchBeyondEditDistance(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "xxxxx" is more than 2 edits from any term
	q := &query.FuzzyQuery{Term: "xxxxx", Fuzziness: 2}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for distant fuzzy term, got %d", len(results))
	}
}

func TestFuzzyQuery_FieldSpecificSearch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "worlf" is 1 edit from "world" in doc1's title
	q := &query.FuzzyQuery{Field: "title", Term: "worlf", Fuzziness: 1}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	found := false
	for _, r := range results {
		if r.DocID == "doc1" {
			found = true
		}
	}
	if !found {
		t.Error("expected doc1 with fuzzy match for title:worlf~1")
	}
}

func TestFuzzyQuery_ZeroFuzzinessIsExactMatch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Fuzziness 0 should be exact match
	q := &query.FuzzyQuery{Term: "hello", Fuzziness: 0}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results for exact match 'hello', got %d", len(results))
	}
}

func TestFuzzyQuery_MultipleMatches(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "test"})
	idx.Index("doc2", map[string]any{"body": "text"}) // 1 edit from "test"
	idx.Index("doc3", map[string]any{"body": "best"}) // 1 edit from "test"
	idx.Index("doc4", map[string]any{"body": "rest"}) // 1 edit from "test"

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.FuzzyQuery{Term: "test", Fuzziness: 1}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Should match "test", "text", "best", "rest"
	if len(results) != 4 {
		t.Errorf("expected 4 results for test~1, got %d", len(results))
	}
}

func TestFuzzyQuery_SubstitutionEdit(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "color"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// "colour" vs "color" - 1 insertion
	q := &query.FuzzyQuery{Term: "colour", Fuzziness: 1}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for colour~1, got %d", len(results))
	}
}

func TestFuzzyQuery_InsertionEdit(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "hello"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// "helo" is 1 deletion from "hello"
	q := &query.FuzzyQuery{Term: "helo", Fuzziness: 1}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for helo~1, got %d", len(results))
	}
}

func TestFuzzyQuery_DeletionEdit(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "hello"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// "helloo" is 1 insertion from "hello"
	q := &query.FuzzyQuery{Term: "helloo", Fuzziness: 1}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for helloo~1, got %d", len(results))
	}
}

func TestFuzzyQuery_WithDeletedDocuments(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "fuzzy match"})
	idx.Index("doc2", map[string]any{"body": "fuzzy match"})

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

	q := &query.FuzzyQuery{Term: "fuzzi", Fuzziness: 1}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result (doc1 deleted), got %d", len(results))
	}
}

func TestFuzzyQuery_HigherFuzzinessMoreMatches(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "cat"})
	idx.Index("doc2", map[string]any{"body": "bat"})  // 1 edit from "cat"
	idx.Index("doc3", map[string]any{"body": "can"})  // 1 edit from "cat"
	idx.Index("doc4", map[string]any{"body": "ban"})  // 2 edits from "cat"
	idx.Index("doc5", map[string]any{"body": "ban"})  // 2 edits from "cat"
	idx.Index("doc6", map[string]any{"body": "xyz"})  // >2 edits from "cat"

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// Fuzziness 1 should match cat, bat, can
	q1 := &query.FuzzyQuery{Term: "cat", Fuzziness: 1}
	results1, err := s.RunQuery(q1)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Fuzziness 2 should match more
	q2 := &query.FuzzyQuery{Term: "cat", Fuzziness: 2}
	results2, err := s.RunQuery(q2)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results2) < len(results1) {
		t.Errorf("higher fuzziness should return at least as many results: fuzz1=%d, fuzz2=%d",
			len(results1), len(results2))
	}
}
