package search

import (
	"testing"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/query"
)

// ============ AND (Must) Tests ============

func TestBoolQuery_AndRequiresAllTerms(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Search for docs with both "hello" AND "go"
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "hello"},
			&query.TermQuery{Term: "go"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Only doc3 has both "hello" and "go"
	if len(results) != 1 {
		t.Errorf("expected 1 result for hello AND go, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc3" {
		t.Errorf("expected doc3, got %s", results[0].DocID)
	}
}

func TestBoolQuery_AndWithNoMatch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Search for docs with both "hello" AND "python"
	// No doc has both
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "hello"},
			&query.TermQuery{Term: "python"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results for hello AND python, got %d", len(results))
	}
}

func TestBoolQuery_AndWithThreeTerms(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "apple banana cherry"})
	idx.Index("doc2", map[string]any{"body": "apple banana"})
	idx.Index("doc3", map[string]any{"body": "apple cherry"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "apple"},
			&query.TermQuery{Term: "banana"},
			&query.TermQuery{Term: "cherry"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Only doc1 has all three
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc1" {
		t.Errorf("expected doc1, got %s", results[0].DocID)
	}
}

func TestBoolQuery_SingleMust(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Single Must is equivalent to term query
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "hello"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results for single AND, got %d", len(results))
	}
}

// ============ OR (Should) Tests ============

func TestBoolQuery_OrMatchesAnyTerm(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Search for docs with "programming" OR "test"
	q := &query.BoolQuery{
		Should: []query.Query{
			&query.TermQuery{Term: "programming"},
			&query.TermQuery{Term: "test"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1 has "test", doc2 and doc4 have "programming"
	if len(results) < 2 {
		t.Errorf("expected at least 2 results for programming OR test, got %d", len(results))
	}
}

func TestBoolQuery_OrWithNoMatch(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.BoolQuery{
		Should: []query.Query{
			&query.TermQuery{Term: "xyz"},
			&query.TermQuery{Term: "abc"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestBoolQuery_SingleShould(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Single Should is equivalent to term query
	q := &query.BoolQuery{
		Should: []query.Query{
			&query.TermQuery{Term: "hello"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results for single OR, got %d", len(results))
	}
}

// ============ NOT (MustNot) Tests ============

func TestBoolQuery_NotExcludesDocs(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Search for "go" AND NOT "hello"
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "go"},
		},
		MustNot: []query.Query{
			&query.TermQuery{Term: "hello"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc2 has "go" but not "hello", doc3 has both
	for _, r := range results {
		if r.DocID == "doc3" {
			t.Error("doc3 should be excluded (has hello)")
		}
	}
	// doc2 should be in results
	found := false
	for _, r := range results {
		if r.DocID == "doc2" {
			found = true
		}
	}
	if !found {
		t.Error("doc2 should be in results (has go, no hello)")
	}
}

func TestBoolQuery_NotOnlyReturnsError(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// NOT-only query should return error
	q := &query.BoolQuery{
		MustNot: []query.Query{
			&query.TermQuery{Term: "hello"},
		},
	}
	_, err := s.RunQuery(q)
	if err == nil {
		t.Error("expected error for NOT-only query")
	}
}

func TestBoolQuery_OrWithNot(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// (programming OR test) AND NOT hello
	q := &query.BoolQuery{
		Should: []query.Query{
			&query.TermQuery{Term: "programming"},
			&query.TermQuery{Term: "test"},
		},
		MustNot: []query.Query{
			&query.TermQuery{Term: "hello"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1 has "test" AND "hello" -> excluded
	// doc2 has "programming" but not "hello" -> included
	// doc4 has "programming" but not "hello" -> included
	for _, r := range results {
		if r.DocID == "doc1" || r.DocID == "doc3" {
			t.Errorf("%s should be excluded (has hello)", r.DocID)
		}
	}
}

func TestBoolQuery_MultipleNotClauses(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// programming AND NOT (hello OR go)
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "programming"},
		},
		MustNot: []query.Query{
			&query.TermQuery{Term: "hello"},
			&query.TermQuery{Term: "go"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc2 has "programming" and "go" -> excluded
	// doc4 has "programming" but not "hello" or "go" -> included
	found := false
	for _, r := range results {
		if r.DocID == "doc2" {
			t.Error("doc2 should be excluded (has go)")
		}
		if r.DocID == "doc4" {
			found = true
		}
	}
	if !found {
		t.Error("doc4 should be in results")
	}
}

// ============ Combined Tests ============

func TestBoolQuery_AndOrCombined(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "apple fruit red"})
	idx.Index("doc2", map[string]any{"body": "banana fruit yellow"})
	idx.Index("doc3", map[string]any{"body": "carrot vegetable orange"})
	idx.Index("doc4", map[string]any{"body": "apple vegetable"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// fruit AND (red OR yellow)
	// This should be interpreted as Must=[fruit], Should=[red, yellow]
	// But with Should, result should intersect with Should union
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "fruit"},
		},
		Should: []query.Query{
			&query.TermQuery{Term: "red"},
			&query.TermQuery{Term: "yellow"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1 has "fruit" and "red" -> match
	// doc2 has "fruit" and "yellow" -> match
	// doc3 has no "fruit" -> no match
	// doc4 has no "fruit" -> no match
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestBoolQuery_Empty(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// Empty bool query
	q := &query.BoolQuery{}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected nil/empty results for empty bool query, got %d", len(results))
	}
}

// ============ Nested Bool Tests ============

func TestBoolQuery_NestedBoolQueries(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "a b c"})
	idx.Index("doc2", map[string]any{"body": "a b d"})
	idx.Index("doc3", map[string]any{"body": "a c d"})
	idx.Index("doc4", map[string]any{"body": "b c d"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// a AND (b OR c)
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "a"},
			&query.BoolQuery{
				Should: []query.Query{
					&query.TermQuery{Term: "b"},
					&query.TermQuery{Term: "c"},
				},
			},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1: a, b, c -> match (has a and (b or c))
	// doc2: a, b, d -> match (has a and b)
	// doc3: a, c, d -> match (has a and c)
	// doc4: b, c, d -> no match (no a)
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	for _, r := range results {
		if r.DocID == "doc4" {
			t.Error("doc4 should not match (no 'a')")
		}
	}
}

// ============ Field-Specific Bool Tests ============

func TestBoolQuery_FieldSpecificTerms(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// title:hello AND body:test
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Field: "title", Term: "hello"},
			&query.TermQuery{Field: "body", Term: "test"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1: title="Hello World", body="This is a test document." -> match
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc1" {
		t.Errorf("expected doc1, got %s", results[0].DocID)
	}
}

// ============ With Other Query Types ============

func TestBoolQuery_WithPhraseQuery(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// "hello world" AND go
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.PhraseQuery{Phrase: "hello world"},
			&query.TermQuery{Term: "go"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1 has "Hello World" but no "go" -> no match
	// doc3 has "hello" and "go" but not phrase "hello world" -> no match
	// No doc has both
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestBoolQuery_WithPrefixQuery(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	// prog* AND test
	q := &query.BoolQuery{
		Must: []query.Query{
			&query.PrefixQuery{Prefix: "prog"},
			&query.TermQuery{Term: "test"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1 has "test" but no "prog*"
	// doc2 has "programming" but no "test"
	// doc4 has "programming" but no "test"
	// No doc has both
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

// ============ Deleted Documents Tests ============

func TestBoolQuery_WithDeletedDocuments(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "apple banana"})
	idx.Index("doc2", map[string]any{"body": "apple banana"})
	idx.Index("doc3", map[string]any{"body": "apple cherry"})

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

	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "apple"},
			&query.TermQuery{Term: "banana"},
		},
	}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// doc1 deleted, only doc2 should match
	if len(results) != 1 {
		t.Errorf("expected 1 result (doc1 deleted), got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != "doc2" {
		t.Errorf("expected doc2, got %s", results[0].DocID)
	}
}

// ============ Score Tests ============

func TestBoolQuery_ResultsHaveScores(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.BoolQuery{
		Must: []query.Query{
			&query.TermQuery{Term: "hello"},
			&query.TermQuery{Term: "go"},
		},
	}
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

func TestBoolQuery_ResultsSortedByScoreDescending(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.BoolQuery{
		Should: []query.Query{
			&query.TermQuery{Term: "go"},
			&query.TermQuery{Term: "hello"},
		},
	}
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
