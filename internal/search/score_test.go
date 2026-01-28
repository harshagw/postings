package search

import (
	"math"
	"testing"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/query"
)


func TestBM25_HigherTermFrequencyHigherScore(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	config.ScoringMode = index.ScoringBM25

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// doc2 has higher term frequency for "test"
	idx.Index("doc1", map[string]any{"body": "test"})
	idx.Index("doc2", map[string]any{"body": "test test test"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "test"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// doc2 should rank higher (higher TF)
	if results[0].DocID != "doc2" {
		t.Errorf("expected doc2 first (higher TF), got %s", results[0].DocID)
	}
	if results[0].Score <= results[1].Score {
		t.Errorf("doc2 should have higher score than doc1: %f <= %f",
			results[0].Score, results[1].Score)
	}
}

func TestBM25_LongerDocumentLowerScore(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	config.ScoringMode = index.ScoringBM25

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// Both docs have same TF=1 for "important", but doc2 is much longer
	idx.Index("doc1", map[string]any{"body": "important"})
	idx.Index("doc2", map[string]any{"body": "important filler words here to make this document much longer"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "important"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// doc1 should rank higher (shorter document, same TF)
	if results[0].DocID != "doc1" {
		t.Errorf("expected doc1 first (shorter doc), got %s", results[0].DocID)
	}
	if results[0].Score <= results[1].Score {
		t.Errorf("doc1 should have higher score than doc2: %f <= %f",
			results[0].Score, results[1].Score)
	}
}

func TestBM25_RareTermHigherScore(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	config.ScoringMode = index.ScoringBM25

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// "rare" appears in 1 doc, "common" appears in all 3
	idx.Index("doc1", map[string]any{"body": "common rare"})
	idx.Index("doc2", map[string]any{"body": "common"})
	idx.Index("doc3", map[string]any{"body": "common"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	// Search for "rare" vs "common" in doc1
	qRare := &query.TermQuery{Term: "rare"}
	resultsRare, err := s.RunQuery(qRare)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	qCommon := &query.TermQuery{Term: "common"}
	resultsCommon, err := s.RunQuery(qCommon)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Rare term has higher IDF, so doc1's score for "rare" should be higher
	// than doc1's score for "common" (assuming same TF and doc length)
	var doc1RareScore, doc1CommonScore float64
	for _, r := range resultsRare {
		if r.DocID == "doc1" {
			doc1RareScore = r.Score
		}
	}
	for _, r := range resultsCommon {
		if r.DocID == "doc1" {
			doc1CommonScore = r.Score
		}
	}

	if doc1RareScore <= doc1CommonScore {
		t.Errorf("rare term should score higher than common term: rare=%f, common=%f",
			doc1RareScore, doc1CommonScore)
	}
}

func TestBM25_TermFrequencySaturation(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	config.ScoringMode = index.ScoringBM25

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// BM25 has term frequency saturation - going from TF=1 to TF=10
	// should not be 10x the score increase
	idx.Index("doc1", map[string]any{"body": "word"})
	idx.Index("doc2", map[string]any{"body": "word word"})
	idx.Index("doc3", map[string]any{"body": "word word word word word word word word word word"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "word"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify saturation: score increase from TF=1 to TF=10 should be less than 10x
	var score1, score10 float64
	for _, r := range results {
		switch r.DocID {
		case "doc1":
			score1 = r.Score
		case "doc3":
			score10 = r.Score
		}
	}

	// Score ratio should be much less than 10 due to saturation
	if score1 > 0 {
		ratio := score10 / score1
		// With BM25 saturation, this ratio should be significantly less than 10
		if ratio >= 10 {
			t.Errorf("BM25 saturation not working: TF=10 score is %fx TF=1 score (should be much less than 10x)",
				ratio)
		}
	}
}

func TestBM25_ScoresArePositive(t *testing.T) {
	idx, cleanup := createTestIndex(t)
	defer cleanup()

	s, sCleanup := createSearcher(t, idx)
	defer sCleanup()

	q := &query.TermQuery{Term: "hello"}
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

// ============ TF-IDF Scoring Tests ============

// TF-IDF formula (as implemented):
// TF = 1 + log(raw_tf)  (sublinear TF scaling, 0 if raw_tf=0)
// IDF = log((N+1)/(df+1)) + 1  (smoothed IDF)
// score = TF * IDF

func TestTFIDF_HigherTermFrequencyHigherScore(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	config.ScoringMode = index.ScoringTFIDF

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "test"})
	idx.Index("doc2", map[string]any{"body": "test test test"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "test"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// doc2 should rank higher (higher TF)
	if results[0].DocID != "doc2" {
		t.Errorf("expected doc2 first (higher TF), got %s", results[0].DocID)
	}
}

func TestTFIDF_RareTermHigherScore(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	config.ScoringMode = index.ScoringTFIDF

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// "rare" appears in 1 doc, "common" appears in all 3
	idx.Index("doc1", map[string]any{"body": "common rare"})
	idx.Index("doc2", map[string]any{"body": "common"})
	idx.Index("doc3", map[string]any{"body": "common"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	qRare := &query.TermQuery{Term: "rare"}
	resultsRare, err := s.RunQuery(qRare)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	qCommon := &query.TermQuery{Term: "common"}
	resultsCommon, err := s.RunQuery(qCommon)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	var doc1RareScore, doc1CommonScore float64
	for _, r := range resultsRare {
		if r.DocID == "doc1" {
			doc1RareScore = r.Score
		}
	}
	for _, r := range resultsCommon {
		if r.DocID == "doc1" {
			doc1CommonScore = r.Score
		}
	}

	if doc1RareScore <= doc1CommonScore {
		t.Errorf("rare term should score higher than common term: rare=%f, common=%f",
			doc1RareScore, doc1CommonScore)
	}
}

func TestTFIDF_SublinearTermFrequency(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	config.ScoringMode = index.ScoringTFIDF

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// TF uses logarithmic scaling: TF = 1 + log(raw_tf)
	idx.Index("doc1", map[string]any{"body": "word"})
	idx.Index("doc2", map[string]any{"body": "word word word word word word word word word word"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "word"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	var score1, score10 float64
	for _, r := range results {
		switch r.DocID {
		case "doc1":
			score1 = r.Score
		case "doc2":
			score10 = r.Score
		}
	}

	// With sublinear TF: TF(1) = 1+log(1) = 1, TF(10) = 1+log(10) ≈ 3.3
	// So score ratio should be around 3.3, not 10
	if score1 > 0 {
		ratio := score10 / score1
		// Expected ratio ≈ (1+ln(10))/(1+ln(1)) = 3.3/1 = 3.3
		expected := (1 + math.Log(10)) / (1 + math.Log(1))
		tolerance := 0.5 // Allow some tolerance
		if math.Abs(ratio-expected) > tolerance {
			t.Errorf("TF-IDF sublinear scaling unexpected: ratio=%f, expected≈%f", ratio, expected)
		}
	}
}

func TestTFIDF_ScoresArePositive(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000
	config.ScoringMode = index.ScoringTFIDF

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "test document"})
	idx.Index("doc2", map[string]any{"body": "another test"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "test"}
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

// ============ Sorting Tests ============

func TestScoring_ResultsSortedDescending(t *testing.T) {
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
			t.Errorf("results not sorted by score descending at index %d: %f > %f",
				i, results[i].Score, results[i-1].Score)
		}
	}
}

func TestScoring_ConsistentScoring(t *testing.T) {
	// Use documents with clearly different scores to avoid ordering issues
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// Create documents with clearly different term frequencies
	idx.Index("doc1", map[string]any{"body": "unique"})
	idx.Index("doc2", map[string]any{"body": "unique unique unique"})
	idx.Index("doc3", map[string]any{"body": "unique unique unique unique unique"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "unique"}

	// Run same query twice
	results1, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	results2, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results1) != len(results2) {
		t.Fatalf("inconsistent result count: %d vs %d", len(results1), len(results2))
	}

	// Verify same documents appear in both result sets (order may vary for equal scores)
	docs1 := make(map[string]float64)
	docs2 := make(map[string]float64)
	for _, r := range results1 {
		docs1[r.DocID] = r.Score
	}
	for _, r := range results2 {
		docs2[r.DocID] = r.Score
	}

	for docID, score1 := range docs1 {
		score2, ok := docs2[docID]
		if !ok {
			t.Errorf("doc %s missing from second run", docID)
			continue
		}
		if math.Abs(score1-score2) > 0.0001 {
			t.Errorf("inconsistent score for %s: %f vs %f", docID, score1, score2)
		}
	}
}

// ============ Edge Cases ============

func TestScoring_SingleDocument(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	idx.Index("doc1", map[string]any{"body": "single document test"})

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "test"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if len(results) > 0 && results[0].Score <= 0 {
		t.Errorf("expected positive score, got %f", results[0].Score)
	}
}

func TestScoring_ManyDocuments(t *testing.T) {
	dir := t.TempDir()
	config := index.DefaultConfig(dir)
	config.FlushThreshold = 10000

	idx, err := index.New(config)
	if err != nil {
		t.Fatalf("New index error: %v", err)
	}
	defer idx.Close()

	// Create 100 documents with varying term frequencies
	for i := 0; i < 100; i++ {
		body := "document"
		for j := 0; j <= i%10; j++ {
			body += " keyword"
		}
		idx.Index(string(rune('a'+i/26))+string(rune('a'+i%26)), map[string]any{"body": body})
	}

	snapshot, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	defer snapshot.Close()

	s := New(snapshot)
	defer s.Close()

	q := &query.TermQuery{Term: "keyword"}
	results, err := s.RunQuery(q)
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}

	// Verify sorted
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted at index %d", i)
			break
		}
	}

	// All scores should be positive
	for _, r := range results {
		if r.Score <= 0 {
			t.Errorf("expected positive score for %s, got %f", r.DocID, r.Score)
		}
	}
}

// ============ BM25 Parameter Verification ============

func TestBM25_UsesCorrectConstants(t *testing.T) {
	// Verify the constants are set correctly
	if BM25_k1 != 1.2 {
		t.Errorf("BM25_k1 should be 1.2, got %f", BM25_k1)
	}
	if BM25_b != 0.75 {
		t.Errorf("BM25_b should be 0.75, got %f", BM25_b)
	}
}
