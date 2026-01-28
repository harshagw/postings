package search

import (
	"testing"
)

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		a, b     string
		expected int
	}{
		{"", "", 0},              // both empty
		{"abc", "", 3},           // one empty
		{"abc", "abc", 0},        // identical
		{"ab", "ac", 1},          // substitution
		{"abc", "ab", 1},         // deletion
		{"ab", "abc", 1},         // insertion
		{"kitten", "sitting", 3}, // real-world example
	}

	for _, tt := range tests {
		got := levenshteinDistance(tt.a, tt.b)
		if got != tt.expected {
			t.Errorf("levenshteinDistance(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.expected)
		}
	}
}

func TestBinarySearchUint64(t *testing.T) {
	slice := []uint64{1, 3, 5, 7, 9}

	if binarySearchUint64([]uint64{}, 5) {
		t.Error("empty slice should return false")
	}
	if !binarySearchUint64(slice, 5) {
		t.Error("existing element should return true")
	}
	if binarySearchUint64(slice, 4) {
		t.Error("non-existing element should return false")
	}
}

func TestSortByScore(t *testing.T) {
	results := []Result{
		{DocID: "doc1", Score: 1.0},
		{DocID: "doc2", Score: 3.0},
		{DocID: "doc3", Score: 2.0},
	}

	sortByScore(results)

	// Verify sorted in descending order
	if results[0].DocID != "doc2" || results[1].DocID != "doc3" || results[2].DocID != "doc1" {
		t.Errorf("got order %s,%s,%s, want doc2,doc3,doc1", results[0].DocID, results[1].DocID, results[2].DocID)
	}
}
