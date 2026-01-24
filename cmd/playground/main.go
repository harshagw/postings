// Playground for testing the postings search engine.
//
// Run with: go run ./cmd/playground
package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/search"
)

func runQueries(searcher *search.Searcher, queries []string) {
	for _, q := range queries {
		fmt.Printf("Query: %s\n", q)
		fmt.Println(strings.Repeat("-", 60))

		results, err := searcher.Query(q)
		if err != nil {
			fmt.Printf("  Error: %v\n\n", err)
			continue
		}

		if len(results) == 0 {
			fmt.Println("  No results found")
		} else {
			for i, r := range results {
				fmt.Printf("  %d. %s (score: %.4f)\n", i+1, r.DocID, r.Score)
			}
		}
		fmt.Println()
	}
}

func main() {
	// Create a temporary directory for the index
	dir, err := os.MkdirTemp("", "postings-playground-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fmt.Println("=== Postings Search Engine Playground ===")
	fmt.Printf("Index directory: %s\n\n", dir)

	// Create index with default config
	cfg := index.DefaultConfig(dir)
	idx, err := index.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer idx.Close()

	// Index some sample documents
	docs := []struct {
		id  string
		doc map[string]any
	}{
		{"doc1", map[string]any{"title": "Introduction to Go", "body": "Go is a statically typed programming language designed at Google."}},
		{"doc2", map[string]any{"title": "Go Concurrency", "body": "Goroutines and channels make concurrent programming easy in Go."}},
		{"doc3", map[string]any{"title": "Python Basics", "body": "Python is a dynamically typed language popular for scripting and data science."}},
		{"doc4", map[string]any{"title": "Rust Programming", "body": "Rust is a systems programming language focused on safety and performance."}},
		{"doc5", map[string]any{"title": "JavaScript Guide", "body": "JavaScript is the language of the web, running in browsers everywhere."}},
		{"doc6", map[string]any{"title": "Go Web Development", "body": "Building web applications with Go is fast and efficient."}},
		{"doc7", map[string]any{"title": "Database Design", "body": "Good database design is essential for scalable applications."}},
		{"doc8", map[string]any{"title": "Search Engines", "body": "Search engines use inverted indexes to find documents quickly."}},
	}

	fmt.Println("Indexing documents...")
	for _, d := range docs {
		if err := idx.Index(d.id, d.doc); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  Indexed: %s - %s\n", d.id, d.doc["title"])
	}
	fmt.Println()

	// Get a snapshot for searching
	snapshot, err := idx.Snapshot()
	if err != nil {
		log.Fatal(err)
	}
	defer snapshot.Close()

	searcher := search.New(snapshot)
	defer searcher.Close()

	// Run some example queries
	fmt.Println("--- Basic Queries ---")
	basicQueries := []string{
		// Simple term search
		"go",
		// Field-specific search
		"title:go",
		// Phrase search
		`"programming language"`,
		// Boolean AND (implicit)
		"go programming",
		// Boolean AND (explicit)
		"go AND web",
		// Boolean OR
		"python OR rust",
		// NOT query (must have positive clause first)
		"programming AND -go",
		// Prefix search
		"program*",
	}

	runQueries(searcher, basicQueries)

	fmt.Println("--- Advanced Queries ---")
	advancedQueries := []string{
		// Grouped query with OR inside AND
		"(python OR rust) AND programming",
		// Field-specific with boolean
		"title:go AND body:web",
		// Multiple field searches
		"title:go OR title:python OR title:rust",
		// Nested grouping
		"(go OR python) AND (web OR programming)",
		// Complex: field + phrase + NOT
		`title:go AND "programming language" AND -concurrency`,
		// Multiple NOT clauses
		"programming AND -go AND -python",
		// OR with NOT
		"(go OR python) AND -web",
		// Prefix with field
		"title:java* OR title:go",
		// Deep nesting
		"((go AND web) OR (python AND data)) AND -rust",
		// All features combined
		`title:go AND (body:web OR body:"programming language") AND -concurrency`,
		// NOT with field:term
		"programming AND -title:to",
	}

	runQueries(searcher, advancedQueries)

	// Demonstrate flush and segment info
	fmt.Println("=== Index Statistics ===")
	fmt.Println()

	if err := idx.Flush(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Number of segments: %d\n", idx.NumSegments())
	for _, seg := range idx.Segments() {
		stats, err := idx.SegmentStats(seg.ID)
		if err != nil {
			continue
		}
		fmt.Printf("  Segment %s: %d docs, fields: %v\n", seg.ID, stats.NumDocs, stats.Fields)
	}
}
