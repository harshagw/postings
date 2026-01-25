package main

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/search"
)

// Document represents a test document with known content.
type Document struct {
	ID     string
	Fields map[string]any
}

// TestCase represents a query with expected results.
type TestCase struct {
	Query    string
	Expected []string // Expected document IDs (order doesn't matter)
}

func main() {
	fmt.Println("Search Engine Verification")
	fmt.Println("==========================")
	fmt.Println()

	// Create a temporary directory for the index
	dir, err := os.MkdirTemp("", "verify-*")
	if err != nil {
		fmt.Printf("Error creating temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	// Create index with small flush threshold to test multi-segment search
	cfg := index.DefaultConfig(dir)
	cfg.FlushThreshold = 10 // Create segments every 10 docs
	idx, err := index.New(cfg)
	if err != nil {
		fmt.Printf("Error creating index: %v\n", err)
		os.Exit(1)
	}
	defer idx.Close()

	// Index our test documents
	docs := getTestDocuments()
	for _, doc := range docs {
		if err := idx.Index(doc.ID, doc.Fields); err != nil {
			fmt.Printf("Error indexing doc %s: %v\n", doc.ID, err)
			os.Exit(1)
		}
	}

	// Flush to create segments
	if err := idx.Flush(); err != nil {
		fmt.Printf("Error flushing: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Indexed %d documents across %d segments\n", len(docs), idx.NumSegments())
	fmt.Println()

	// Create searcher
	snapshot, err := idx.Snapshot()
	if err != nil {
		fmt.Printf("Error getting snapshot: %v\n", err)
		os.Exit(1)
	}
	defer snapshot.Close()

	searcher := search.New(snapshot)
	defer searcher.Close()

	// Run all test cases
	passed := 0
	failed := 0

	// Group test cases by category
	categories := getTestCategories()

	for _, category := range categories {
		fmt.Printf("\n%s\n", category.Name)
		fmt.Println(strings.Repeat("-", len(category.Name)))

		for _, tc := range category.Cases {
			ok := runTestCase(searcher, tc)
			if ok {
				passed++
			} else {
				failed++
			}
		}
	}

	// Summary
	fmt.Println()
	fmt.Println("========================================")
	fmt.Printf("Results: %d passed, %d failed, %d total\n", passed, failed, passed+failed)

	if failed > 0 {
		os.Exit(1)
	}
	fmt.Println("\nAll tests passed!")
}

func runTestCase(s *search.Searcher, tc TestCase) bool {
	results, err := s.RunQueryString(tc.Query)
	if err != nil {
		fmt.Printf("  ✗ %s\n", tc.Query)
		fmt.Printf("    Error: %v\n", err)
		return false
	}

	// Extract doc IDs from results
	gotIDs := make([]string, len(results))
	for i, r := range results {
		gotIDs[i] = r.DocID
	}

	// Sort both for comparison
	sort.Strings(gotIDs)
	expectedSorted := make([]string, len(tc.Expected))
	copy(expectedSorted, tc.Expected)
	sort.Strings(expectedSorted)

	if !slices.Equal(gotIDs, expectedSorted) {
		fmt.Printf("  ✗ %s\n", tc.Query)
		fmt.Printf("    Expected: %v\n", expectedSorted)
		fmt.Printf("    Got:      %v\n", gotIDs)
		return false
	}

	fmt.Printf("  ✓ %s\n", tc.Query)
	return true
}

// getTestDocuments returns our deterministic test data set.
func getTestDocuments() []Document {
	return []Document{
		// Doc 1: About programming languages
		{
			ID: "doc1",
			Fields: map[string]any{
				"title": "Introduction to Go Programming",
				"body":  "Go is a statically typed compiled language designed at Google. It has garbage collection and structural typing.",
				"tags":  "programming language go google",
			},
		},
		// Doc 2: About programming languages (Python)
		{
			ID: "doc2",
			Fields: map[string]any{
				"title": "Python Programming Guide",
				"body":  "Python is a high-level interpreted language known for its simplicity. It is widely used in data science and machine learning.",
				"tags":  "programming language python data science",
			},
		},
		// Doc 3: About programming languages (Rust)
		{
			ID: "doc3",
			Fields: map[string]any{
				"title": "Rust Programming Language",
				"body":  "Rust is a systems programming language focused on safety and performance. No garbage collection needed.",
				"tags":  "programming language rust systems safety",
			},
		},
		// Doc 4: About databases (PostgreSQL) - only "database" in body
		{
			ID: "doc4",
			Fields: map[string]any{
				"title": "PostgreSQL Guide",
				"body":  "PostgreSQL is a powerful open source relational database. It supports advanced features like JSON and full text search.",
				"tags":  "database sql postgresql open source",
			},
		},
		// Doc 5: About databases (Redis) - "database" in title and body, "data" in body
		{
			ID: "doc5",
			Fields: map[string]any{
				"title": "Redis In-Memory Database",
				"body":  "Redis is an in-memory data structure store used as a database cache and message broker.",
				"tags":  "database redis cache memory nosql",
			},
		},
		// Doc 6: About web development (React) - "development" in title
		{
			ID: "doc6",
			Fields: map[string]any{
				"title": "Web Development with React",
				"body":  "React is a JavaScript library for building user interfaces. It uses a virtual DOM for performance.",
				"tags":  "web frontend javascript react dom",
			},
		},
		// Doc 7: About web development (backend) - "development" in title+body, "database" in body
		{
			ID: "doc7",
			Fields: map[string]any{
				"title": "Backend Web Development",
				"body":  "Backend development involves server-side logic and database interactions. Common languages include Python Go and Java.",
				"tags":  "web backend server api",
			},
		},
		// Doc 8: About cloud computing
		{
			ID: "doc8",
			Fields: map[string]any{
				"title": "Cloud Computing Overview",
				"body":  "Cloud computing provides on-demand computing resources over the internet. Major providers include AWS Azure and Google Cloud.",
				"tags":  "cloud computing aws azure google",
			},
		},
		// Doc 9: About machine learning - "data" in body+tags
		{
			ID: "doc9",
			Fields: map[string]any{
				"title": "Machine Learning Fundamentals",
				"body":  "Machine learning is a subset of artificial intelligence. It uses algorithms to learn from data and make predictions.",
				"tags":  "machine learning ai data algorithms",
			},
		},
		// Doc 10: About DevOps - "development" in body
		{
			ID: "doc10",
			Fields: map[string]any{
				"title": "DevOps Best Practices",
				"body":  "DevOps combines development and operations to improve collaboration. Key practices include CI/CD and infrastructure as code.",
				"tags":  "devops cicd infrastructure automation",
			},
		},
		// Doc 11: About Google (for testing repeated terms)
		{
			ID: "doc11",
			Fields: map[string]any{
				"title": "Google Search Engine",
				"body":  "Google is the most popular search engine. Google was founded in 1998 by Larry Page and Sergey Brin at Google headquarters.",
				"tags":  "google search engine company",
			},
		},
		// Doc 12: About New York (for phrase testing) - phrases like "new york", "united states"
		{
			ID: "doc12",
			Fields: map[string]any{
				"title": "New York City Guide",
				"body":  "New York City is the largest city in the United States. New York is known for the Statue of Liberty and Central Park.",
				"tags":  "new york city travel usa",
			},
		},
		// Doc 13: About Los Angeles (for phrase testing) - "united states" in body
		{
			ID: "doc13",
			Fields: map[string]any{
				"title": "Los Angeles Travel Guide",
				"body":  "Los Angeles is a major city in California in the United States. It is known for Hollywood and beautiful beaches.",
				"tags":  "los angeles california travel usa",
			},
		},
		// Doc 14: About United Kingdom (to differentiate from United States)
		{
			ID: "doc14",
			Fields: map[string]any{
				"title": "United Kingdom Overview",
				"body":  "The United Kingdom consists of England Scotland Wales and Northern Ireland. London is the capital city.",
				"tags":  "united kingdom uk europe london",
			},
		},
		// Doc 15: About football
		{
			ID: "doc15",
			Fields: map[string]any{
				"title": "Football Rules and History",
				"body":  "Football is the most popular sport in the world. The player kicks the ball into the goal to score points.",
				"tags":  "football sport player team ball",
			},
		},
		// Doc 16: About basketball
		{
			ID: "doc16",
			Fields: map[string]any{
				"title": "Basketball Game Rules",
				"body":  "Basketball is a team sport where players score by shooting the ball through a hoop. Each team has five players.",
				"tags":  "basketball sport player team ball",
			},
		},
		// Doc 17: About data structures - "data" in title+body+tags, "programming" in tags
		{
			ID: "doc17",
			Fields: map[string]any{
				"title": "Data Structures Overview",
				"body":  "Data structures organize and store data efficiently. Common structures include arrays lists trees and hash tables.",
				"tags":  "data structures programming algorithms",
			},
		},
		// Doc 18: About algorithms - "programming" in body+tags
		{
			ID: "doc18",
			Fields: map[string]any{
				"title": "Algorithm Design Patterns",
				"body":  "Algorithm design patterns help solve complex problems. Common patterns include divide and conquer dynamic programming and greedy algorithms.",
				"tags":  "algorithms programming patterns",
			},
		},
		// Doc 19: About testing
		{
			ID: "doc19",
			Fields: map[string]any{
				"title": "Software Testing Methods",
				"body":  "Software testing ensures code quality. Types include unit testing integration testing and end to end testing.",
				"tags":  "testing software quality assurance",
			},
		},
		// Doc 20: About security
		{
			ID: "doc20",
			Fields: map[string]any{
				"title": "Cybersecurity Fundamentals",
				"body":  "Cybersecurity protects systems from attacks. Important concepts include encryption authentication and authorization.",
				"tags":  "security cyber encryption protection",
			},
		},
	}
}


type TestCategory struct {
	Name  string
	Cases []TestCase
}

func getTestCategories() []TestCategory {
	return []TestCategory{
		{
			Name: "TERM QUERIES",
			Cases: []TestCase{
				{"programming", []string{"doc1", "doc2", "doc3", "doc17", "doc18"}},
				{"database", []string{"doc4", "doc5", "doc7"}},
				{"google", []string{"doc1", "doc8", "doc11"}},
				{"nonexistent", []string{}},
				{"football", []string{"doc15"}},
				{"basketball", []string{"doc16"}},
				{"player", []string{"doc15", "doc16"}},
				{"data", []string{"doc2", "doc5", "doc9", "doc17"}},
			},
		},
		{
			Name: "FIELD-SPECIFIC QUERIES",
			Cases: []TestCase{
				{"title:programming", []string{"doc1", "doc2", "doc3"}},
				{"title:database", []string{"doc5"}},
				{"title:guide", []string{"doc2", "doc4", "doc12", "doc13"}},
				{"body:google", []string{"doc1", "doc8", "doc11"}},
				{"body:python", []string{"doc2", "doc7"}},
				{"tags:programming", []string{"doc1", "doc2", "doc3", "doc17", "doc18"}},
				{"tags:database", []string{"doc4", "doc5"}},
				{"tags:sport", []string{"doc15", "doc16"}},
				{"tags:usa", []string{"doc12", "doc13"}},
			},
		},
		{
			Name: "PHRASE QUERIES",
			Cases: []TestCase{
				{`"united states"`, []string{"doc12", "doc13"}},
				{`"machine learning"`, []string{"doc2", "doc9"}},
				{`"the united states"`, []string{"doc12", "doc13"}},
				{`body:"united states"`, []string{"doc12", "doc13"}},
				{`title:"united states"`, []string{}},
				{`"python rust"`, []string{}}, 
				{`"new york"`, []string{"doc12"}},
				{`"los angeles"`, []string{"doc13"}},
				{`"united kingdom"`, []string{"doc14"}},
				{`"data science"`, []string{"doc2"}},
				{`"new york city"`, []string{"doc12"}},
				{`title:"new york"`, []string{"doc12"}},
			},
		},
		{
			Name: "PREFIX QUERIES",
			Cases: []TestCase{
				{"prog*", []string{"doc1", "doc2", "doc3", "doc17", "doc18"}},
				{"data*", []string{"doc2", "doc4", "doc5", "doc7", "doc9", "doc17"}},
				{"dev*", []string{"doc6", "doc7", "doc10"}},
				{"foot*", []string{"doc15"}},
				{"bask*", []string{"doc16"}},
				{"postgre*", []string{"doc4"}},
				{"title:prog*", []string{"doc1", "doc2", "doc3"}},
				{"tags:sport*", []string{"doc15", "doc16"}},
			},
		},
		{
			Name: "BOOLEAN AND",
			Cases: []TestCase{
				{"programming AND language", []string{"doc1", "doc2", "doc3"}},
				{"database AND open", []string{"doc4"}},
				{"player AND ball", []string{"doc15", "doc16"}},
				{"united AND states", []string{"doc12", "doc13"}},
				{"football AND basketball", []string{}},
				{"redis AND postgresql", []string{}},
				{"programming AND language AND go", []string{"doc1"}},
				{"web AND development AND react", []string{"doc6"}},
				{"title:programming AND tags:go", []string{"doc1"}},
				{"title:guide AND tags:travel", []string{"doc12", "doc13"}},
			},
		},
		{
			Name: "BOOLEAN OR",
			Cases: []TestCase{
				{"football OR basketball", []string{"doc15", "doc16"}},
				{"postgresql OR redis", []string{"doc4", "doc5"}},
				{"go OR python OR rust", []string{"doc1", "doc2", "doc3", "doc7"}},
				{"programming OR language", []string{"doc1", "doc2", "doc3", "doc17", "doc18"}},
				{"title:guide OR title:overview", []string{"doc2", "doc4", "doc8", "doc12", "doc13", "doc14", "doc17"}},
				{"tags:sport OR tags:travel", []string{"doc12", "doc13", "doc15", "doc16"}},
			},
		},
		{
			Name: "BOOLEAN NOT",
			Cases: []TestCase{
				{"programming AND NOT language", []string{"doc17", "doc18"}},
				{"programming AND -language", []string{"doc17", "doc18"}},
				{"database AND NOT redis", []string{"doc4", "doc7"}},
				{"database AND -redis", []string{"doc4", "doc7"}},
				{"player AND NOT football", []string{"doc16"}},
				{"player AND -football", []string{"doc16"}},
				{"united AND NOT states AND NOT kingdom", []string{}},
				{"united AND -states AND -kingdom", []string{}},
				{"programming AND NOT go AND NOT python AND NOT rust", []string{"doc17", "doc18"}},
				{"programming AND -go AND -python AND -rust", []string{"doc17", "doc18"}},
				{"tags:programming AND NOT tags:language", []string{"doc17", "doc18"}},
				{"tags:programming AND -tags:language", []string{"doc17", "doc18"}},
				{"programming -language", []string{"doc17", "doc18"}}, // Implicit AND
			},
		},
		{
			Name: "GROUPED/NESTED QUERIES",
			Cases: []TestCase{
				{"(football OR basketball) AND player", []string{"doc15", "doc16"}},
				{"(go OR python) AND programming", []string{"doc1", "doc2"}},
				{"(redis OR postgresql) AND database", []string{"doc4", "doc5"}},
				{"united AND (states OR kingdom)", []string{"doc12", "doc13", "doc14"}},
				{"programming AND (systems OR data)", []string{"doc2", "doc3", "doc17"}},
				{"(football OR basketball) AND (player OR team)", []string{"doc15", "doc16"}},
				{"(new OR los) AND (york OR angeles)", []string{"doc12", "doc13"}},
				{"(new AND york) OR (los AND angeles)", []string{"doc12", "doc13"}},
				{"(football AND player) OR (basketball AND team)", []string{"doc15", "doc16"}},
			},
		},
		{
			Name: "COMPLEX MIXED QUERIES",
			Cases: []TestCase{
				{`"united states" AND city`, []string{"doc12", "doc13"}},
				{`"machine learning" AND data`, []string{"doc2", "doc9"}},
				{`"new york" OR "los angeles"`, []string{"doc12", "doc13"}},
				{`"united states" OR "united kingdom"`, []string{"doc12", "doc13", "doc14"}},
				{"prog* AND tags:language", []string{"doc1", "doc2", "doc3"}},
				{"data* AND body:algorithms", []string{"doc9"}},
				{`"united states" AND NOT california`, []string{"doc12"}},
				{`"united states" AND -california`, []string{"doc12"}},
				{`title:guide AND "united states"`, []string{"doc12", "doc13"}},
				{"(foot* OR bask*) AND (player OR team)", []string{"doc15", "doc16"}},
				{"(prog* AND language) OR database", []string{"doc1", "doc2", "doc3", "doc4", "doc5", "doc7"}},
			},
		},
		{
			Name: "EDGE CASES",
			Cases: []TestCase{
				{"nonexistent", []string{}},
				{"nonexistent AND programming", []string{}},
				{"cybersecurity", []string{"doc20"}},
				{"devops", []string{"doc10"}},
				{"programming OR database OR web OR cloud OR machine OR devops OR data OR testing OR security OR sport OR travel",
				 []string{"doc1", "doc2", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8", "doc9", "doc10", "doc12", "doc13", "doc15", "doc16", "doc17", "doc18", "doc19", "doc20"}},
			},
		},
	}
}

