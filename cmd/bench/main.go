package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/search"
)

const numDocs = 10000

func main() {
	benchDir := getBenchDir()

	// Handle download command
	if len(os.Args) >= 2 && os.Args[1] == "download" {
		target := 10000 // default target
		if len(os.Args) >= 3 {
			if t, err := strconv.Atoi(os.Args[2]); err == nil {
				target = t
			}
		}

		fmt.Println("Wikipedia Data Downloader")
		fmt.Println("=========================")
		fmt.Println()

		if err := DownloadMoreDocs(benchDir, target); err != nil {
			fmt.Printf("\nError: %v\n", err)
			fmt.Println("\nRun 'go run ./cmd/bench download' again to resume.")
			os.Exit(1)
		}
		return
	}

	// Run benchmark
	fmt.Println("Search Engine Benchmark")
	fmt.Println("=======================")
	fmt.Println()

	benchStart := time.Now()

	// Load test data
	docs := loadDocs(benchDir)
	fmt.Printf("Loaded %d Wikipedia articles\n\n", len(docs))

	// Run indexing benchmark
	idx := runIndexingBenchmark(docs)
	defer idx.Close()

	// Show index info
	printIndexInfo(idx)

	// Build searcher
	snapshot, _ := idx.Snapshot()
	defer snapshot.Close()
	searcher := search.New(snapshot)
	defer searcher.Close()

	// Run all query benchmarks
	runAllQueryBenchmarks(searcher)

	// Print total time
	fmt.Printf("Total time: %.2f seconds\n", time.Since(benchStart).Seconds())
}

func getBenchDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Dir(filename)
}

func loadDocs(benchDir string) []Doc {
	cachePath := filepath.Join(benchDir, cacheFile)

	data, err := os.ReadFile(cachePath)
	if err != nil {
		fmt.Printf("Error: No test data found at %s\n", cachePath)
		fmt.Println("Run 'go run ./cmd/bench download' to download Wikipedia data")
		os.Exit(1)
	}

	var docs []Doc
	if err := json.Unmarshal(data, &docs); err != nil {
		fmt.Printf("Error parsing test data: %v\n", err)
		os.Exit(1)
	}

	if len(docs) < numDocs {
		fmt.Printf("Warning: Only %d docs available (wanted %d)\n", len(docs), numDocs)
		fmt.Println("Run 'go run ./cmd/bench download' to get more data")
		return docs
	}
	return docs[:numDocs]
}

func runIndexingBenchmark(docs []Doc) *index.Index {
	fmt.Println("INDEXING")
	fmt.Println("--------")

	// Warm up run
	dir, _ := os.MkdirTemp("", "bench-warmup-*")
	cfg := index.DefaultConfig(dir)
	cfg.FlushThreshold = 2000
	idx, _ := index.New(cfg)
	for _, d := range docs[:100] {
		idx.Index(d.ID, d.Fields)
	}
	idx.Close()
	os.RemoveAll(dir)

	// Benchmark runs
	var totalTime time.Duration
	runs := 3

	var lastDir string
	var lastIdx *index.Index

	for i := 0; i < runs; i++ {
		dir, _ := os.MkdirTemp("", "bench-*")
		start := time.Now()

		cfg := index.DefaultConfig(dir)
		cfg.FlushThreshold = 1000 // Create multiple segments
		idx, _ := index.New(cfg)

		for _, d := range docs {
			idx.Index(d.ID, d.Fields)
		}
		idx.Flush()

		elapsed := time.Since(start)
		totalTime += elapsed

		if lastIdx != nil {
			lastIdx.Close()
			os.RemoveAll(lastDir)
		}
		lastDir = dir
		lastIdx = idx
	}

	avgTime := totalTime / time.Duration(runs)
	throughput := float64(len(docs)) / avgTime.Seconds()

	fmt.Printf("  Documents:  %d\n", len(docs))
	fmt.Printf("  Time:       %v\n", avgTime.Round(time.Millisecond))
	fmt.Printf("  Throughput: %.0f docs/sec\n", throughput)
	fmt.Println()

	return lastIdx
}

func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
	)
	if bytes >= MB {
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	}
	if bytes >= KB {
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	}
	return fmt.Sprintf("%d B", bytes)
}

func printIndexInfo(idx *index.Index) {
	fmt.Println("INDEX INFO")
	fmt.Println("----------")
	fmt.Printf("  Segments: %d\n", idx.NumSegments())

	var totalSize int64
	var totalDocs uint64

	for _, seg := range idx.Segments() {
		stats, err := idx.SegmentStats(seg.ID)
		if err != nil {
			continue
		}
		totalSize += stats.SizeBytes
		totalDocs += stats.NumDocs
		fmt.Printf("    [%s] %d docs, %s, fields: %v\n",
			seg.ID, stats.NumDocs, formatBytes(stats.SizeBytes), stats.Fields)
	}

	fmt.Println()
	fmt.Printf("  Total Size: %s\n", formatBytes(totalSize))
	fmt.Printf("  Avg/Doc:    %s\n", formatBytes(totalSize/int64(totalDocs)))
	fmt.Println()
}

func runAllQueryBenchmarks(s *search.Searcher) {
	// ============================================================
	// TERM QUERIES - Based on actual word frequencies
	// ============================================================
	fmt.Println("TERM QUERIES")
	fmt.Println("------------")
	runQueries(s, []string{
		// Very common (39k occurrences)
		"the",
		// Common (5k+ occurrences)
		"saint",
		"united",
		"states",
		// Medium frequency (500 occurrences)
		"football",
		"general",
		"released",
		// Rare (50 occurrences)
		"highway",
		"newspaper",
		"broadcast",
		// Very rare (10 occurrences)
		"periodic",
		"berkeley",
	})

	// ============================================================
	// FIELD-SPECIFIC QUERIES
	// ============================================================
	fmt.Println("FIELD QUERIES")
	fmt.Println("-------------")
	runQueries(s, []string{
		// Title field (short, ~16 chars avg)
		"title:list",
		"title:county",
		"title:district",
		"title:film",
		"title:movie",
		// Body field (long, ~1877 chars avg)
		"body:wikipedia",
		"body:references",
		"body:population",
		"body:government",
		// Summary field (~357 chars avg, 94% of docs)
		"summary:retrieved",
		"summary:archived",
	})

	// ============================================================
	// PHRASE QUERIES - Based on actual bigrams/trigrams
	// ============================================================
	fmt.Println("PHRASE QUERIES")
	fmt.Println("--------------")
	runQueries(s, []string{
		// Very common phrases (2000+ occurrences)
		`"united states"`,
		`"short article"`,
		`"help wikipedia"`,
		// Common phrases (500-1000)
		`"new york"`,
		`"the first"`,
		`"other websites"`,
		// Medium frequency (100-500)
		`"south africa"`,
		`"world war"`,
		`"los angeles"`,
		// Rare phrases
		`"periodic table"`,
		`"grand duke"`,
		// 3-word phrases
		`"the united states"`,
		`"new york city"`,
		`"united arab emirates"`,
		// Field + phrase
		`title:"list of"`,
		`body:"archived from the"`,
	})

	// ============================================================
	// PREFIX/WILDCARD - Based on prefix analysis
	// ============================================================
	fmt.Println("PREFIX QUERIES")
	fmt.Println("--------------")
	runQueries(s, []string{
		// Very broad (14k+ words match)
		"co*",
		"re*",
		"st*",
		// Broad (3k-4k words)
		"com*",
		"sta*",
		"pro*",
		// Medium (1k-2k words)
		"wiki*",
		"stat*",
		"coun*",
		// Narrow (100-500 words)
		"comp*",
		"inter*",
		// Very narrow (<50 words)
		"footb*",
		"newsp*",
		// Field + prefix
		"title:list*",
		"title:dist*",
		"body:refer*",
	})

	// ============================================================
	// BOOLEAN AND
	// ============================================================
	fmt.Println("BOOLEAN AND")
	fmt.Println("-----------")
	runQueries(s, []string{
		// Common AND common
		"the AND was",
		"united AND states",
		// Common AND medium
		"the AND football",
		"states AND population",
		// Common AND rare
		"the AND periodic",
		"wikipedia AND berkeley",
		// Medium AND medium
		"football AND player",
		"county AND population",
		// 3-way AND
		"united AND states AND population",
		"new AND york AND city",
		// 4-way AND
		"the AND united AND states AND america",
		// 5-way AND (stress test)
		"the AND and AND was AND from AND with",
		// Field combinations
		"title:county AND body:population",
		"title:list AND body:references",
		"title:film AND body:released AND body:director",
	})

	// ============================================================
	// BOOLEAN OR
	// ============================================================
	fmt.Println("BOOLEAN OR")
	fmt.Println("----------")
	runQueries(s, []string{
		// Common OR common (lots of results)
		"the OR and",
		"united OR states",
		// Common OR rare
		"the OR periodic",
		"wikipedia OR berkeley",
		// Medium OR medium
		"football OR basketball",
		"county OR district",
		// 3-way OR
		"football OR basketball OR baseball",
		"county OR district OR province",
		// 4-way OR
		"film OR movie OR cinema OR theatre",
		// 5-way OR (stress test)
		"football OR basketball OR baseball OR hockey OR tennis",
		// Field OR
		"title:list OR title:county",
		"title:film OR title:movie OR title:song",
		"body:population OR body:government OR body:economy",
	})

	// ============================================================
	// NOT/NEGATION
	// ============================================================
	fmt.Println("NOT/NEGATION")
	fmt.Println("------------")
	runQueries(s, []string{
		// Subtract common term
		"united AND -the",
		"states AND -united",
		// Subtract medium term
		"football AND -player",
		"county AND -texas",
		// Subtract rare term
		"population AND -census",
		// Multiple negations
		"united AND -states AND -kingdom",
		"film AND -movie AND -released",
		"county AND -texas AND -iowa AND -california",
		// Field negation
		"population AND -title:list",
		"football AND -title:county",
		"title:film AND -body:horror",
		// Negate field term
		"county AND -body:texas",
		"list AND -body:references",
	})

	// ============================================================
	// GROUPED/NESTED
	// ============================================================
	fmt.Println("GROUPED/NESTED")
	fmt.Println("--------------")
	runQueries(s, []string{
		// (A OR B) AND C
		"(united OR kingdom) AND states",
		"(football OR basketball) AND player",
		"(film OR movie) AND released",
		// A AND (B OR C)
		"population AND (county OR district)",
		"references AND (archived OR retrieved)",
		// (A OR B) AND (C OR D)
		"(united OR kingdom) AND (states OR england)",
		"(film OR movie) AND (released OR directed)",
		"(football OR basketball) AND (player OR team)",
		// Nested: ((A OR B) AND C) OR D
		"((united OR kingdom) AND states) OR america",
		"((film OR movie) AND released) OR directed",
		// (A AND B) OR (C AND D)
		"(united AND states) OR (united AND kingdom)",
		"(new AND york) OR (los AND angeles)",
		// 3-level nesting
		"((title:film OR title:movie) AND body:released) OR body:directed",
	})

	// ============================================================
	// COMPLEX MIXED
	// ============================================================
	fmt.Println("COMPLEX MIXED")
	fmt.Println("-------------")
	runQueries(s, []string{
		// Phrase + boolean
		`"united states" AND population`,
		`"new york" OR "los angeles"`,
		`"world war" AND -germany`,
		// Prefix + boolean
		"foot* AND player",
		"coun* AND population",
		"(foot* OR bask*) AND team",
		// Field + phrase + boolean
		`title:list AND "united states"`,
		`title:county AND body:"population of"`,
		// Field + prefix + boolean
		"title:list* AND body:population",
		"title:dist* AND body:government",
		// Everything combined
		`(title:film OR title:movie) AND "united states" AND -horror`,
		`title:county AND (body:population OR body:government) AND -body:texas`,
		`(foot* OR bask*) AND (player OR team) AND -title:list`,
		// Phrase + prefix + NOT + nested
		`("united states" OR "united kingdom") AND govern* AND -title:list`,
		`(title:list* OR title:county*) AND body:"population of" AND -body:census`,
		// Stress: many clauses
		`(film OR movie OR cinema) AND (released OR directed OR produced) AND (title:the OR title:a) AND -horror AND -comedy`,
		`(united OR kingdom OR states OR america) AND (population OR government OR economy) AND -title:list AND -body:census`,
	})

}

func runQueries(s *search.Searcher, queries []string) {
	for _, q := range queries {
		latency, hits := benchmarkQuery(s, q)
		fmt.Printf("  %-55s %s  (%d hits)\n", q, formatLatency(latency), hits)
	}
	fmt.Println()
}

func benchmarkQuery(s *search.Searcher, query string) (time.Duration, int) {
	var hits int

	// Warm up
	for i := 0; i < 10; i++ {
		results, _ := s.RunQueryString(query)
		hits = len(results)
	}

	// Benchmark
	iterations := 500
	start := time.Now()
	for i := 0; i < iterations; i++ {
		s.RunQueryString(query)
	}
	return time.Since(start) / time.Duration(iterations), hits
}

func formatLatency(d time.Duration) string {
	return fmt.Sprintf("%8.2f µs", float64(d.Nanoseconds())/1000)
}
