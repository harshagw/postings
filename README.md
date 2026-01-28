# postings

A full-text search engine written in Go with LSM-style writes and immutable segments.

Read the blog post: [Building a Search Engine](https://harshagw.github.io/blog/building-a-search-engine/)

## Features

- **Inverted Index** with FST (Finite State Transducer) dictionaries using [Vellum](https://github.com/couchbase/vellum)
- **Immutable Segments** with memory-mapped I/O for efficient disk access
- **Rich Query Support**: Term, Phrase, Prefix, Regex, Fuzzy, and Boolean queries
- **Relevance Scoring**: TF-IDF and BM25 scoring algorithms
- **Logical Deletions** via Roaring Bitmaps - segments remain immutable
- **Segment Merging** to reclaim space and optimize query performance
- **JSON Document Indexing** with per-field search

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Index                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  In-Memory  │  │  Segment 1  │  │  Segment 2  │ ...   │  │
│  │   Builder   │  │   (mmap)    │  │   (mmap)    │       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         │                │                  │               │
│         └────────────────┴──────────────────┘               │
│                          │                                  │
│                    ┌─────┴─────┐                            │
│                    │  Metadata │  (BoltDB)                  │
│                    │ - Segments│                            │
│                    │ - Deletions│                           │
│                    │ - DocIDs  │                            │
│                    └───────────┘                            │
└─────────────────────────────────────────────────────────────┘
```

### How It Works

1. **Indexing**: Documents are first indexed in an in-memory builder. When the builder reaches a threshold (default: 1000 docs), it's flushed to disk as an immutable segment.

2. **Segments**: Each segment is a complete inverted index containing:
   - Per-field FST-based term dictionaries (each field has its own FST mapping terms to posting list offsets)
   - Posting lists with document IDs, term frequencies, and positions
   - Stored fields (compressed with Snappy)
   - Document ID mapping via a special `_id` field FST for fast lookups

3. **Deletions**: Documents are never physically deleted from segments. Instead, deletion bitmaps track which documents are logically deleted. These bitmaps are stored in BoltDB metadata.

4. **Searching**: Queries run against all segments (in-memory + on-disk). Results are merged, deleted documents filtered out, and scored using BM25 or TF-IDF.

5. **Merging**: Multiple segments can be merged into one, physically removing deleted documents and reducing query overhead.

## Installation

```bash
go get harshagw/postings
```

Or clone and build:

```bash
git clone https://github.com/harshagw/postings.git
cd postings
go build -o postings ./cmd/postings
```

## Usage

### REPL

Start the interactive REPL:

```bash
./postings
# or
make run
```

## Query Syntax

| Query Type | Syntax           | Example                |
| ---------- | ---------------- | ---------------------- |
| Term       | `word`           | `hello`                |
| Field      | `field:word`     | `title:hello`          |
| Phrase     | `"exact phrase"` | `"hello world"`        |
| Prefix     | `prefix*`        | `hel*`                 |
| Regex      | `/pattern/`      | `/hel+o/`              |
| Fuzzy      | `word~N`         | `hello~1`              |
| AND        | `a AND b`        | `hello AND world`      |
| OR         | `a OR b`         | `hello OR world`       |
| NOT        | `-word`          | `hello -spam`          |
| Grouping   | `(a OR b)`       | `(cat OR dog) AND pet` |

## Programmatic API

```go
package main

import (
    "fmt"
    "harshagw/postings/internal/index"
    "harshagw/postings/internal/search"
)

func main() {
    // Create or open an index
    config := index.DefaultConfig("./my-index")
    idx, err := index.New(config)
    if err != nil {
        panic(err)
    }
    defer idx.Close()

    // Index documents
    idx.Index("doc1", map[string]any{
        "title": "Introduction to Go",
        "body":  "Go is a statically typed programming language.",
    })
    idx.Index("doc2", map[string]any{
        "title": "Python Tutorial",
        "body":  "Python is a dynamic programming language.",
    })

    // Flush to disk
    idx.Flush()

    // Search
    snapshot, _ := idx.Snapshot()
    defer snapshot.Close()

    searcher := search.New(snapshot)
    results, _ := searcher.RunQueryString("programming AND language")

    for _, r := range results {
        fmt.Printf("%s (score: %.4f)\n", r.DocID, r.Score)
    }
}
```

## Configuration

```go
config := index.Config{
    Dir:            "./index",    // Index directory
    FlushThreshold: 1000,         // Docs before auto-flush
    Analyzer:       analysis.NewSimple(), // Text analyzer
    ScoringMode:    index.ScoringBM25,    // BM25 or TF-IDF
}
```

## Dependencies

- [vellum](https://github.com/couchbase/vellum) - FST implementation for term dictionaries
- [roaring](https://github.com/RoaringBitmap/roaring) - Compressed bitmaps for deletions
- [bolt](https://github.com/boltdb/bolt) - Embedded key-value store for metadata
- [mmap-go](https://github.com/edsrzf/mmap-go) - Memory-mapped file I/O
- [snappy](https://github.com/golang/snappy) - Fast compression for stored fields
- [go-prompt](https://github.com/c-bata/go-prompt) - Interactive REPL

## What This Implementation Omits

This is an educational implementation. Production search engines like Elasticsearch or Bleve include:

- Advanced compression (posting list delta encoding, etc.)
- Distributed features (sharding, replication, clustering)
- Query optimization and caching
- Highlighting and snippets
- More sophisticated text analysis (stemming, synonyms, etc.)
- Numeric and date range queries
- Sorting and faceting

## License

MIT
