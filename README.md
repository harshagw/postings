# postings

A full-text search engine with LSM-style writes and immutable segments.

## Features

- Inverted index with FST dictionaries
- Immutable segments with mmap
- Term / AND / OR / Phrase queries / Regex / Prefix / Fuzzy queries
- TF-IDF scoring
- Tombstone deletes via roaring bitmaps
- Interactive REPL (go-prompt)

## Usage

```bash
# Build and run
go build -o postings ./cmd/postings
./postings

# REPL commands
postings> index doc1 {"title": "hello world", "body": "this is a test"}
postings> search hello
postings> delete doc1
postings> flush
postings> quit
```
