package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"harshagw/postings/internal/index"
	"harshagw/postings/internal/search"

	"github.com/c-bata/go-prompt"
)

const IndexDir = ".history"

type REPL struct {
	idx *index.Index
}

func main() {
	fmt.Println("Postings Search Engine REPL")
	fmt.Println()
	printHelp()
	fmt.Println()

	// Open index at fixed location
	config := index.DefaultConfig(IndexDir)
	idx, err := index.New(config)
	if err != nil {
		fmt.Printf("Error opening index: %v\n", err)
		os.Exit(1)
	}

	r := &REPL{idx: idx}
	fmt.Printf("Index loaded from %s (%d segments)\n\n", IndexDir, idx.NumSegments())

	p := prompt.New(
		r.executor,
		func(d prompt.Document) []prompt.Suggest { return nil },
		prompt.OptionPrefix("postings >> "),
		prompt.OptionTitle("postings"),
	)
	p.Run()
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  index <docID> <json>         - Add document to batch")
	fmt.Println("  delete <docID>               - Mark document as deleted")
	fmt.Println("  flush                        - Write batch to new segment")
	fmt.Println("  merge                        - Merge segments, remove deleted docs")
	fmt.Println("  search [--field=F] <query>   - Term/phrase search (optionally in field F)")
	fmt.Println("  search [--field=F] --and ... - AND search: docs containing ALL terms")
	fmt.Println("  search [--field=F] --or ...  - OR search: docs containing ANY term")
	fmt.Println("  segments                     - List all segments")
	fmt.Println("  segment <id> stats           - Show segment details")
	fmt.Println("  doc <segment> <docNum>       - Load stored document")
	fmt.Println("  dump postings <field> <term> - Show posting list")
	fmt.Println("  dump deletions <segment>     - Show deletion bitmap")
	fmt.Println("  help                         - Show this help")
	fmt.Println("  quit                         - Exit")
}

func (r *REPL) executor(input string) {
	input = strings.TrimSpace(input)
	if input == "" {
		return
	}

	parts := strings.Fields(input)
	cmd := parts[0]

	switch cmd {
	case "index":
		r.cmdIndex(input)
	case "delete":
		r.cmdDelete(parts[1:])
	case "flush":
		r.cmdFlush()
	case "merge":
		r.cmdMerge()
	case "search":
		r.cmdSearch(parts[1:])
	case "segments":
		r.cmdSegments()
	case "segment":
		r.cmdSegment(parts[1:])
	case "doc":
		r.cmdDoc(parts[1:])
	case "dump":
		r.cmdDump(parts[1:])
	case "help":
		printHelp()
	case "quit", "exit":
		fmt.Println("Goodbye!")
		r.idx.Close()
		os.Exit(0)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
	}
}

func (r *REPL) cmdIndex(input string) {
	parts := strings.SplitN(input, " ", 3)
	if len(parts) < 3 {
		fmt.Println("Usage: index <docID> <json>")
		return
	}

	docID := parts[1]
	jsonStr := parts[2]

	var doc map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		return
	}

	if err := r.idx.Index(docID, doc); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Indexed '%s' (%d fields)\n", docID, len(doc))
}

func (r *REPL) cmdDelete(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: delete <docID>")
		return
	}

	docID := args[0]
	if err := r.idx.Delete(docID); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Deleted '%s'\n", docID)
}

func (r *REPL) cmdFlush() {
	if err := r.idx.Flush(); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Flushed. %d segments.\n", r.idx.NumSegments())
}

func (r *REPL) cmdMerge() {
	if err := r.idx.ForceMerge(); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Merged. %d segments.\n", r.idx.NumSegments())
}

func (r *REPL) cmdSearch(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: search [--field=<name>] <query>")
		fmt.Println("       search [--field=<name>] --and <term1> <term2> ...")
		fmt.Println("       search [--field=<name>] --or <term1> <term2> ...")
		return
	}

	// Parse --field flag
	field := ""
	if f, ok := strings.CutPrefix(args[0], "--field="); ok {
		field = f
		args = args[1:]
		if len(args) < 1 {
			fmt.Println("Error: missing query after --field")
			return
		}
	}

	snap, err := r.idx.Snapshot()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer snap.Close()

	searcher := search.New(snap)

	var results []search.Result
	var queryDesc string

	// Check for --and or --or flags
	if args[0] == "--and" {
		if len(args) < 2 {
			fmt.Println("Usage: search --and <term1> <term2> ...")
			return
		}
		terms := make([]string, len(args)-1)
		for i, t := range args[1:] {
			terms[i] = strings.ToLower(t)
		}
		queryDesc = "AND(" + strings.Join(terms, ", ") + ")"
		results, err = searcher.AndSearch(terms, field)
	} else if args[0] == "--or" {
		if len(args) < 2 {
			fmt.Println("Usage: search --or <term1> <term2> ...")
			return
		}
		terms := make([]string, len(args)-1)
		for i, t := range args[1:] {
			terms[i] = strings.ToLower(t)
		}
		queryDesc = "OR(" + strings.Join(terms, ", ") + ")"
		results, err = searcher.OrSearch(terms, field)
	} else if len(args) == 1 {
		query := strings.ToLower(args[0])
		queryDesc = query
		results, err = searcher.Search(query, field)
	} else {
		query := strings.ToLower(strings.Join(args, " "))
		queryDesc = "\"" + query + "\""
		results, err = searcher.PhraseSearch(query, field)
	}

	if field != "" {
		queryDesc += " in:" + field
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(results) == 0 {
		fmt.Printf("No results for %s\n", queryDesc)
	} else {
		fmt.Printf("Found %d results for %s:\n", len(results), queryDesc)
		for _, res := range results {
			fmt.Printf("  %s (%.4f)\n", res.DocID, res.Score)
		}
	}
}

func (r *REPL) cmdSegments() {
	segs := r.idx.Segments()
	if len(segs) == 0 {
		fmt.Println("No segments")
		return
	}
	fmt.Printf("%d segments:\n", len(segs))
	for _, seg := range segs {
		fmt.Printf("  %s: %d docs\n", seg.ID, seg.NumDocs)
	}
}

func (r *REPL) cmdSegment(args []string) {
	if len(args) < 2 || args[1] != "stats" {
		fmt.Println("Usage: segment <id> stats")
		return
	}

	segID := args[0]
	info, err := r.idx.SegmentStats(segID)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Segment %s:\n", segID)
	fmt.Printf("  Documents: %d\n", info.NumDocs)
	fmt.Printf("  Deleted: %d\n", info.NumDeleted)
	fmt.Printf("  Fields: %v\n", info.Fields)
}

func (r *REPL) cmdDoc(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: doc <segment> <docNum>")
		return
	}

	segID := args[0]
	docNum, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		fmt.Printf("Invalid docNum: %v\n", err)
		return
	}

	doc, err := r.idx.LoadDoc(segID, docNum)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	data, _ := json.MarshalIndent(doc, "", "  ")
	fmt.Println(string(data))
}

func (r *REPL) cmdDump(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: dump postings <field> <term>")
		fmt.Println("       dump deletions <segment>")
		return
	}

	switch args[0] {
	case "postings":
		if len(args) < 3 {
			fmt.Println("Usage: dump postings <field> <term>")
			return
		}
		r.dumpPostings(args[1], args[2])
	case "deletions":
		r.dumpDeletions(args[1])
	default:
		fmt.Printf("Unknown dump type: %s\n", args[0])
	}
}

func (r *REPL) dumpPostings(field, term string) {
	postings, err := r.idx.DumpPostings(field, term)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(postings) == 0 {
		fmt.Printf("No postings for %s:%s\n", field, term)
		return
	}

	fmt.Printf("Postings for %s:%s (%d docs):\n", field, term, len(postings))
	for _, p := range postings {
		fmt.Printf("  doc=%d freq=%d pos=%v\n", p.DocNum, p.Freq, p.Positions)
	}
}

func (r *REPL) dumpDeletions(segID string) {
	deleted, err := r.idx.DumpDeletions(segID)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(deleted) == 0 {
		fmt.Printf("No deletions in segment %s\n", segID)
		return
	}

	fmt.Printf("Deletions in %s: %v\n", segID, deleted)
}
