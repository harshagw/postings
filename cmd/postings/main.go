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
	fmt.Println("  index <docID> <json>       - Add document")
	fmt.Println("  delete <docID>             - Delete document")
	fmt.Println("  flush                      - Flush to segment")
	fmt.Println("  merge                      - Merge all segments")
	fmt.Println()
	fmt.Println("  search <query>             - Search with query syntax:")
	fmt.Println("    term                     - Single term search")
	fmt.Println("    field:term               - Field-specific search")
	fmt.Println("    \"exact phrase\"           - Phrase search")
	fmt.Println("    term1 AND term2          - Both must match")
	fmt.Println("    term1 OR term2           - Either matches")
	fmt.Println("    term1 -term2             - Exclude term2")
	fmt.Println("    (a OR b) AND c           - Grouping")
	fmt.Println("    term*                    - Prefix search")
	fmt.Println()
	fmt.Println("  segments                   - List segments")
	fmt.Println("  segment <id> stats         - Segment details")
	fmt.Println("  doc <segment> <docNum>     - Load document")
	fmt.Println("  dump postings <field> <term>")
	fmt.Println("  dump deletions <segment>")
	fmt.Println("  help                       - Show this help")
	fmt.Println("  quit                       - Exit")
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
		r.cmdSearch(input)
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

func (r *REPL) cmdSearch(input string) {
	query := strings.TrimPrefix(input, "search")
	query = strings.TrimSpace(query)

	if query == "" {
		fmt.Println("Usage: search <query>")
		fmt.Println("Examples:")
		fmt.Println("  search hello")
		fmt.Println("  search title:hello")
		fmt.Println("  search \"hello world\"")
		fmt.Println("  search hello AND world")
		fmt.Println("  search hello OR world")
		fmt.Println("  search hello -spam")
		fmt.Println("  search hel*")
		return
	}

	snap, err := r.idx.Snapshot()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer snap.Close()

	searcher := search.New(snap)
	results, err := searcher.Query(query)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(results) == 0 {
		fmt.Printf("No results for: %s\n", query)
	} else {
		fmt.Printf("Found %d results for: %s\n", len(results), query)
		for _, res := range results {
			if len(res.MatchedTerms) > 0 {
				fmt.Printf("  %s (%.4f) [%s]\n", res.DocID, res.Score, strings.Join(res.MatchedTerms, ", "))
			} else {
				fmt.Printf("  %s (%.4f)\n", res.DocID, res.Score)
			}
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
