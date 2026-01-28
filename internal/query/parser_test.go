package query

import (
	"testing"
)

func TestParse_TermQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tq := assertTermQuery(t, q)
	if tq.Term != "hello" || tq.Field != "" {
		t.Errorf("got Term=%q Field=%q, want Term=hello Field=", tq.Term, tq.Field)
	}
}

func TestParse_FieldTermQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "title:hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tq := assertTermQuery(t, q)
	if tq.Term != "hello" || tq.Field != "title" {
		t.Errorf("got Term=%q Field=%q, want Term=hello Field=title", tq.Term, tq.Field)
	}
}

func TestParse_PhraseQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, `"hello world"`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pq := assertPhraseQuery(t, q)
	if pq.Phrase != "hello world" || pq.Field != "" {
		t.Errorf("got Phrase=%q Field=%q, want Phrase='hello world' Field=", pq.Phrase, pq.Field)
	}
}

func TestParse_FieldPhraseQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, `title:"go programming"`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pq := assertPhraseQuery(t, q)
	if pq.Phrase != "go programming" || pq.Field != "title" {
		t.Errorf("got Phrase=%q Field=%q, want Phrase='go programming' Field=title", pq.Phrase, pq.Field)
	}
}

func TestParse_PrefixQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "hel*"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pq := assertPrefixQuery(t, q)
	if pq.Prefix != "hel" || pq.Field != "" {
		t.Errorf("got Prefix=%q Field=%q, want Prefix=hel Field=", pq.Prefix, pq.Field)
	}
}

func TestParse_FieldPrefixQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "title:prog*"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pq := assertPrefixQuery(t, q)
	if pq.Prefix != "prog" || pq.Field != "title" {
		t.Errorf("got Prefix=%q Field=%q, want Prefix=prog Field=title", pq.Prefix, pq.Field)
	}
}

func TestParse_RegexQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "/hel.*/"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rq := assertRegexQuery(t, q)
	if rq.Pattern != "hel.*" || rq.Field != "" {
		t.Errorf("got Pattern=%q Field=%q, want Pattern=hel.* Field=", rq.Pattern, rq.Field)
	}
}

func TestParse_FieldRegexQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "body:/test[0-9]+/"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rq := assertRegexQuery(t, q)
	if rq.Pattern != "test[0-9]+" || rq.Field != "body" {
		t.Errorf("got Pattern=%q Field=%q, want Pattern=test[0-9]+ Field=body", rq.Pattern, rq.Field)
	}
}

func TestParse_FuzzyQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "hello~2"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fq := assertFuzzyQuery(t, q)
	if fq.Term != "hello" || fq.Fuzziness != 2 || fq.Field != "" {
		t.Errorf("got Term=%q Fuzziness=%d Field=%q, want Term=hello Fuzziness=2 Field=", fq.Term, fq.Fuzziness, fq.Field)
	}
}

func TestParse_FuzzyDefaultFuzziness(t *testing.T) {
	q, err := Parse(mustTokenize(t, "hello~"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fq := assertFuzzyQuery(t, q)
	if fq.Fuzziness != 1 {
		t.Errorf("got Fuzziness=%d, want 1", fq.Fuzziness)
	}
}

func TestParse_FieldFuzzyQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "title:wrold~1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fq := assertFuzzyQuery(t, q)
	if fq.Term != "wrold" || fq.Fuzziness != 1 || fq.Field != "title" {
		t.Errorf("got Term=%q Fuzziness=%d Field=%q, want Term=wrold Fuzziness=1 Field=title", fq.Term, fq.Fuzziness, fq.Field)
	}
}

func TestParse_AndQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "hello AND world"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bq := assertBoolQuery(t, q)
	if len(bq.Must) != 2 || len(bq.Should) != 0 || len(bq.MustNot) != 0 {
		t.Fatalf("got Must=%d Should=%d MustNot=%d, want Must=2 Should=0 MustNot=0", len(bq.Must), len(bq.Should), len(bq.MustNot))
	}
	// Verify clause contents
	t1 := assertTermQuery(t, bq.Must[0])
	t2 := assertTermQuery(t, bq.Must[1])
	if t1.Term != "hello" || t2.Term != "world" {
		t.Errorf("got terms %q and %q, want hello and world", t1.Term, t2.Term)
	}
}

func TestParse_OrQuery(t *testing.T) {
	q, err := Parse(mustTokenize(t, "hello OR world"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bq := assertBoolQuery(t, q)
	if len(bq.Should) != 2 || len(bq.Must) != 0 || len(bq.MustNot) != 0 {
		t.Fatalf("got Must=%d Should=%d MustNot=%d, want Must=0 Should=2 MustNot=0", len(bq.Must), len(bq.Should), len(bq.MustNot))
	}
	t1 := assertTermQuery(t, bq.Should[0])
	t2 := assertTermQuery(t, bq.Should[1])
	if t1.Term != "hello" || t2.Term != "world" {
		t.Errorf("got terms %q and %q, want hello and world", t1.Term, t2.Term)
	}
}

func TestParse_NotQuery(t *testing.T) {
	// "hello NOT world" parses as: hello AND (NOT world)
	// which is BoolQuery{Must: [hello, BoolQuery{MustNot: [world]}]}
	q, err := Parse(mustTokenize(t, "hello NOT world"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bq := assertBoolQuery(t, q)
	if len(bq.Must) != 2 {
		t.Fatalf("got Must=%d, want 2", len(bq.Must))
	}
	// First clause is "hello"
	t1 := assertTermQuery(t, bq.Must[0])
	if t1.Term != "hello" {
		t.Errorf("first clause: got %q, want hello", t1.Term)
	}
	// Second clause is BoolQuery with MustNot containing "world"
	nested := assertBoolQuery(t, bq.Must[1])
	if len(nested.MustNot) != 1 {
		t.Fatalf("nested MustNot=%d, want 1", len(nested.MustNot))
	}
	t2 := assertTermQuery(t, nested.MustNot[0])
	if t2.Term != "world" {
		t.Errorf("MustNot clause: got %q, want world", t2.Term)
	}
}

func TestParse_GroupedQuery(t *testing.T) {
	// "(hello OR world) AND foo" should parse as:
	// BoolQuery{Must: [BoolQuery{Should: [hello, world]}, foo]}
	q, err := Parse(mustTokenize(t, "(hello OR world) AND foo"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bq := assertBoolQuery(t, q)
	if len(bq.Must) != 2 {
		t.Fatalf("got Must=%d, want 2", len(bq.Must))
	}
	// First clause is the grouped OR
	grouped := assertBoolQuery(t, bq.Must[0])
	if len(grouped.Should) != 2 {
		t.Fatalf("grouped Should=%d, want 2", len(grouped.Should))
	}
	// Second clause is "foo"
	foo := assertTermQuery(t, bq.Must[1])
	if foo.Term != "foo" {
		t.Errorf("got %q, want foo", foo.Term)
	}
}

func TestParse_ImplicitAnd(t *testing.T) {
	q, err := Parse(mustTokenize(t, "hello world foo"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bq := assertBoolQuery(t, q)
	if len(bq.Must) != 3 {
		t.Fatalf("got Must=%d, want 3", len(bq.Must))
	}
	terms := []string{"hello", "world", "foo"}
	for i, expected := range terms {
		tq := assertTermQuery(t, bq.Must[i])
		if tq.Term != expected {
			t.Errorf("clause %d: got %q, want %q", i, tq.Term, expected)
		}
	}
}

func TestParse_EmptyInput(t *testing.T) {
	q, err := Parse([]Token{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q != nil {
		t.Errorf("expected nil for empty input, got %T", q)
	}

	q, err = Parse([]Token{{Type: TokenEOF}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q != nil {
		t.Errorf("expected nil for EOF only, got %T", q)
	}
}

func TestParse_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"unmatched paren", "(hello world"},
		{"field without value", "title:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, _ := Tokenize(tt.input)
			_, err := Parse(tokens)
			if err == nil {
				t.Errorf("expected error for %q", tt.input)
			}
		})
	}
}

func TestParse_InvalidFuzziness(t *testing.T) {
	tokens := []Token{
		{Type: TokenFuzzy, Value: "hello~5"},
		{Type: TokenEOF},
	}
	_, err := Parse(tokens)
	if err == nil {
		t.Error("expected error for fuzziness > 2")
	}
}

// Helper functions

func mustTokenize(t *testing.T, input string) []Token {
	t.Helper()
	tokens, err := Tokenize(input)
	if err != nil {
		t.Fatalf("Tokenize(%q) error: %v", input, err)
	}
	return tokens
}

func assertTermQuery(t *testing.T, q Query) *TermQuery {
	t.Helper()
	tq, ok := q.(*TermQuery)
	if !ok {
		t.Fatalf("expected *TermQuery, got %T", q)
	}
	return tq
}

func assertPhraseQuery(t *testing.T, q Query) *PhraseQuery {
	t.Helper()
	pq, ok := q.(*PhraseQuery)
	if !ok {
		t.Fatalf("expected *PhraseQuery, got %T", q)
	}
	return pq
}

func assertPrefixQuery(t *testing.T, q Query) *PrefixQuery {
	t.Helper()
	pq, ok := q.(*PrefixQuery)
	if !ok {
		t.Fatalf("expected *PrefixQuery, got %T", q)
	}
	return pq
}

func assertRegexQuery(t *testing.T, q Query) *RegexQuery {
	t.Helper()
	rq, ok := q.(*RegexQuery)
	if !ok {
		t.Fatalf("expected *RegexQuery, got %T", q)
	}
	return rq
}

func assertFuzzyQuery(t *testing.T, q Query) *FuzzyQuery {
	t.Helper()
	fq, ok := q.(*FuzzyQuery)
	if !ok {
		t.Fatalf("expected *FuzzyQuery, got %T", q)
	}
	return fq
}

func assertBoolQuery(t *testing.T, q Query) *BoolQuery {
	t.Helper()
	bq, ok := q.(*BoolQuery)
	if !ok {
		t.Fatalf("expected *BoolQuery, got %T", q)
	}
	return bq
}
