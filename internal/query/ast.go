package query

import (
	"fmt"
	"strings"
)

// Query is the interface for all query types.
type Query interface {
	queryNode()
	String() string
}

// TermQuery searches for a single term.
type TermQuery struct {
	Field string
	Term  string
}

func (q *TermQuery) queryNode() {}

func (q *TermQuery) String() string {
	if q.Field != "" {
		return fmt.Sprintf("term(%s:%s)", q.Field, q.Term)
	}
	return fmt.Sprintf("term(%s)", q.Term)
}

// PhraseQuery searches for an exact phrase.
type PhraseQuery struct {
	Field  string
	Phrase string
}

func (q *PhraseQuery) queryNode() {}

func (q *PhraseQuery) String() string {
	if q.Field != "" {
		return fmt.Sprintf("phrase(%s:\"%s\")", q.Field, q.Phrase)
	}
	return fmt.Sprintf("phrase(\"%s\")", q.Phrase)
}

// PrefixQuery searches for terms starting with a prefix.
type PrefixQuery struct {
	Field  string
	Prefix string
}

func (q *PrefixQuery) queryNode() {}

func (q *PrefixQuery) String() string {
	if q.Field != "" {
		return fmt.Sprintf("prefix(%s:%s*)", q.Field, q.Prefix)
	}
	return fmt.Sprintf("prefix(%s*)", q.Prefix)
}

// BoolQuery combines multiple queries with boolean logic.
type BoolQuery struct {
	Must    []Query
	Should  []Query
	MustNot []Query
}

func (q *BoolQuery) queryNode() {}

func (q *BoolQuery) String() string {
	var parts []string

	if len(q.Must) > 0 {
		mustStrs := make([]string, len(q.Must))
		for i, m := range q.Must {
			mustStrs[i] = m.String()
		}
		parts = append(parts, fmt.Sprintf("AND(%s)", strings.Join(mustStrs, ", ")))
	}

	if len(q.Should) > 0 {
		shouldStrs := make([]string, len(q.Should))
		for i, s := range q.Should {
			shouldStrs[i] = s.String()
		}
		parts = append(parts, fmt.Sprintf("OR(%s)", strings.Join(shouldStrs, ", ")))
	}

	if len(q.MustNot) > 0 {
		notStrs := make([]string, len(q.MustNot))
		for i, n := range q.MustNot {
			notStrs[i] = n.String()
		}
		parts = append(parts, fmt.Sprintf("NOT(%s)", strings.Join(notStrs, ", ")))
	}

	if len(parts) == 0 {
		return "bool(empty)"
	}

	return fmt.Sprintf("bool(%s)", strings.Join(parts, " "))
}

// MatchAllQuery matches all documents.
type MatchAllQuery struct{}

func (q *MatchAllQuery) queryNode() {}

func (q *MatchAllQuery) String() string {
	return "match_all"
}

// MatchNoneQuery matches no documents.
type MatchNoneQuery struct{}

func (q *MatchNoneQuery) queryNode() {}

func (q *MatchNoneQuery) String() string {
	return "match_none"
}
