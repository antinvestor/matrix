package types

import (
	"github.com/antinvestor/matrix/roomserver/types"
)

// SearchResult obtains a result from doing a fulltext search on code
type SearchResult struct {
	Results []*SearchResultHit
	Total   int
}

func (sr *SearchResult) Highlights() []string {
	highlights := make([]string, len(sr.Results))
	for _, r := range sr.Results {
		highlights = append(highlights, r.Highlight)
	}
	return highlights
}

type SearchResultHit struct {
	Event          *types.HeaderedEvent
	Highlight      string
	StreamPosition StreamPosition
	Score          *float64
}
