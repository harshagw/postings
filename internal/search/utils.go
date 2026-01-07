package search

import "slices"

// sortByScore sorts the results by score in descending order.
func sortByScore(results []Result) {
	slices.SortFunc(results, func(a, b Result) int {
		if a.Score > b.Score {
			return -1
		}
		if a.Score < b.Score {
			return 1
		}
		return 0
	})
}
