package plumtree

import (
	"maps"
	"slices"
)

func mapKeys[T comparable, Q any](m map[T]Q) []T {
	return slices.Collect(maps.Keys(m))
}
