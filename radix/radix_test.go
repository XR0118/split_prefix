package radix

import (
	"testing"
)

func TestInsert(t *testing.T) {
	radix := NewTrieTree(10)
	testString := []string{"abc", "ab", "abcd", "a", "acc", "ac", "accd", "bbc", "bb", "bbcd", "b"}
	for _, v := range testString {
		radix.Insert(v)
	}
	t.Log(radix.Len())
	limit := 2
	radix.Walk(&limit)
	t.Log(radix.Result())
	radix.Clear()
	limit = 3
	radix.Walk(&limit)
	t.Log(radix.Result())
}
