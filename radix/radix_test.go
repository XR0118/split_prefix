package radix

import (
	"testing"
)

func TestInsert(t *testing.T) {
	radix := NewTrieTree()
	testMap := make(map[string]int)
	testMap[""] = 10
	testMap["abcd"] = 1000
	testMap["abc"] = 100
	testMap["ab"] = 10
	testMap["a"] = 1
	testMap["acc"] = 100
	testMap["ac"] = 10
	testMap["accd"] = 1000
	testMap["bb"] = 10
	testMap["b"] = 1
	testMap["bbcd"] = 1000

	for k, v := range testMap {
		radix.Insert(k, v)
	}
	t.Log(radix.Len())
	limit := 2000
	belowLImit := make(map[string]int)
	fn := func(s string, v int) bool {
		if v <= limit && len(s) > 0 {
			belowLImit[s] = v
			return true
		}
		return false
	}
	radix.Pick(fn)
	t.Log(radix.ToMap())
	t.Log(belowLImit)
}
