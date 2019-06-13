package file

import (
	"testing"
)

// func TestGetCounterResult(t *testing.T) {
// 	manager := NewFileManager("/tmp/test/test/")
// 	result, err := manager.getCounterResult()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	t.Logf("%v, %d", len(result), result["assets/m"])
// }

// func TestCounterSingleFile(t *testing.T) {
// 	manager := NewFileManager("/tmp/test/test/")
// 	_, _ = manager.getCounterResult()
// 	prefix := "assets/"
// 	result, err := manager.countSingleFile(prefix)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	total := 0
// 	for _, v := range result {
// 		total += v
// 	}
// 	t.Logf("%v", result)
// 	t.Logf("%v", total)
// }

// func TestGetTree(t *testing.T) {
// 	manager := NewFileManager("/tmp/test/test/")
// 	tree := manager.getTree()
// 	belowLImit := make(map[string]int)
// 	overlimit := make(map[string]int)
// 	fn2 := manager.countSingleFile
// 	tree.ReCreate(fn2)
// 	limit := 10000000
// 	// fn := func(s string, v interface{}) bool {
// 	// 	if v.(int) <= limit {
// 	// 		belowLImit[s] = v.(int)
// 	// 		return true
// 	// 	}
// 	// 	return false
// 	// }
// 	// tree.Pick(fn)
// 	// total_v := 0
// 	// for k, v := range belowLImit {
// 	// 	total_v += v
// 	// 	t.Logf("%s = %d \n", k, v)
// 	// }
// 	// t.Log("total file:", total_v)
// 	allChild := make(map[string]int)
// 	fn3 := func(s string, v interface{}) bool {
// 		allChild[s] = v.(int)
// 		if v.(int) > limit {
// 			overlimit[s] = v.(int)
// 		} else {
// 			belowLImit[s] = v.(int)
// 		}
// 		return false
// 	}
// 	tree.GetAllChild(fn3)
// 	total := 0
// 	f, _ := os.Create("data.out")
// 	defer f.Close()
// 	for k, v := range allChild {
// 		total += v
// 		msg := fmt.Sprintf("%s = %d \n", k, v)
// 		_, _ = f.WriteString(msg)
// 	}
// 	f.Sync()
// 	total_below := 0
// 	for k, v := range belowLImit {
// 		total_below += v
// 		t.Logf("%s = %d \n", k, v)
// 	}
// 	total_over := 0
// 	for k, v := range overlimit {
// 		total_over += v
// 		t.Logf("%s = %d \n", k, v)
// 	}
// 	t.Logf("total: %d, below: %d, over: %d", total, total_below, total_over)
// }

func TestCreateTrie(t *testing.T) {
	manager := NewFileManager("/tmp/test/test/")
	trie, err := manager.createTrieResult("assets/p")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(trie.Len())
	limit := 10000
	trie.Walk(&limit)
	result := trie.Result()
	total_v := 0
	for k, v := range result {
		total_v += v
		t.Logf("%s = %d \n", k, v)
	}
	t.Log("total file:", total_v)
	// result := trie.ResultToString()
	// t.Logf("result list: %v", result)
}

func TestSplitFile(t *testing.T) {
	manager := NewFileManager("/tmp/test/test/")
	err := manager.SplitFile(1000000)
	if err != nil {
		t.Fatal(err)
	}
	// manager.splitFile("/Users/qnxr/go/src/trie_tool/file/data.out", 2212, 1000)
	t.Logf("len files: %v", len(manager.files))
	// result := trie.ResultToString()
	// t.Logf("result list: %v", result)
}
