package file

import (
	"errors"
	"fmt"
	"log"
	rad "split_prefix/go-radix"
	"split_prefix/radix"
	"sync"
	"sync/atomic"
)

type countResult map[string]int

func newCounterResult() countResult {
	return make(map[string]int)
}

func newCounterResultFromMap(m map[string]int) countResult {
	return m
}

func (cr countResult) total() int {
	total := 0
	for _, v := range cr {
		total += v
	}
	return total
}

type Counter struct {
	fileManager *FileManager
	splitLimit  int
	countLimit  int
	counterPool chan bool
	wg          *sync.WaitGroup
}

func NewCounter(path string, splitLimit, countLimit, poolLimit int) *Counter {
	fileManager := NewFileManager(path)
	if splitLimit > 200000 {
		countLimit = 20000
	}
	if countLimit > splitLimit/5 {
		countLimit = splitLimit / 5
	}
	return &Counter{
		fileManager: fileManager,
		splitLimit:  splitLimit,
		countLimit:  countLimit,
		counterPool: make(chan bool, poolLimit),
		wg:          new(sync.WaitGroup),
	}
}

func (c *Counter) Result(limit int) (countResult, error) {
	tree, err := c.count()
	if err != nil {
		return nil, err
	}
	val, _ := tree.Get("")

	retryFn := func(limit int) (countResult, int) {
		belowLImit := newCounterResult()
		fn := func(s string, v interface{}) bool {
			if v.(int) <= limit {
				belowLImit[s] = v.(int)
				return true
			}
			return false
		}
		tree.Pick(fn)
		return belowLImit, belowLImit.total()
	}
	result, _ := c.getResultWithRetry(val.(int), limit, retryFn)
	return result, nil
}

func (c *Counter) count() (*rad.Tree, error) {
	totalSize := new(int64) // 总共文件数
	resultTree := rad.New() // 统计用的前缀树
	if err := c.fileManager.SplitFile(c.splitLimit); err != nil {
		return nil, err
	}
	childResultPool := make(chan countResult, 100)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		count := 0
		for {
			tmp := <-childResultPool
			mergeResult(tmp, resultTree)
			count++
			if count == len(c.fileManager.Files()) {
				log.Printf("total result number: %d", count)
				break
			}
		}
	}()

	// walk 选出了满足 limit 条件的前缀后需要确认这些前缀的数量之和是不是与当前树的根目录 size 一致
	// 如果不一致，那么就将 limit 放大，知道 size 一致为止
	c.wg.Add(len(c.fileManager.Files()))
	for _, file := range c.fileManager.Files() {
		c.counterPool <- true
		go c.getChildResult(file, totalSize, childResultPool)
	}
	c.wg.Wait()
	t := int(*totalSize)
	log.Print(t)
	val, ok := resultTree.Get("")
	if !ok {
		return nil, errors.New("no root path")
	}
	if t != val {
		return nil, errors.New(fmt.Sprintf("total count result is wrong: totalSize = %d, tree size = %d", t, val))
	}
	return resultTree, nil
}

func (c *Counter) getChildResult(s string, totalSize *int64, childResultPool chan countResult) {
	defer func() {
		c.wg.Done()
		<-c.counterPool
	}()
	tree, lines := c.createTrie(s)
	if lines == -1 {
		log.Fatalf("create tree error when read file")
		return
	}
	fn := func(limit int) (countResult, int) {
		tree.Clear()
		tree.Walk(&limit)
		result := newCounterResultFromMap(tree.Result())
		size := result.total()
		return result, size
	}
	result, size := c.getResultWithRetry(lines, c.countLimit, fn)
	atomic.AddInt64(totalSize, int64(size))
	childResultPool <- result
}

func (c *Counter) createTrie(fileName string) (trie_tree TrieTree, lines int) {
	log.Printf("start to create Trie for %v", fileName)
	trie_tree = radix.NewTrieTree(10)
	lines = readFile(fileName, c.fileManager, trie_tree)
	return
}

func (c *Counter) getResultWithRetry(total, limit int, fn func(limit int) (countResult, int)) (countResult, int) {
	result, size := fn(limit)
	times := 1
	for size != total { // 不相等说明无法细分到 limit 的大小，需要加大limit 重试
		log.Printf("Wrong result with limit (%d) error: total size (%d) != file line number (%d)", limit*times, size, total)
		times++
		result, size = fn(limit * times)
	}
	return result, size
}

// 还能优化，中间有许多不必要的操作
func mergeResult(a countResult, t *rad.Tree) {
	for k, vFile := range a {
		for i := 0; i <= len(k); i++ {
			if vOld, ok := t.Get(k[:i]); ok {
				t.Insert(k[:i], vOld.(int)+vFile)
			} else {
				t.Insert(k[:i], vFile)
			}
		}
	}
}
