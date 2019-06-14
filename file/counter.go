package file

import (
	"errors"
	"fmt"
	"log"
	"split_prefix/radix"
	rad "split_prefix/radix-counter"
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
	limitUp     int
	wg          *sync.WaitGroup
}

func NewCounter(path string, splitLimit, countLimit, limitUp, poolLimit int) *Counter {
	fileManager := NewFileManager(path)
	if countLimit > splitLimit/100 { // 需要保证 countLimit 足够小
		countLimit = splitLimit / 100
	}
	return &Counter{
		fileManager: fileManager,
		splitLimit:  splitLimit,
		countLimit:  countLimit,
		limitUp:     limitUp,
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

	// 对统计结果进行筛选
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

	// 统计线程
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		count := 0
		for {
			childResult := <-childResultPool
			mergeResult(childResult, resultTree)
			count++
			if count == len(c.fileManager.Files()) {
				log.Printf("total result number: %d", count)
				break
			}
		}
	}()

	c.wg.Add(len(c.fileManager.Files()))
	for _, file := range c.fileManager.Files() {
		c.counterPool <- true
		go c.getChildResult(file, totalSize, childResultPool)
	}
	c.wg.Wait()

	// 所有线程完成后统计
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

func (c *Counter) createTrie(fileName string) (trie_tree *radix.Tree, lines int) {
	log.Printf("start to create Trie for %v", fileName)
	trie_tree = radix.NewTrieTree(10)
	lines = readFile(fileName, c.fileManager, trie_tree)
	return
}

// walk 选出了满足 limit 条件的前缀后需要确认这些前缀的数量之和是不是与当前树的根目录 size 一致
// 如果不一致，那么就将 limit 放大，知道 size 一致为止
func (c *Counter) getResultWithRetry(total, limit int, fn func(limit int) (countResult, int)) (countResult, int) {
	result, size := fn(limit)
	for size != total { // 不相等说明无法细分到 limit 的大小，需要加大limit 重试
		if limit == c.limitUp { // 最大上线也无法满足结果，需要提升上线或者减小
			log.Fatalf("can not get complete result from current Uplimit(%d)", c.limitUp)
			break
		}
		log.Printf("Wrong result with limit (%d) error: total size (%d) != file line number (%d)", limit, size, total)
		limit *= 2
		if limit > c.limitUp {
			limit = c.limitUp
		}
		result, size = fn(limit)
	}
	return result, size
}

// 将各个文件的结果合并到统计结果中
func mergeResult(childResult countResult, t *rad.Tree) {
	for k, vFile := range childResult {
		for i := 0; i <= len(k); i++ {
			if vOld, ok := t.Get(k[:i]); ok {
				t.Insert(k[:i], vOld.(int)+vFile)
			} else {
				t.Insert(k[:i], vFile)
			}
		}
	}
}
