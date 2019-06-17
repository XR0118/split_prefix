package file

import (
	"errors"
	"fmt"
	"log"
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
	limitUp     int
	poolLimit   int
	wg          *sync.WaitGroup
}

func NewCounter(path string, splitLimit, countLimit, limitUp, poolLimit int) *Counter {
	fileManager := NewFileManager(path)
	return &Counter{
		fileManager: fileManager,
		splitLimit:  splitLimit,
		countLimit:  countLimit,
		limitUp:     limitUp,
		poolLimit:   poolLimit,
		counterPool: make(chan bool, poolLimit),
		wg:          new(sync.WaitGroup),
	}
}

// limit 控制前缀分割的细粒度，limit 越大前缀越粗
func (c *Counter) Result(limit int) (countResult, error) {
	tree, err := c.count()
	if err != nil {
		return nil, err
	}
	val := tree.Len()

	// 对统计结果进行筛选
	retryFn := getRetryFn(tree)
	result, _ := c.getResultWithRetry(val, limit, retryFn)
	return result, nil
}

func (c *Counter) count() (*radix.Tree, error) {
	totalSize := new(int64)           // 总共文件数
	resultTree := radix.NewTrieTree() // 统计用的前缀树
	lineNum, err := c.fileManager.SplitFile(c.splitLimit)
	if err != nil {
		return nil, err
	}
	if lineNum < c.splitLimit {
		c.countLimit = lineNum / 10
	}

	// 与读文件线程数相同，避免过多的 map 结果占用内存
	childResultPool := make(chan countResult, c.poolLimit)

	// 统计线程
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		count := 0
		for {
			childResult := <-childResultPool
			// 单线程统计，插入速度过慢可能会是瓶颈，需要将树改为支持并发的结构,支持多线程插入
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
	val := resultTree.Len()
	if val < 1 {
		return nil, errors.New("result tree root is empty")
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
	retryFn := getRetryFn(tree)
	result, size := c.getResultWithRetry(lines, c.countLimit, retryFn)
	atomic.AddInt64(totalSize, int64(size))
	childResultPool <- result
}

func (c *Counter) createTrie(fileName string) (trie_tree *radix.Tree, lines int) {
	log.Printf("start to create Trie for %v", fileName)
	trie_tree = radix.NewTrieTree()
	lines = readFile(fileName, c.fileManager, trie_tree)
	return
}

// walk 选出了满足 limit 条件的前缀后需要确认这些前缀的数量之和是不是与当前树的根目录 size 一致
// 如果不一致，那么就将 limit 放大，直到 size 一致为止
func (c *Counter) getResultWithRetry(total, limit int, fn func(limit int) (countResult, int)) (countResult, int) {
	result, size := fn(limit)
	for size != total { // 不相等说明无法细分到 limit 的大小，需要加大limit 重试
		log.Printf("Wrong result with limit (%d) error: total size (%d) != file line number (%d)", limit, size, total)
		if limit == c.limitUp {
			log.Fatalf("can not get complete result from current Uplimit(%d)", c.limitUp)
			break
		}
		limit *= 2
		if limit > c.limitUp {
			limit = c.limitUp
		}
		result, size = fn(limit)
	}
	return result, size
}

// 将各个文件的结果合并到统计结果中
func mergeResult(childResult countResult, t *radix.Tree) {
	for k, vFile := range childResult {
		t.Insert(k, vFile)
	}
}

func getRetryFn(tree *radix.Tree) func(limit int) (countResult, int) {
	return func(limit int) (countResult, int) {
		belowLImit := newCounterResult()
		fn := getWalkFn(limit, belowLImit)
		tree.Pick(fn)
		return belowLImit, belowLImit.total()
	}
}

func getWalkFn(limit int, result countResult) radix.WalkFn {
	return func(s string, v int) bool {
		if v <= limit && len(s) > 0 {
			result[s] = v
			return true
		}
		return false
	}
}
