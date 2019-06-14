package radix

import (
	"fmt"
	"sort"
	"strconv"
)

// edge is used to represent an edge node
type edge struct {
	label byte
	node  *node
}

type node struct {
	size int

	// prefix is the common prefix we ignore
	prefix string

	// Edges should be stored in-order for iteration.
	// We avoid a fully materialized slice to save memory,
	// since in most cases we expect to be sparse
	edges edges
}

func (n *node) addEdge(e edge) {
	n.edges = append(n.edges, e)
	n.edges.Sort()
}

func (n *node) updateEdge(label byte, node *node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		n.edges[idx].node = node
		return
	}
	panic("replacing missing edge")
}

func (n *node) getAndCreateEdge(s string) (*node, bool) {
	var nChild *node
	n.size++
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= s[0]
	})
	if idx < num && n.edges[idx].label == s[0] {
		nChild = n.edges[idx].node
	} else {
		nChild = nil
	}
	create := false
	if nChild == nil {
		e := edge{
			label: s[0],
			node: &node{
				prefix: s,
				size:   1,
			},
		}
		n.edges = append(n.edges, e)
		n.edges.Sort()
		create = true
	}
	return nChild, create
}

func (n *node) getAndSplit(sOld string, parent *node) (sNew string, next, ret bool) {
	commonPrefix := longestPrefix(sOld, n.prefix)
	if commonPrefix == len(n.prefix) {
		sNew = sOld[commonPrefix:]
		next = true
		return
	}
	child := &node{
		prefix: sOld[:commonPrefix],
		size:   n.size + 1,
	}
	parent.updateEdge(sOld[0], child)
	child.addEdge(edge{
		label: n.prefix[commonPrefix],
		node:  n,
	})
	n.prefix = n.prefix[commonPrefix:]

	// If the new key is a subset, add to to this node
	sNew = sOld[commonPrefix:]
	if len(sNew) == 0 {
		ret = true
		return
	}

	// Create a new edge for the node
	child.addEdge(edge{
		label: sNew[0],
		node: &node{
			prefix: sNew,
			size:   1,
		},
	})
	ret = true
	return
}

func (e edges) Len() int {
	return len(e)
}

func (e edges) Less(i, j int) bool {
	return e[i].label < e[j].label
}

func (e edges) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e edges) Sort() {
	sort.Sort(e)
}

type edges []edge

// Tree implements a radix tree. This can be treated as a
// Dictionary abstract data type. The main advantage over
// a standard hash map is prefix-based lookups and
// ordered iteration,
type Tree struct {
	root   *node
	result map[string]int
}

// New returns an empty Tree
func NewTrieTree(maxThread int) *Tree {
	return NewFromArray(nil)
}

// NewFromMap returns a new tree containing the keys
// from an existing map
func NewFromArray(a []string) *Tree {
	t := &Tree{
		root:   &node{},
		result: make(map[string]int),
	}
	for _, v := range a {
		t.Insert(v)
	}
	return t
}

// Len is used to return the number of elements in the tree
func (t *Tree) Len() int {
	return t.root.size
}

func (t *Tree) Result() map[string]int {
	return t.result
}

func (t *Tree) Clear() {
	t.result = make(map[string]int)
}

// longestPrefix finds the length of the shared prefix
// of two strings
func longestPrefix(k1, k2 string) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

// Insert is used to add a newentry or update
// an existing entry. Returns if updated.
func (t *Tree) Insert(s string) {
	var parent *node
	n := t.root
	search := s
	for {
		// Handle key exhaution
		if len(search) == 0 {
			return
		}

		// Look for the edge
		parent = n
		var create bool
		n, create = n.getAndCreateEdge(search)
		if create {
			return
		}
		var (
			next bool
			ret  bool
		)
		search, next, ret = n.getAndSplit(search, parent)
		if next {
			continue
		}
		if ret {
			return
		}
		return
	}
}

// Walk is used to walk the tree
func (t *Tree) Walk(limit *int) {
	t.recursiveWalk(t.root, "", limit)
}

// recursiveWalk is used to do a pre-order walk of a node
// recursively. Returns true if the walk should be aborted
func (t *Tree) recursiveWalk(n *node, tmp string, limit *int) {
	tmp += n.prefix
	if n.size <= *limit {
		if tmp == "" {
			for _, e := range n.edges {
				t.result[e.node.prefix] = e.node.size
			}
		} else {
			t.result[tmp] = n.size
		}
		return
	}
	for _, n := range n.edges {
		t.recursiveWalk(n.node, tmp, limit)
	}
}

func (t *Tree) ResultToString() (result string) {
	total := 0
	for k, v := range t.result {
		tmp := strconv.Itoa(int(v))
		str := k + "=" + tmp + ","
		result += str
		total += v
	}
	return fmt.Sprintf("%s, total = %d, len_resultMap = %d", result[:len(result)-1], total, len(t.result))
}

func (t *Tree) ToMap() map[string]int {
	m := make(map[string]int)
	t.walk(t.root.prefix, t.root, m)
	return m
}

func (t *Tree) walk(prefix string, n *node, m map[string]int) {
	tmp := prefix + n.prefix
	m[tmp] = n.size
	for _, e := range n.edges {
		t.walk(tmp, e.node, m)
	}
}
