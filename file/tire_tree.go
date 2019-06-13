package file

type TrieTree interface {
	Insert(word string)
	Walk(limit *int)
	Result() map[string]int
	Len() int
	ResultToString() string
	ToMap() map[string]int
	Clear()
}
