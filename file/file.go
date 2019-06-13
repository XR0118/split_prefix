package file

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	rad "split_prefix/go-radix"
	"split_prefix/radix"
	"strconv"
	"strings"
	"sync"

	"github.com/hpcloud/tail"
)

type FileManager struct {
	path        string
	files       []string
	countResult map[string]interface{}
	// ch          chan int
	// tireThread int
	// wg sync.WaitGroup
}

type file struct {
	Name string `json:"key"`
}

func NewFileManager(path string) *FileManager {
	return &FileManager{
		path:  path,
		files: listDir(path),
		// countResult: make(map[string]interface{}),
		// ch:          make(chan int, 1),
		// tireThread: maxTireThread,
	}
}

func (fm *FileManager) Files() []string {
	return fm.files
}

// 优化：获取了需要 split 的前缀后并行 split
func (fm *FileManager) SplitFile(limit int) error {
	splitList, err := fm.getFileToSplit(limit)
	if err != nil {
		return err
	}
	bakPath, err := fm.mkdir()
	log.Printf("bak path: %s", bakPath)
	if err != nil {
		return err
	}
	ch := make(chan bool, 5)
	wg := new(sync.WaitGroup)
	wg.Add(len(splitList))
	for file, lines := range splitList {
		// if err := fm.splitFile(file, bakPath, lines, limit); err != nil {
		// 	return err
		// }
		ch <- true
		go fm.splitFile(file, bakPath, lines, limit, ch, wg)
	}
	wg.Wait()
	fm.files = listDir(fm.path)
	log.Printf("len files: %v", len(fm.files))
	return nil
}

func (fm *FileManager) getFileToSplit(limit int) (map[string]int, error) {
	splitList := make(map[string]int)
	for _, fileName := range fm.files {
		lines, err := countLine(fm.path + fileName)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("get split list err: %s", err))
		}
		if lines > limit {
			splitList[fileName] = lines
		}
	}
	log.Printf("file need to split: %v", splitList)
	return splitList, nil
}

func (fm *FileManager) mkdir() (string, error) {
	parentPath := fm.path[:len(fm.path)-1]
	parentPath = getParentDirectory(parentPath)
	newPath := parentPath + "/bak/"
	_, err := os.Stat(newPath)
	if os.IsNotExist(err) {
		err := os.Mkdir(newPath, os.ModePerm)
		if err != nil {
			log.Printf("mkdir failed![%v]\n", err)
			return "", err
		} else {
			log.Printf("mkdir success!\n")
		}
	}
	return newPath, nil
}

func (fm *FileManager) mvFile(oldPath, newPath string) error {
	err := os.Rename(oldPath, newPath)
	if err != nil {
		return errors.New(fmt.Sprintf("mv file err: %s", err))
	}
	return nil
}

func (fm *FileManager) splitFile(file, bakPath string, lines, limit int, ch chan bool, wg *sync.WaitGroup) {
	defer func() {
		<-ch
		wg.Done()
	}()
	absPath := fm.path + file
	c := fmt.Sprintf("./split.sh %s %d %s", fm.path, limit, file)
	log.Print(c)
	err := exec_shell(c)
	if err != nil {
		log.Fatalf("split err when split %s, err: %s", absPath, err)
		return
	}
	// log.Printf("start to split: %s", absPath)
	// f, err := os.Open(absPath)
	// if err != nil {
	// 	f.Close()
	// 	return err
	// }
	// scanner := bufio.NewScanner(f)
	// for i := 0; i < lines/limit+1; i++ {
	// 	if err := copyFile(i, absPath, scanner, limit, lines); err != nil {
	// 		return errors.New(fmt.Sprintf("copyFile err: %s", err))
	// 	}
	// }
	// f.Close()
	if err := fm.mvFile(absPath, bakPath+file); err != nil {
		log.Printf("mv err when mv %s, err: %s", absPath, err)
		return
	}
	return
}

func (fm *FileManager) CreateTrieResult(s string) (TrieTree, error) {
	return fm.createTrieResult(s)
}

func (fm *FileManager) createTrieResult(prefix string) (trie_tree TrieTree, err error) {
	log.Printf("start to create Trie for %v", prefix)
	containPrefix, err := findStartFile(prefix, fm.files, fm.path)
	log.Printf("file contain this prefix: %v, total %d files", containPrefix, len(containPrefix))
	if err != nil {
		return nil, err
	}
	trie_tree = radix.NewTrieTree(10)
	for _, fileName := range containPrefix {
		readFile(fileName, fm, trie_tree)
	}
	return trie_tree, nil
}

func (fm *FileManager) getCounterResult() (map[string]interface{}, error) {
	path := fm.path + "counter_result.txt"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	resultDataArray := strings.Split(string(data), " ")
	resultDataArray = resultDataArray[:len(resultDataArray)-1]
	for _, resultData := range resultDataArray {
		tmpArray := strings.Split(resultData, "=")
		if len(tmpArray) != 2 {
			err := errors.New("resultData length wrong")
			return nil, err
		}
		tmpInt, err := strconv.Atoi(tmpArray[1])
		if err != nil {
			return nil, err
		}
		fm.countResult[tmpArray[0]] = tmpInt
	}
	return fm.countResult, nil
}

func (fm *FileManager) getTree() *rad.Tree {
	m, err := fm.getCounterResult()
	if err != nil {
		log.Fatal("get result map fail")
	}
	tree := rad.NewFromMap(m)
	return tree
}

// 不应该找当前前缀多一位，而是应该找到第一个不存在的前缀
func (fm *FileManager) countSingleFile(prefix string) (map[string]int, error) {
	fileNames, err := getFileName(prefix, fm.files)
	if err != nil {
		return nil, err
	}
	log.Print("read file:", fileNames)
	result := make(map[string]int)
	for _, fileName := range fileNames {
		f, err := os.Open(fm.path + fileName)
		defer f.Close()
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(f)
		dealFn := fm.dealLine()
		for scanner.Scan() {
			if err := dealFn(scanner.Bytes(), result, &prefix); err != nil {
				log.Fatal("please confirm your file content is json style, deal with line fail:", err)
				return nil, err
			}
		}
		if err := scanner.Err(); err != nil {
			log.Fatal("scan file fail:", err)
		}
	}
	return result, nil
}

func (fm *FileManager) dealLine() func(line []byte, result map[string]int, prefix *string) error {
	tmpKey := ""
	tmpLen := -1
	fn := func(line []byte, result map[string]int, prefix *string) error {
		if tmpKey == "" && tmpLen == -1 { // 初始化
			tmpKey = *prefix
			tmpLen = len(tmpKey) + 1
		}
		f := new(file)
		if err := json.Unmarshal(line, f); err != nil {
			return err
		}

		var key string
		for i := tmpLen; i <= len(f.Name); i++ { // find key
			key = f.Name[0:i]
			if isTmpKey := tmpKey == key; isTmpKey { // 先看是不是和之前的前缀一致
				break
			} else { // 不一致的话更新 key，tmpKey 和 tmpLen
				tmpKey = key
				tmpLen = len(key)
			}
			_, isContain := fm.countResult[key] // 在已有的前缀中查询,如果已经有的话就不插入结果，没有的话就插入
			if !isContain {
				break
			}
		}
		updateMap(result, &key)
		return nil
	}
	return fn
}

func findStartFile(prefix string, files []string, fPath string) ([]string, error) {
	result := make([]string, 0)
	prefixes := make([]string, 0)
	for i := 0; i < strings.Count(prefix, ""); i++ {
		prefixes = append(prefixes, prefix[:i])
	}
	fileNames, err := getMultiFileName(prefixes, files)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("find start file error when getMultiFileName: %s", err))
	}
	for _, file := range fileNames {
		isContain, err := readFileTail(file, fPath, prefix)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("find start file error when readFileTail: %s", err))
		}
		if isContain {
			result = append(result, file)
		}
	}
	return result, nil
}

func readFileTail(path string, fPath string, prefix string) (bool, error) {
	t, err := tail.TailFile(fPath+path, tail.Config{Follow: true})
	if err != nil {
		return false, err
	}
	for line := range t.Lines {
		content := []byte(line.Text)
		f := new(file)
		if err := json.Unmarshal(content, f); err != nil {
			return false, err
		}
		if strings.HasPrefix(f.Name, prefix) {
			return true, nil
		}
		return false, nil
	}
	return false, nil
}

func getMultiFileName(prefixes []string, files []string) ([]string, error) {
	fileNames := make([]string, 0)
	for _, file := range prefixes {
		tmp, err := getFileName(file, files)
		if err != nil {
			return nil, err
		}
		fileNames = append(fileNames, tmp...)
	}
	return fileNames, nil
}

func getFileName(prefix string, files []string) ([]string, error) {
	prefix = changePrefix(prefix)
	fileNames := make([]string, 0)
	if len(prefix) == 0 {
		for _, file := range files {
			match, err := regexp.MatchString("qiniu_success_[0-9]*_.txt", file)
			if err != nil {
				return nil, err
			}
			if match && strings.HasSuffix(file, fmt.Sprintf("_%s.txt", prefix)) {
				fileNames = append(fileNames, file)
			}
		}
	} else {
		for _, file := range files {
			if strings.HasSuffix(file, fmt.Sprintf("_%s.txt", prefix)) {
				fileNames = append(fileNames, file)
			}
		}
	}
	if len(fileNames) == 0 {
		err := errors.New("Wrong prefix, no such file")
		return nil, err
	}
	return fileNames, nil
}

func listDir(path string) []string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	fileNames := make([]string, len(files))
	for idx, file := range files {
		fileNames[idx] = file.Name()
	}
	target := fileNames[:0]
	for _, item := range fileNames {
		if strings.HasPrefix(item, "qiniu") {
			target = append(target, item)
		}
	}
	return fileNames
}

func changePrefix(s string) string {
	return strings.Replace(s, "/", "~", -1)
}

func updateMap(m map[string]int, key *string) {
	if _, ok := m[*key]; ok {
		m[*key]++
	} else {
		m[*key] = 1
	}
}

func readFile(fileName string, fm *FileManager, trie_tree TrieTree) int {
	lines := 0
	f, err := os.Open(fm.path + fileName)
	defer f.Close()
	if err != nil {
		log.Fatalf("open file %s fail", fileName)
		return -1
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines++
		if err := deal(scanner.Bytes(), trie_tree); err != nil {
			log.Fatal("please confirm your file content is json style, deal with line fail:", err)
			return -1
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal("scan file fail:", err)
	}
	return lines
}

func deal(line []byte, trie TrieTree) error {
	f := new(file)
	if err := json.Unmarshal(line, f); err != nil {
		return err
	}
	trie.Insert(f.Name)
	return nil
}

func countLine(file string) (int, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return -1, err
	}
	lines, err := lineCounter(f)
	return lines, nil
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func exec_shell(s string) error {
	//函数返回一个*Cmd，用于使用给出的参数执行name指定的程序
	cmd := exec.Command("/bin/bash", "-c", s)
	//Run执行c包含的命令，并阻塞直到完成。  这里stdout被取出，cmd.Wait()无法正确获取stdin,stdout,stderr，则阻塞在那了
	err := cmd.Run()
	return err
}

func copyFile(idx int, file string, scanner *bufio.Scanner, limit, lines int) error {
	count := 0
	newFileName := fmt.Sprintf("%s_%d", file, idx)
	f, err := os.OpenFile(newFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	defer f.Close()
	if err != nil {
		return err
	}
	// m := new(sync.Mutex)
	// pool := make(chan []byte, 10)
	// wg := new(sync.WaitGroup)
	// if lines-limit*(idx+1) > 0 {
	// 	wg.Add(limit)
	// } else {
	// 	wg.Add(lines - limit*idx)
	// }
	for scanner.Scan() {
		count++
		context := append(scanner.Bytes(), 10)
		// pool <- context
		// go writeToFile(pool, f, m, wg)
		_, err := f.Write(context)
		if err != nil {
			return err
		}
		if count == limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// wg.Wait()
	return nil
}

func writeToFile(pool chan []byte, f *os.File, m *sync.Mutex, wg *sync.WaitGroup) {
	defer func() {
		m.Unlock()
		wg.Done()
	}()
	m.Lock()
	f.Write(<-pool)
}

func substr(s string, pos, length int) string {
	runes := []rune(s)
	l := pos + length
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l])
}

func getParentDirectory(dirctory string) string {
	return substr(dirctory, 0, strings.LastIndex(dirctory, "/"))
}
