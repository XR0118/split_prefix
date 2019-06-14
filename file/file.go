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
	"split_prefix/radix"
	"strings"
	"sync"
)

type FileManager struct {
	path        string
	files       []string
	countResult map[string]interface{}
}

type file struct {
	Name string `json:"key"`
}

func NewFileManager(path string) *FileManager {
	return &FileManager{
		path:  path,
		files: listDir(path),
	}
}

func (fm *FileManager) Files() []string {
	return fm.files
}

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

	// go 实现的 slplit 没有 shell split 快
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

func readFile(fileName string, fm *FileManager, trie_tree *radix.Tree) int {
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

func deal(line []byte, trie *radix.Tree) error {
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
	cmd := exec.Command("/bin/bash", "-c", s)
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
	for scanner.Scan() {
		count++
		context := append(scanner.Bytes(), 10)
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
	return nil
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
