package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"split_prefix/file"
	"strings"
)

func main() {
	limit := flag.Int("l", 10000000, "Up-limit for File with the Same Prefix")
	fthread := flag.Int("ft", 10, "Max Read File Thread")
	tThread := flag.Int("tt", 100, "Max Trie Search Thread")
	path := flag.String("fp", "", "File Path")
	savePath := flag.String("sp", "", "Result Save Path")
	flag.Parse()

	if *path == "" || *savePath == "" {
		log.Fatal("bucket list path and result save path should be both provided")
		return
	}

	fp, err := os.Stat(*path)
	if os.IsNotExist(err) {
		log.Fatal("File Path not exist!")
		return
	}
	if !fp.IsDir() {
		log.Fatal("File Path is not directory")
		return
	}
	if strings.HasSuffix(*savePath, "/") {
		log.Fatal("Save Path should be file")
		return
	}

	saveDir := (*savePath)[:strings.LastIndex(*savePath, "/")+1]
	if _, err := os.Stat(saveDir); os.IsNotExist(err) {
		log.Fatal("Save Directory is not exist!")
		return
	}

	fm := file.NewFileManager(*path, *fthread, *tThread)
	trie, err := fm.CreateTrie()
	if err != nil {
		log.Fatal("create tire failed, reason:", err)
		return
	}
	// trie.Pick(uint(*limit), wg)
	trie.Walk(limit)

	f, err := os.Create(*savePath)
	if err != nil {
		log.Fatal("create save file fail, reason:", err)
		return
	}
	defer f.Close()

	result_string := trie.ResultToString()
	fmt.Print(result_string)

	if _, err := f.WriteString(fmt.Sprintf("%s \nresult length = %d", result_string, len(result_string))); err != nil {
		log.Fatal("write result failed, reason:", err)
	}
	if err := f.Sync(); err != nil {
		log.Fatal("file sync fail:", err)
	}

}
