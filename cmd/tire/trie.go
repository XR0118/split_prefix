package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"split_prefix/file"
	"strings"
	"time"
)

func main() {
	t1 := time.Now()
	limit := flag.Int("l", 20000000, "desired limit for file with the same prefix")
	splitLimit := flag.Int("sl", 1000000, "split large file to the specified number")
	path := flag.String("fp", "", "directory for files need to split prefix")
	Uplimit := flag.Int("upl", 40000000, "up-limit for file with the same prefix")
	savePath := flag.String("sp", "", "result save path")
	flag.Parse()

	if *path == "" || *savePath == "" {
		log.Fatal("bucket list path and result save path should be both provided")
		return
	}
	if strings.HasSuffix(*savePath, "/") {
		log.Fatal("Save Path should be file")
		return
	}
	if !strings.HasSuffix(*path, "/") {
		log.Fatal("File Path should be directory end with /")
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

	saveDir := (*savePath)[:strings.LastIndex(*savePath, "/")+1]
	if _, err := os.Stat(saveDir); os.IsNotExist(err) {
		log.Fatal("Save Directory is not exist!")
		return
	}

	if *splitLimit > 10000000 {
		*splitLimit = 1000000
		log.Print("split limit is too large, change it to 100W")
	}

	// 保证每次最多读 1000W 文件，内存不会超标
	poolLimit := 10000000 / *splitLimit
	if poolLimit < 1 {
		poolLimit = 1
	}
	counter := file.NewCounter(*path, *splitLimit, *splitLimit/100, *Uplimit, poolLimit)

	result, err := counter.Result(*limit)
	if err != nil {
		log.Fatal("err when get result:", err)
		return
	}

	f, err := os.Create(*savePath)
	if err != nil {
		log.Fatal("create save file fail, reason:", err)
		return
	}
	defer f.Close()

	for k, v := range result {
		if _, err := f.WriteString(fmt.Sprintf("%s = %d \n", k, v)); err != nil {
			log.Fatal("write result failed, reason:", err)
		}
	}

	if err := f.Sync(); err != nil {
		log.Fatal("file sync fail:", err)
	}

	elapsed := time.Since(t1)
	log.Println("App elapsed: ", elapsed.Seconds())
}
