package file

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestResult10000W(t *testing.T) {
	counter := NewCounter("/tmp/test_10000W/", 1000000, 1000, 30000000, 2)
	result, err := counter.Result(20000000)
	if err != nil {
		t.Fatal(err)
	}
	total := result.total()
	f, _ := os.Create("data10000W.out")
	defer f.Close()
	for k, v := range result {
		msg := fmt.Sprintf("%s = %d \n", k, v)
		_, _ = f.WriteString(msg)
	}
	f.Sync()
	log.Print("total file:", total)
}

func TestResultLarge(t *testing.T) {
	counter := NewCounter("/tmp/test/test/", 1000000, 1000, 30000000, 10)
	result, err := counter.Result(20000000)
	if err != nil {
		t.Fatal(err)
	}
	total := result.total()
	f, _ := os.Create("data300W.out")
	defer f.Close()
	for k, v := range result {
		msg := fmt.Sprintf("%s = %d \n", k, v)
		_, _ = f.WriteString(msg)
	}
	f.Sync()
	log.Print("total file:", total)
}

func TestResultSmall(t *testing.T) {
	counter := NewCounter("/Users/qnxr/test/test/", 5000, 1000, 30000000, 3)
	result, err := counter.Result(10000)
	if err != nil {
		t.Fatal(err)
	}
	total := result.total()
	f, _ := os.Create("data.out")
	defer f.Close()
	for k, v := range result {
		msg := fmt.Sprintf("%s = %d \n", k, v)
		_, _ = f.WriteString(msg)
	}
	f.Sync()
	log.Print("total file:", total)
}
