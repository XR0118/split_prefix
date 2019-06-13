package file

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestResult(t *testing.T) {
	counter := NewCounter("/tmp/test/test/", 1000000, 100000, 10)
	result, err := counter.Result(300000)
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
