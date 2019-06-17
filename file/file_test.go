package file

import (
	"testing"
)

func TestSplitFile(t *testing.T) {
	manager := NewFileManager("/tmp/test/test/")
	val, err := manager.SplitFile(1000000)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(val)
	t.Logf("len files: %v", len(manager.files))
}
