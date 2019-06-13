package file

import (
	"testing"
)

func TestSplitFile(t *testing.T) {
	manager := NewFileManager("/tmp/test/test/")
	err := manager.SplitFile(1000000)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("len files: %v", len(manager.files))
}
