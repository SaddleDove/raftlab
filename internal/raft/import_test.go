package raft

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoForbiddenImports(t *testing.T) {
	forbidden := []string{
		"time",
		"net",
		"raftlab/internal/sim",
		"raftlab/internal/check",
		"raftlab/internal/unsafe",
		"raftlab/internal/transport",
	}
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, im := range f.Imports {
			path := strings.Trim(im.Path.Value, `"`)
			for _, bad := range forbidden {
				if path == bad || strings.HasPrefix(path, bad+"/") {
					t.Errorf("%s imports forbidden package %s", path, bad)
				}
			}
			if strings.Contains(path, "time") && path == "time" {
				t.Errorf("%s imports time", f.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
