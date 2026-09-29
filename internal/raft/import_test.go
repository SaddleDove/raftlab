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
		"raftlab/internal/storage",
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
			importPath := strings.Trim(im.Path.Value, `"`)
			for _, bad := range forbidden {
				if importPath == bad || strings.HasPrefix(importPath, bad+"/") {
					t.Errorf("%s imports forbidden package %s", path, importPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
