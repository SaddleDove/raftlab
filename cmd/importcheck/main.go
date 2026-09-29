package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	raftDir := filepath.Join(root, "internal", "raft")
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
	var bad []string
	err := filepath.WalkDir(raftDir, func(path string, d fs.DirEntry, err error) error {
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
			p := strings.Trim(im.Path.Value, `"`)
			for _, fb := range forbidden {
				if p == fb {
					bad = append(bad, fmt.Sprintf("%s imports %s", path, p))
				}
			}
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(src), "time.Now") || strings.Contains(string(src), "time.Sleep") {
			bad = append(bad, fmt.Sprintf("%s uses time.Now or time.Sleep", path))
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(bad) > 0 {
		for _, b := range bad {
			fmt.Fprintln(os.Stderr, b)
		}
		os.Exit(1)
	}
	fmt.Println("import graph ok: internal/raft depends only on injected interfaces")
}
