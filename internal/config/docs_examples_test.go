package config

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const docsExampleKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func TestDocumentationExamplesLoadStrictly(t *testing.T) {
	examplesRoot := filepath.Join("..", "..", "docs", "examples")
	err := filepath.WalkDir(examplesRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".yaml" {
			return nil
		}
		t.Run(filepath.ToSlash(path), func(t *testing.T) {
			loadDocumentationExample(t, path)
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk documentation examples: %v", err)
	}
}

func loadDocumentationExample(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	data = []byte(strings.ReplaceAll(string(data),
		`key_file: "./olcrtc.key"`, `key: "`+docsExampleKey+`"`))
	tempPath := filepath.Join(t.TempDir(), filepath.Base(path))
	// #nosec G703 -- filepath.Base confines the generated file to t.TempDir.
	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		t.Fatalf("write temporary example: %v", err)
	}
	if _, err := Load(tempPath); err != nil {
		t.Fatalf("Load(%s): %v", path, err)
	}
}
