package config

import (
	"fmt"
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

func TestDocumentationYAMLBlocksLoadStrictly(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "docs", "*.md"))
	if err != nil {
		t.Fatalf("glob documentation: %v", err)
	}
	for _, path := range paths {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read %s: %v", path, readErr)
		}
		for index, block := range documentationYAMLBlocks(string(data)) {
			name := fmt.Sprintf("%s/yaml-%d", filepath.Base(path), index+1)
			t.Run(name, func(t *testing.T) {
				loadDocumentationYAML(t, filepath.Base(path), []byte(block))
			})
		}
	}
}

func loadDocumentationExample(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	loadDocumentationYAML(t, filepath.Base(path), data)
}

func loadDocumentationYAML(t *testing.T, name string, data []byte) {
	t.Helper()
	content := strings.ReplaceAll(string(data),
		`key_file: "./olcrtc.key"`, `key: "`+docsExampleKey+`"`)
	content = strings.ReplaceAll(content,
		`key_file: ./olcrtc.key`, `key: "`+docsExampleKey+`"`)
	tempPath := filepath.Join(t.TempDir(), name+".yaml")
	// #nosec G703 -- filepath.Base confines the generated file to t.TempDir.
	if err := os.WriteFile(tempPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write temporary example: %v", err)
	}
	if _, err := Load(tempPath); err != nil {
		t.Fatalf("Load(%s): %v", name, err)
	}
}

func documentationYAMLBlocks(source string) []string {
	var blocks []string
	var current []string
	inYAML := false
	for _, line := range strings.Split(source, "\n") {
		switch strings.TrimSpace(line) {
		case "```yaml":
			inYAML = true
			current = current[:0]
		case "```":
			if inYAML {
				blocks = append(blocks, strings.Join(current, "\n"))
				inYAML = false
			}
		default:
			if inYAML {
				current = append(current, line)
			}
		}
	}
	return blocks
}
