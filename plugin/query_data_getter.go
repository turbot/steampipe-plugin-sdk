package plugin

import (
	"log"

	filehelpers "github.com/turbot/go-kit/files"
)

func (q *QueryData) GetSourceFiles(source string) ([]string, error) {
	resolvedSourcePath, glob, err := ResolveSourcePath(source, q.tempDir)
	if err != nil {
		return nil, err
	}
	log.Printf("[WARN] Source: %s, Glob: %s", resolvedSourcePath, glob)

	opts := &filehelpers.ListOptions{
		Flags:   filehelpers.AllRecursive,
		Include: []string{glob},
	}
	return filehelpers.ListFiles(resolvedSourcePath, opts)
}

// Get the current timestamp
func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
