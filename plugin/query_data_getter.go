package plugin

import (
	"log"

	filehelpers "github.com/turbot/go-kit/files"
)

// GetSourceFiles accept an array of source path strings, resolves these into a list of local file paths/globs,
// and returns all the files paths
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
