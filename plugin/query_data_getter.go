package plugin

import (
	"log"
	"path"
	"strings"

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

	// if resolvedSourcePath and glob is same, it indicates that no glob patterns are defined in source. For example:
	//
	// if the path referred a file, for example: s3::https://bucket.s3.amazonaws.com/foo.ext
	// then return resolvedSourcePath directly
	//
	// if a folder, for example: s3::https://bucket.s3.amazonaws.com/foo
	// then append '*' with the glob explicitly, to match all files in that folder.
	if resolvedSourcePath == glob {
		if filehelpers.FileExists(resolvedSourcePath) {
			return []string{resolvedSourcePath}, nil
		}
		glob = path.Join(glob, "*")
	}

	// by default, all top-level files in dest should be returned
	opts := &filehelpers.ListOptions{
		Flags:   filehelpers.AllFlat,
		Include: []string{glob},
	}

	// if glob contains '**', then it should match all files recursively
	if strings.Contains(glob, "**/*") {
		opts.Flags = filehelpers.AllRecursive
	}

	return filehelpers.ListFiles(resolvedSourcePath, opts)
}

// Get the current timestamp
func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
