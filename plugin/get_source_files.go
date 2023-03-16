package plugin

import (
	"log"
	"path"

	filehelpers "github.com/turbot/go-kit/files"
	"github.com/turbot/steampipe-plugin-sdk/v5/getter"
)

// getSourceFiles accept a source path downloads files if necessary, and returns a list of local file paths
func getSourceFiles(source, tempDir string) ([]string, error) {
	// get the files into a temporary location
	resolvedSourcePath, glob, err := getter.GetFiles(source, tempDir)
	if err != nil {
		return nil, err
	}

	if resolvedSourcePath == "" && glob == "" {
		log.Printf("[TRACE] getSourceFiles: no files found")
		return nil, nil
	}
	log.Printf("[TRACE] getSourceFiles source: %s, glob: %s", resolvedSourcePath, glob)

	// if resolvedSourcePath and glob is same, it indicates that no glob patterns are defined in source
	// determine whether the target is a file or folder
	if resolvedSourcePath == glob {
		// if the path referred a file, for example: s3::https://bucket.s3.amazonaws.com/foo.ext
		// then return resolvedSourcePath directly
		if filehelpers.FileExists(resolvedSourcePath) {
			return []string{resolvedSourcePath}, nil
		}
		// must be a folder, for example: s3::https://bucket.s3.amazonaws.com/foo
		//  append '*' to the glob explicitly, to match all files in that folder.
		glob = path.Join(glob, "*")
	}

	opts := &filehelpers.ListOptions{
		Flags:   filehelpers.AllRecursive,
		Include: []string{glob},
	}

	return filehelpers.ListFiles(resolvedSourcePath, opts)
}
