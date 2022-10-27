package plugin

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/go-getter"
	filehelpers "github.com/turbot/go-kit/files"
)

func ResolveSourcePath(sourcePath, tmpDir string) (sourceDir string, globPattern string, err error) {
	if sourcePath == "" {
		return "", "", fmt.Errorf("source cannot be empty")
	}

	sourceDir, globPattern, err = filehelpers.GlobRoot(sourcePath)
	if err != nil {
		return "", "", err
	}

	// if we managed to resolve the sourceDir, treat this as a local path
	if sourceDir != "" {
		return sourceDir, globPattern, nil
	}

	// create temporary directory to store the go-getter data
	dest := createTempDirForGet(tmpDir)

	lastIndex := strings.LastIndex(sourcePath, "//")
	if lastIndex != -1 && sourcePath[lastIndex-1:lastIndex] != ":" {
		globPattern = sourcePath[lastIndex+2:]
		sourcePath = sourcePath[:lastIndex]
	}

	// if the source path is a S3 URL, and the path refers to a top-level file, for example:
	// s3::https://bucket.s3.amazonaws.com/foo
	// send the path directly to go-getter, and use the destination path as glob pattern for file searching
	// and also remove the query parameters (parts after ?) from destination path
	if sourcePath == globPattern {
		filename := strings.Split(sourcePath, "/")
		dest = path.Join(dest, strings.Split(filename[len(filename)-1], "?")[0])
		globPattern = dest
	}

	err = getter.Get(dest, sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to get directory specified by the source %s: %s", sourcePath, err.Error())
	}

	if globPattern != "" && dest != globPattern {
		globPattern = path.Join(dest, globPattern)
	} else {
		// update the destination to point the folder where the file stored
		splitDest := strings.Split(dest, "/")
		dest = strings.Join(splitDest[:len(splitDest)-1], "/")
	}

	return dest, globPattern, nil
}

// create a uniquely named sub-directory
func createTempDirForGet(tmpDir string) string {
	var dest string
	for {
		dest = path.Join(tmpDir, timestamp())
		_, err := os.Stat(dest)
		if err == nil {
			break
		}

		// return true if unique
		if os.IsNotExist(err) {
			break
		}
	}

	return dest
}

// get the current timestamp
func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
