package plugin

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-getter"
	filehelpers "github.com/turbot/go-kit/files"
)

func ResolveSourcePath(source, tmpDir string) (sourceDir string, globPattern string, err error) {
	if source == "" {
		return "", "", fmt.Errorf("source cannot be empty")
	}

	lastIndex := strings.LastIndex(source, "//")

	if lastIndex != -1 && source[lastIndex-1:lastIndex] != ":" {
		globPattern = source[lastIndex+2:]
		source = source[:lastIndex]
	}

	// TODO :: Use separate function ResolveLocalPath

	// resolve ~ and get the absolute filepath
	sanitizedSource, err := filehelpers.Tildefy(source)
	if err == nil {
		// check whether the source is a file
		if filehelpers.FileExists(sanitizedSource) {
			// if so, there should be no glob
			if globPattern != "" {
				return "", "", fmt.Errorf("glob is not expected if the source is a filename")
			}
			dir := filepath.Dir(sanitizedSource)

			// use the filename as glob
			return dir, sanitizedSource, nil
		}

		// check whether the source is a directory
		if filehelpers.DirectoryExists(sanitizedSource) {
			return sanitizedSource, globPattern, nil
		}
	}
	//

	var dest string
	for {
		dest = path.Join(tmpDir, timestamp())
		_, err := os.Stat(dest)
		if err == nil {
			break
		}

		// Return true if not a duplicate directory
		if os.IsNotExist(err) {
			break
		}
	}

	err = getter.Get(dest, source)
	if err != nil {
		return "", "", fmt.Errorf("failed to get directory specified by the source %s: %s", source, err.Error())
	}

	return dest, globPattern, nil
}

// Get the current timestamp
func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
