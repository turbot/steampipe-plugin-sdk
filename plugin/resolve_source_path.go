package plugin

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/hashicorp/go-getter"
	filehelpers "github.com/turbot/go-kit/files"
)

func ResolveSourcePath(source, tmpDir string) (sourceDir string, globPattern string, err error) {
	if source == "" {
		return "", "", fmt.Errorf("source cannot be empty")
	}

	sanitizedSource, _ := filehelpers.Tildefy(source)

	sourceDir, globPattern, err = filehelpers.PathToRootAndGlob(sanitizedSource)
	if err != nil {
		return "", "", err
	}

	if sourceDir != "" {
		return sourceDir, globPattern, nil
	}

	// create temporary directory to store the go-getter data
	dest := createTempDirForGet(tmpDir)
	err = getter.Get(dest, source)
	if err != nil {
		return "", "", fmt.Errorf("failed to get directory specified by the source %s: %s", source, err.Error())
	}

	return dest, globPattern, nil
}

func createTempDirForGet(tmpDir string) string {
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

	return dest
}

// Get the current timestamp
func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
