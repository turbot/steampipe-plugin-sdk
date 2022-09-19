package plugin

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/hashicorp/go-getter"
)

func (q *QueryData) GetSourceFiles(source string) (string, error) {
	if source == "" {
		return "", fmt.Errorf("source cannot be empty")
	}

	var dest string
	for {
		dest = path.Join(q.tempDir, timestamp())
		_, err := os.Stat(dest)
		if err == nil {
			break
		}

		// Return true if not a duplicate directory
		if os.IsNotExist(err) {
			break
		}
	}

	err := getter.Get(dest, source)
	if err != nil {
		return "", fmt.Errorf("failed to get directory specified by the source %s: %s", source, err.Error())
	}

	return dest, nil
}

// Get the current timestamp
func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
