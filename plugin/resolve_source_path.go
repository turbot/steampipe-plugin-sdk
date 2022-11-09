package plugin

import (
	"fmt"
	"net/url"
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

	// parse source path to extract the raw query string
	u, err := url.Parse(sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse the source %s: %s", sourcePath, err.Error())
	}

	// if a S3 URL, build the URL path properly
	// URL with prefixes s3::, or git:: are not normal URL. For example:
	// s3::bucket.s3.amazonaws.com/test//*.tf?aws_profile=check&region=us-east-1
	// hence host and path comes empty while parsing the URL
	sourcePath = u.Path
	if u.Scheme != "" {
		if u.Host != "" && u.Path != "" { // i.e. https, http
			sourcePath = fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, u.Path)
		} else { // i.e. s3::, git::
			sourcePath = fmt.Sprintf("%s:%s", u.Scheme, u.Opaque)
		}
	}

	// extract the glob pattern from the source path
	lastIndex := strings.LastIndex(sourcePath, "//")
	if lastIndex != -1 && sourcePath[lastIndex-1:lastIndex] != ":" {
		globPattern = sourcePath[lastIndex+2:]
		sourcePath = sourcePath[:lastIndex]
	}

	// // if the source path for S3 has a '/' at the end, go-getter downloads all the contents stored inside that bucket.
	// // For example:
	// // s3::https://bucket.s3.us-east-1.amazonaws.com/
	// if strings.HasSuffix(sourcePath, ".amazonaws.com") {
	// 	sourcePath = fmt.Sprintf("%s/", sourcePath)
	// }

	// if any query string passed in the URL, append those with the source path
	if u.RawQuery != "" {
		sourcePath = fmt.Sprintf("%s?%s", sourcePath, u.RawQuery)
	}

	// if the source path is a S3 URL, and the path refers to a top-level file, for example:
	// s3::https://bucket.s3.amazonaws.com/foo.ext
	// send the path directly to go-getter, and use the destination path as glob pattern for file searching
	// and also remove the query parameters (parts after ?) from destination path
	if sourcePath == globPattern {
		filename := strings.Split(sourcePath, "/")
		dest = path.Join(dest, strings.Split(filename[len(filename)-1], "?")[0])
		globPattern = dest
	} else {
		// s3::https://my-bucket.s3.us-east-1.amazonaws.com/test_folder//*.ext?aws_profile=test_profile
		if strings.Contains(sourcePath, "amazonaws.com/") {
			sourceSplit := strings.Split(sourcePath, "amazonaws.com/")
			if len(sourceSplit) > 1 {
				dest = path.Join(dest, strings.Split(sourceSplit[1], "?")[0])
			}
		}
	}

	err = getter.Get(dest, sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to get directory specified by the source %s: %s", sourcePath, err.Error())
	}

	if globPattern != "" && dest != globPattern {
		globPattern = path.Join(dest, globPattern)
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
