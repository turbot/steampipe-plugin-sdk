package getter

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

// GetFiles determines whether sourcePath is a local or remote path
// - if local, it returns the root directory and any glob specified
// - if remote, it uses go-getter to download the files into a temporary location,
//   then returns the location and the glob used the retrieve the files
// and the glob
func GetFiles(sourcePath, tmpDir string) (localSourcePath string, globPattern string, err error) {
	if sourcePath == "" {
		return "", "", fmt.Errorf("source cannot be empty")
	}

	// check whether sourcePath is a glob with a root location which exists in the file system
	localSourcePath, globPattern, err = filehelpers.GlobRoot(sourcePath)
	if err != nil {
		return "", "", err
	}
	// if we managed to resolve the sourceDir, treat this as a local path
	if localSourcePath != "" {
		return localSourcePath, globPattern, nil
	}

	remoteSourcePath, queryString, globPattern, err := resolveGlobAndSourcePath(sourcePath)
	if err != nil {
		return "", "", err
	}

	// create temporary directory to store the go-getter data
	dest := createTempDirForGet(tmpDir)

	// TODO tidy this up
	// if the source path is a S3 URL, and the path refers to a top-level file, for example:
	// s3::https://bucket.s3.amazonaws.com/foo.ext
	// send the path directly to go-getter, and use the destination path as glob pattern for file searching
	// and also remove the query parameters (parts after ?) from destination path

	// IF THIS IS AN S3 FILENAME
	// special case S3 code
	if globPattern == "" {
		parts := strings.Split(remoteSourcePath, string(os.PathSeparator))
		filename := parts[len(parts)-1]
		dest = path.Join(dest, filename)
		globPattern = dest
	} else {
		// IF THIS IS AN S3 FOLDER
		// s3::https://my-bucket.s3.us-east-1.amazonaws.com/test_folder//*.ext?aws_profile=test_profile
		if strings.Contains(remoteSourcePath, "amazonaws.com/") {
			// add the bucket folder to the end of the dest path
			sourceSplit := strings.Split(remoteSourcePath, "amazonaws.com/")
			if len(sourceSplit) > 1 {
				dest = path.Join(dest, strings.Split(sourceSplit[1], "?")[0])
			}
		}
	}

	// if any query string passed in the URL, it will appear in u.RawQuery
	// append this back into with the source path
	// (in other words we have stripped out the glob)
	if queryString != "" {
		remoteSourcePath = fmt.Sprintf("%s?%s", remoteSourcePath, queryString)
	}

	err = getter.Get(dest, remoteSourcePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to get directory specified by the source %s: %s", remoteSourcePath, err.Error())
	}

	if globPattern != "" && dest != globPattern {
		globPattern = path.Join(dest, globPattern)
	}

	return dest, globPattern, nil
}

func resolveGlobAndSourcePath(sourcePath string) (remoteSourcePath, queryString, glob string, err error) {
	// parse source path to extract the raw query string
	u, err := url.Parse(sourcePath)
	if err != nil {

		return "", "", "", fmt.Errorf("failed to parse the source %s: %s", sourcePath, err.Error())
	}

	// rebuild the source path without any query params
	remoteSourcePath = removeQueryParams(u, remoteSourcePath)

	// extract the glob
	// e.g. for github.com/turbot/steampipe-plugin-alicloud//*.tf"
	// remoteSourcePath is github.com/turbot/steampipe-plugin-alicloud
	// glob is *.tf
	remoteSourcePath, globPattern := extractGlob(remoteSourcePath)

	// // if the source path for S3 has a '/' at the end, go-getter downloads all the contents stored inside that bucket.
	// // For example:
	// // s3::https://bucket.s3.us-east-1.amazonaws.com/
	// if strings.HasSuffix(sourcePath, ".amazonaws.com") {
	// 	sourcePath = fmt.Sprintf("%s/", sourcePath)
	// }

	return remoteSourcePath, u.RawQuery, globPattern, nil
}

func removeQueryParams(u *url.URL, remoteSourcePath string) string {

	if u.Scheme == "" {
		// no scheme specified
		// e.g. gitlab.com/subhajit7/example-files//terraform-examples//*.tf
		remoteSourcePath = u.Path
	} else {
		// go getter supports s3 and git urls which have prefixes s3:: or git::
		// For example:
		// 	s3::bucket.s3.amazonaws.com/test//*.tf?aws_profile=check&region=us-east-1
		// 	git::bitbucket.org/benturrell/terraform-arcgis-portal
		// In these cases host and path comes empty while parsing the URL
		if u.Host != "" && u.Path != "" {
			// i.e. https, http
			remoteSourcePath = fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, u.Path)
		} else {
			// i.e. s3::, git::
			remoteSourcePath = fmt.Sprintf("%s:%s", u.Scheme, u.Opaque)
		}
	}
	return remoteSourcePath
}

// extract the glob pattern from the source path
func extractGlob(remoteSourcePath string) (string, string) {
	var globPattern string
	lastIndex := strings.LastIndex(remoteSourcePath, "//")
	// if this is NOT the '//' after a http://
	if lastIndex != -1 && remoteSourcePath[lastIndex-1:lastIndex] != ":" {
		globPattern = remoteSourcePath[lastIndex+2:]
		remoteSourcePath = remoteSourcePath[:lastIndex]
	}
	return remoteSourcePath, globPattern
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
