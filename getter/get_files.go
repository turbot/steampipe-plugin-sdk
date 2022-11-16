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
//   - if local, it returns the root directory and any glob specified
//   - if remote, it uses go-getter to download the files into a temporary location,
//     then returns the location and the glob used the retrieve the files
//
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

	remoteSourcePath, globPattern, urlData, err := resolveGlobAndSourcePath(sourcePath)
	if err != nil {
		return "", "", err
	}

	// create temporary directory to store the go-getter data
	dest := createTempDirForGet(tmpDir)

	// If there is no glob pattern, source path is a filename - make the glob pattern the full DESTINATION file path
	if globPattern == "" {

		// if the source path is a S3 URL, and the path refers to a top-level file, for example:
		// s3::https://bucket.s3.amazonaws.com/foo.ext
		// send the path directly to go-getter, and use the destination path as glob pattern for file searching
		// and also remove the query parameters (parts after ?) from destination path

		parts := strings.Split(remoteSourcePath, string(os.PathSeparator))
		// extract file name from remoteSourcePath
		filename := parts[len(parts)-1]
		// build the dest filename and assign to glob
		dest = path.Join(dest, filename)
		globPattern = dest
	} else {
		// so this is a folder - apply special case s3 handling
		remoteSourcePath, dest = handleS3FolderPath(remoteSourcePath, dest)
	}

	// is there was a query string, escape the values and add to the url
	remoteSourcePath = addQueryToSourcePath(urlData, remoteSourcePath)

	err = getter.Get(dest, remoteSourcePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to get directory specified by the source %s: %s", remoteSourcePath, err.Error())
	}

	if globPattern != "" && dest != globPattern {
		globPattern = path.Join(dest, globPattern)
	}

	return dest, globPattern, nil
}

func addQueryToSourcePath(urlData *url.URL, sourcePath string) string {
	// if any query string passed in the URL, it will appear in u.RawQuery
	// (in other words we have stripped out the glob)
	if urlData.RawQuery == "" {
		return sourcePath
	}

	// iterate through all the query params and escape the characters (if needed)
	values := urlData.Query()
	for k := range values {
		// we must use values.Get rather that ranging over k,v as the value is an array
		// and Get returns the first value
		values.Set(k, url.QueryEscape(values.Get(k)))
	}
	queryString := values.Encode()

	// append the query params to the source path
	return fmt.Sprintf("%s?%s", sourcePath, queryString)
}

func handleS3FolderPath(remoteSourcePath string, dest string) (string, string) {
	// When querying s3 with go-getter, we specify the folder as part of the url
	// (as opposed to after a double slash - as it is for github/gitlab etc)
	// e.g:
	// 	s3::https://my-bucket.s3.us-east-1.amazonaws.com/test_folder//*.ext?aws_profile=test_profile
	//
	// In this case we need to extract the folder path from the url to build the dest path.
	//
	// We do this by taking everything after the string "amazonaws.com/"
	if strings.Contains(remoteSourcePath, "amazonaws.com") {
		// add the bucket folder to the end of the dest path
		sourceSplit := strings.Split(remoteSourcePath, "amazonaws.com/")
		if len(sourceSplit) > 1 {
			// extract the folder path, (removing query params, which will be re-added later)
			folderName := strings.Split(sourceSplit[1], "?")[0]
			// set dest to the dest folder path
			dest = path.Join(dest, folderName)
		}

		// go-getter supports an extra / at the end of the source path
		// which will download all data stored in that path

		// for example if source path is specified as:
		// 	s3::https://my-bucket.s3.us-east-1.amazonaws.com//*.ext?aws_profile=test_profile
		// we need to pass the following to go getter
		// (query param is re-added later)
		// 	s3::https://my-bucket.s3.us-east-1.amazonaws.com/?aws_profile=test_profile
		if !strings.HasSuffix(remoteSourcePath, "/") {
			remoteSourcePath += "/"
		}
	}
	return remoteSourcePath, dest
}

func resolveGlobAndSourcePath(sourcePath string) (remoteSourcePath, glob string, urlData *url.URL, err error) {
	// parse source path to extract the raw query string
	u, err := url.Parse(sourcePath)
	if err != nil {

		return "", "", nil, fmt.Errorf("failed to parse the source %s: %s", sourcePath, err.Error())
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

	return remoteSourcePath, globPattern, u, nil
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
