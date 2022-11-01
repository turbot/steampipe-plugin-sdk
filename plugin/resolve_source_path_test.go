package plugin

import (
	"os"
	"path"
	"strings"
	"testing"
)

var fileTestCurrentDir, _ = os.Getwd()
var fileTestHomeDir, _ = os.UserHomeDir()

type sourceTest struct {
	Name                string
	Input               string
	ExpectedSourcePath  string
	ExpectedGlobPattern string
}

var sourcePaths = []sourceTest{
	// {
	// 	"matches top level tf files",
	// 	"/Users/subhajit/Desktop/terraform/*.tf",
	// 	"/Users/subhajit/Desktop/terraform",
	// 	"/Users/subhajit/Desktop/terraform/*.tf",
	// },
	// {
	// 	"recursive tf files",
	// 	"/Users/subhajit/Desktop/terraform/**/*.tf",
	// 	"/Users/subhajit/Desktop/terraform",
	// 	"/Users/subhajit/Desktop/terraform/**/*.tf",
	// },
	// {
	// 	"home dir (~) tf files",
	// 	"~/*.tf",
	// 	fileTestHomeDir,
	// 	path.Join(fileTestHomeDir, "*.tf"),
	// },
	// {
	// 	"home dir (~) specific tf file",
	// 	"~/Desktop/terraform/example.tf",
	// 	path.Join(fileTestHomeDir, "/Desktop/terraform"),
	// 	path.Join(fileTestHomeDir, "/Desktop/terraform/example.tf"),
	// },
	// {
	// 	"CWD (.) no file pattern",
	// 	".",
	// 	fileTestCurrentDir,
	// 	fileTestCurrentDir,
	// },
	// {
	// 	"CWD (.) tf files",
	// 	"./*.tf",
	// 	fileTestCurrentDir,
	// 	path.Join(fileTestCurrentDir, "*.tf"),
	// },
	{
		"github URL",
		"github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account/*.tf",
		"github.com/turbot/steampipe-plugin-alicloud",
		"alicloud-test/tests/alicloud_account/*.tf",
	},
	// for the below test cases, data fetched from the remote URL will be stored in /tmp/ directory,
	// and will require manual action to delete those directories
	{
		"matches Dockerfile stored inside a folder using S3 URL",
		"s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/ghost//Dockerfile",
		"",
		"Dockerfile",
	},
	{
		"matches Dockerfile stored at the top level of a bucket",
		"s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/Dockerfile",
		"",
		"Dockerfile",
	},
	{
		"matches all tf files inside a specific folder using github URL",
		"github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account/*.tf",
		"",
		"alicloud-test/tests/alicloud_account/*.tf",
	},
	{
		"matches all tf files recursively in a specific github URL",
		"git::https://github.com/turbot/steampipe-plugin-alicloud.git//alicloud-test/tests//**/*.tf",
		"",
		"**/*.tf",
	},
	{
		"matches all tf files in a specific bitbucket URL",
		"bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
		"",
		"*.tf",
	},
	{
		"matches all tf files in a specific bitbucket URL using git protocol",
		"git::bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
		"",
		"*.tf",
	},
	{
		"matches all tf files in a specific gitlab URL",
		"gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		"",
		"*.tf",
	},
	{
		"matches all tf files in a specific gitlab URL using git protocol",
		"git::gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		"",
		"*.tf",
	},
}

func TestResolveSourcePath(t *testing.T) {
	for _, test := range sourcePaths {
		source, glob, _ := ResolveSourcePath(test.Input, "/tmp")

		// if the input is a remote URL, downloaded data gets stored inside a tmp directory.
		// since, the name of the folder is dynamic, use the suffix to validate the glob
		if test.ExpectedSourcePath == "" {
			splitGlob := strings.Split(glob, "/")
			if len(splitGlob) >= 4 {
				glob = strings.Join(splitGlob[3:], "/")
			}
		}

		if test.ExpectedSourcePath != "" && source != test.ExpectedSourcePath {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, test.Name, test.ExpectedSourcePath, source)
		}
		if glob != test.ExpectedGlobPattern {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, test.Name, test.ExpectedGlobPattern, glob)
		}
	}
}
