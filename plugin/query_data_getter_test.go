package plugin

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	filehelpers "github.com/turbot/go-kit/files"
)

type getSourceFilesTest struct {
	Input             string
	ExpectedFilePaths []string
}

var getSourceFilesTestCases = map[string]getSourceFilesTest{
	"matches top level tf files": {
		Input:             "/Users/subhajit/Desktop/terraform/*.tf",
		ExpectedFilePaths: []string{"terraform/aws.tf", "terraform/example.tf", "terraform/test.tf"},
	},
	"recursive tf files": {
		"/Users/subhajit/Desktop/terraform/**/*.tf",
		[]string{"terraform/aws.tf", "terraform/example.tf", "terraform/test.tf"},
	},
	"home dir (~) tf files": {
		Input:             "~/*.tf",
		ExpectedFilePaths: []string{},
	},
	"home dir (~) specific tf file": {
		Input:             "~/Desktop/terraform/example.tf",
		ExpectedFilePaths: []string{"terraform/example.tf"},
	},
	"CWD (.) tf files": {
		Input:             "./*.tf",
		ExpectedFilePaths: []string{},
	},
	// for the below test cases, data fetched from the remote URL will be stored in /tmp/ directory,
	// and will require manual action to delete those directories
	"matches Dockerfile stored inside a folder using S3 URL": {
		Input:             "s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/ghost//Dockerfile",
		ExpectedFilePaths: []string{"ghost/Dockerfile"},
	},
	"matches Dockerfile stored at the top level of a bucket": {
		Input:             "s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/Dockerfile",
		ExpectedFilePaths: []string{"Dockerfile"},
	},
	"matches all tf files inside a specific folder using github URL": {
		Input:             "github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account//*.tf",
		ExpectedFilePaths: []string{"variables.tf"},
	},
	"matches all tf files recursively in a specific github URL": {
		Input:             "s3::https://my-bucket.s3.us-east-1.amazonaws.com/test_folder?aws_profile=test_profile",
		ExpectedFilePaths: []string{"alicloud_account/variables.tf", "alicloud_action_trail/variables.tf", "alicloud_cas_certificate/variables.tf", "alicloud_cms_monitor_host/variables.tf", "alicloud_cs_kubernetes_cluster/variables.tf", "alicloud_cs_kubernetes_cluster_node/variables.tf", "alicloud_ecs_auto_provisioning_group/variables.tf", "alicloud_ecs_key_pair/variables.tf", "alicloud_ecs_launch_template/variables.tf", "alicloud_ecs_network_interface/variables.tf", "alicloud_ecs_region/variables.tf", "alicloud_ecs_zone/variables.tf", "alicloud_kms_key/variables.tf", "alicloud_kms_secret/variables.tf", "alicloud_oss_bucket/variables.tf", "alicloud_ram_policy/variables.tf", "alicloud_ram_user/variables.tf", "alicloud_rds_instance/variables.tf", "alicloud_vpc_dhcp_options_set/variables.tf", "alicloud_vpc_nat_gateway/variables.tf", "alicloud_vpc_network_acl/variables.tf", "alicloud_vpc_route_entry/variables.tf", "alicloud_vpc_route_table/variables.tf", "alicloud_vpc_vpn_customer_gateway/variables.tf", "alicloud_vpc_vpn_gateway/variables.tf"},
	},
	"matches all tf files in a specific bitbucket URL": {
		Input:             "bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
		ExpectedFilePaths: []string{"_outputs.tf", "_variables.tf", "vpc.tf"},
	},
	"matches all tf files in a specific bitbucket URL using git protocol": {
		Input:             "git::bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
		ExpectedFilePaths: []string{"_outputs.tf", "_variables.tf", "vpc.tf"},
	},
	"matches all tf files in a specific gitlab URL": {
		Input:             "gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		ExpectedFilePaths: []string{"account.tf"},
	},
	"matches all tf files in a specific gitlab URL using git protocol": {
		Input:             "git::gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		ExpectedFilePaths: []string{"account.tf"},
	},
	"test": {
		Input:             "gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		ExpectedFilePaths: []string{"account.tf"},
	},
}

func TestGetSourceFiles(t *testing.T) {
	tmpDir := filepath.Join("/tmp", "testGetSourceFiles")
	if !filehelpers.DirectoryExists(tmpDir) {
		os.RemoveAll(tmpDir)
	}
	os.Mkdir(tmpDir, fs.ModePerm)
	defer os.RemoveAll(tmpDir)

	q := &QueryData{tempDir: tmpDir}

	prefixDividerCount := 4

	for name, test := range getSourceFilesTestCases {
		filePaths, _ := q.GetSourceFiles(test.Input)

		for i, filePath := range filePaths {
			// remove the <tmpdir>/<timestamp>/ prefix for the filepath
			splitPath := strings.Split(filePath, string(os.PathSeparator))
			filePaths[i] = path.Join(splitPath[prefixDividerCount:]...)
		}

		if !reflect.DeepEqual(test.ExpectedFilePaths, filePaths) {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, name, test.ExpectedFilePaths, filePaths)
		}
	}
}
