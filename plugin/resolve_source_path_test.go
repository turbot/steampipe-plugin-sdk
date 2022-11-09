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

type sourceTest struct {
	Name              string
	Input             string
	ExpectedFilePaths []string
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
		[]string{"ghost/Dockerfile"},
	},
	{
		"matches Dockerfile stored at the top level of a bucket",
		"s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/Dockerfile",
		[]string{"Dockerfile"},
	},
	{
		"matches all tf files inside a specific folder using github URL",
		"github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account//*.tf",
		[]string{"variables.tf"},
	},
	{
		"matches all tf files recursively in a specific github URL",
		"git::https://github.com/turbot/steampipe-plugin-alicloud.git//alicloud-test/tests//**/*.tf",
		[]string{"alicloud_account/variables.tf", "alicloud_action_trail/variables.tf", "alicloud_cas_certificate/variables.tf", "alicloud_cms_monitor_host/variables.tf", "alicloud_cs_kubernetes_cluster/variables.tf", "alicloud_cs_kubernetes_cluster_node/variables.tf", "alicloud_ecs_auto_provisioning_group/variables.tf", "alicloud_ecs_key_pair/variables.tf", "alicloud_ecs_launch_template/variables.tf", "alicloud_ecs_network_interface/variables.tf", "alicloud_ecs_region/variables.tf", "alicloud_ecs_zone/variables.tf", "alicloud_kms_key/variables.tf", "alicloud_kms_secret/variables.tf", "alicloud_oss_bucket/variables.tf", "alicloud_ram_policy/variables.tf", "alicloud_ram_user/variables.tf", "alicloud_rds_instance/variables.tf", "alicloud_vpc_dhcp_options_set/variables.tf", "alicloud_vpc_nat_gateway/variables.tf", "alicloud_vpc_network_acl/variables.tf", "alicloud_vpc_route_entry/variables.tf", "alicloud_vpc_route_table/variables.tf", "alicloud_vpc_vpn_customer_gateway/variables.tf", "alicloud_vpc_vpn_gateway/variables.tf"},
	},
	{
		"matches all tf files in a specific bitbucket URL",
		"bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
		[]string{"_outputs.tf", "_variables.tf", "vpc.tf"},
	},
	{
		"matches all tf files in a specific bitbucket URL using git protocol",
		"git::bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
		[]string{"_outputs.tf", "_variables.tf", "vpc.tf"},
	},
	{
		"matches all tf files in a specific gitlab URL",
		"gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		[]string{"account.tf"},
	},
	{
		"matches all tf files in a specific gitlab URL using git protocol",
		"git::gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		[]string{"account.tf"},
	},

	{
		"test",
		"gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		[]string{"account.tf"},
	},
}

func TestResolveSourcePath(t *testing.T) {
	tmpDir := filepath.Join("/tmp", "testGetSourceFiles")
	if !filehelpers.DirectoryExists(tmpDir) {
		os.RemoveAll(tmpDir)
	}
	os.Mkdir(tmpDir, fs.ModePerm)
	defer os.RemoveAll(tmpDir)

	q := &QueryData{tempDir: tmpDir}

	for _, source := range sources {
		filePaths, _ := q.GetSourceFiles(source.Input)
		for i, filePath := range filePaths {
			splitPath := strings.Split(filePath, "/")
			filePaths[i] = path.Join(splitPath[4:]...)
		}

		if !reflect.DeepEqual(source.ExpectedFilePaths, filePaths) {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, source.Name, source.ExpectedFilePaths, filePaths)
		}
	}
}
