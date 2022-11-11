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

var wd, _ = os.Getwd()

var getSourceFilesTestCases = map[string]getSourceFilesTest{
	// local files
	"matches all tf files in a specific path": {
		Input:             filepath.Join(wd, "test_data/source_files/*.tf"),
		ExpectedFilePaths: []string{"test_data/source_files/steampipe.tf", "test_data/source_files/steampipe1.tf", "test_data/source_files/steampunk.tf"},
	},
	"matches all tf files recursively": {
		Input:             filepath.Join(wd, "test_data/source_files/**/*.tf"),
		ExpectedFilePaths: []string{"test_data/source_files/aws/accessanalyzer.tf", "test_data/source_files/aws/account.tf", "test_data/source_files/gcp/compute_instance_actual_earwig.tf", "test_data/source_files/gcp/compute_instance_native_rodent.tf", "test_data/source_files/gcp/main.tf", "test_data/source_files/gcp/vars.tf", "test_data/source_files/steampipe.tf", "test_data/source_files/steampipe1.tf", "test_data/source_files/steampunk.tf"},
	},
	"matches all tf files starts with steampipe": {
		Input:             filepath.Join(wd, "test_data/source_files/steampipe*.tf"),
		ExpectedFilePaths: []string{"test_data/source_files/steampipe.tf", "test_data/source_files/steampipe1.tf"},
	},
	"matches a specific file": {
		Input:             filepath.Join(wd, "test_data/source_files/steampipe.tf"),
		ExpectedFilePaths: []string{"test_data/source_files/steampipe.tf"},
	},
	"matches all tf files in CWD": {
		Input:             filepath.Join(wd, "*.tf"),
		ExpectedFilePaths: nil,
	},

	// github URLs
	"matches all tf files inside a specific folder using github URL": {
		Input:             "github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account//*.tf",
		ExpectedFilePaths: []string{"variables.tf"},
	},
	"matches all tf files recursively in a specific github URL": {
		Input:             "github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests//**/*.tf",
		ExpectedFilePaths: []string{"alicloud_account/variables.tf", "alicloud_action_trail/variables.tf", "alicloud_cas_certificate/variables.tf", "alicloud_cms_monitor_host/variables.tf", "alicloud_cs_kubernetes_cluster/variables.tf", "alicloud_cs_kubernetes_cluster_node/variables.tf", "alicloud_ecs_auto_provisioning_group/variables.tf", "alicloud_ecs_key_pair/variables.tf", "alicloud_ecs_launch_template/variables.tf", "alicloud_ecs_network_interface/variables.tf", "alicloud_ecs_region/variables.tf", "alicloud_ecs_zone/variables.tf", "alicloud_kms_key/variables.tf", "alicloud_kms_secret/variables.tf", "alicloud_oss_bucket/variables.tf", "alicloud_ram_policy/variables.tf", "alicloud_ram_user/variables.tf", "alicloud_rds_instance/variables.tf", "alicloud_vpc_dhcp_options_set/variables.tf", "alicloud_vpc_nat_gateway/variables.tf", "alicloud_vpc_network_acl/variables.tf", "alicloud_vpc_route_entry/variables.tf", "alicloud_vpc_route_table/variables.tf", "alicloud_vpc_vpn_customer_gateway/variables.tf", "alicloud_vpc_vpn_gateway/variables.tf"},
	},
	"matches all tf files inside a specific folder using github URL with force protocol": {
		Input:             "git::github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account//*.tf",
		ExpectedFilePaths: []string{"variables.tf"},
	},
	"matches all tf files recursively in a specific github URL with force protocol": {
		Input:             "git::github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests//**/*.tf",
		ExpectedFilePaths: []string{"alicloud_account/variables.tf", "alicloud_action_trail/variables.tf", "alicloud_cas_certificate/variables.tf", "alicloud_cms_monitor_host/variables.tf", "alicloud_cs_kubernetes_cluster/variables.tf", "alicloud_cs_kubernetes_cluster_node/variables.tf", "alicloud_ecs_auto_provisioning_group/variables.tf", "alicloud_ecs_key_pair/variables.tf", "alicloud_ecs_launch_template/variables.tf", "alicloud_ecs_network_interface/variables.tf", "alicloud_ecs_region/variables.tf", "alicloud_ecs_zone/variables.tf", "alicloud_kms_key/variables.tf", "alicloud_kms_secret/variables.tf", "alicloud_oss_bucket/variables.tf", "alicloud_ram_policy/variables.tf", "alicloud_ram_user/variables.tf", "alicloud_rds_instance/variables.tf", "alicloud_vpc_dhcp_options_set/variables.tf", "alicloud_vpc_nat_gateway/variables.tf", "alicloud_vpc_network_acl/variables.tf", "alicloud_vpc_route_entry/variables.tf", "alicloud_vpc_route_table/variables.tf", "alicloud_vpc_vpn_customer_gateway/variables.tf", "alicloud_vpc_vpn_gateway/variables.tf"},
	},

	// bitbucket URLs
	"matches all tf files in a specific bitbucket URL": {
		Input:             "bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
		ExpectedFilePaths: []string{"_outputs.tf", "_variables.tf", "vpc.tf"},
	},
	"matches all tf files in a specific bitbucket URL using git protocol": {
		Input:             "git::bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
		ExpectedFilePaths: []string{"_outputs.tf", "_variables.tf", "vpc.tf"},
	},

	// gitlab URLs
	"matches all tf files in a specific gitlab URL": {
		Input:             "gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		ExpectedFilePaths: []string{"account.tf"},
	},
	"matches all tf files in a specific gitlab URL using git protocol": {
		Input:             "git::gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
		ExpectedFilePaths: []string{"account.tf"},
	},

	// s3 URLs
	"matches Dockerfile stored inside a folder using S3 URL": {
		Input:             "s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/ghost//Dockerfile",
		ExpectedFilePaths: []string{"ghost/Dockerfile"},
	},
	"matches Dockerfile stored inside a folder using S3 URL with query params": {
		Input:             "s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/ghost//Dockerfile?aws_profile=default",
		ExpectedFilePaths: []string{"ghost/Dockerfile"},
	},
	"matches Dockerfile stored inside a S3 bucket folder without using s3 protocol in the URL": {
		Input:             "demo-integrated-2022.s3-ap-southeast-1.amazonaws.com/ghost//Dockerfile?aws_profile=default",
		ExpectedFilePaths: []string{"ghost/Dockerfile"},
	},
	"matches JSON files stored inside a S3 bucket without using s3 protocol in the URL": {
		Input:             "cloudformation-templates-ap-southeast-1.s3-ap-southeast-1.amazonaws.com///*.json?aws_profile=default",
		ExpectedFilePaths: []string{"Managed_EC2_Batch_Environment.json", "Managed_EC2_and_Spot_Batch_Environment.json", "cc1-cluster.json", "cc2-cluster.json"},
	},
}

func TestGetSourceFiles(t *testing.T) {
	tmpDir := filepath.Join("/tmp", "testGetSourceFiles")
	if !filehelpers.DirectoryExists(tmpDir) {
		os.RemoveAll(tmpDir)
	}
	os.Mkdir(tmpDir, fs.ModePerm)
	defer os.RemoveAll(tmpDir)

	// required to list the bucket mentioned above in the test
	os.Setenv("AWS_REGION", "ap-southeast-1")

	q := &QueryData{tempDir: tmpDir}

	prefixDividerCount := 4

	for name, test := range getSourceFilesTestCases {
		filePaths, _ := q.GetSourceFiles(test.Input)

		for i, filePath := range filePaths {
			var splitPath []string
			// remove the <tmpdir>/<timestamp>/ prefix for the filepath
			if strings.Contains(filePath, wd) {
				splitPath = strings.Split(filePath, wd+"/")
				filePaths[i] = path.Join(splitPath[1:]...)
				continue
			}
			splitPath = strings.Split(filePath, string(os.PathSeparator))
			filePaths[i] = path.Join(splitPath[prefixDividerCount:]...)
		}

		if !reflect.DeepEqual(test.ExpectedFilePaths, filePaths) {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, name, test.ExpectedFilePaths, filePaths)
		}
	}
}
