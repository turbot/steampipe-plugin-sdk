package plugin

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v5/getter"
)

type getSourceFilesTest struct {
	Input             string
	ExpectedFilePaths []string
}

type getSourceFilesWithFolderJailTest struct {
	Input             string
	ExpectedFilePaths []string
	// NOTE: these are relative, but will be converted to absolute before calling the test
	PermittedFileRoots []string
}

var wd, _ = os.Getwd()

var getSourceFilesTestCases = map[string]map[string]getSourceFilesTest{
	// local files
	"local": {
		"local - *.tf": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/*.tf"),
			ExpectedFilePaths: []string{"test_data/get_source_files_test/steampipe.tf", "test_data/get_source_files_test/steampipe1.tf", "test_data/get_source_files_test/steampunk.tf"},
		},
		"local - **/*.tf": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/**/*.tf"),
			ExpectedFilePaths: []string{"test_data/get_source_files_test/aws/accessanalyzer.tf", "test_data/get_source_files_test/aws/account.tf", "test_data/get_source_files_test/gcp/compute_instance_actual_earwig.tf", "test_data/get_source_files_test/gcp/compute_instance_native_rodent.tf", "test_data/get_source_files_test/gcp/main.tf", "test_data/get_source_files_test/gcp/vars.tf", "test_data/get_source_files_test/steampipe.tf", "test_data/get_source_files_test/steampipe1.tf", "test_data/get_source_files_test/steampunk.tf"},
		},
		"local - steampipe*.tf": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/steampipe*.tf"),
			ExpectedFilePaths: []string{"test_data/get_source_files_test/steampipe.tf", "test_data/get_source_files_test/steampipe1.tf"},
		},
		"local - steampipe.tf": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/steampipe.tf"),
			ExpectedFilePaths: []string{"test_data/get_source_files_test/steampipe.tf"},
		},
		"local - CWD": {
			Input:             filepath.Join(wd, "*.tf"),
			ExpectedFilePaths: nil,
		},
		"local - file.txt": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/**/file.txt"),
			ExpectedFilePaths: []string{"test_data/get_source_files_test/sub1/sub11/file.txt", "test_data/get_source_files_test/sub2/sub21/file.txt"},
		},
		"local - sub*/*/file.txt": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/sub*/*/file.txt"),
			ExpectedFilePaths: []string{"test_data/get_source_files_test/sub1/sub11/file.txt", "test_data/get_source_files_test/sub2/sub21/file.txt"},
		},
		"local - sub*/**/file.txt": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/sub*/**/file.txt"),
			ExpectedFilePaths: []string{"test_data/get_source_files_test/sub1/sub11/file.txt", "test_data/get_source_files_test/sub2/sub21/file.txt"},
		},
		"local - sub*/file.txt": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/sub*/file.txt"),
			ExpectedFilePaths: nil,
		},
		"local - **/file.txt": {
			Input:             filepath.Join(wd, "test_data/get_source_files_test/**/file.txt"),
			ExpectedFilePaths: []string{"test_data/get_source_files_test/sub1/sub11/file.txt", "test_data/get_source_files_test/sub2/sub21/file.txt"},
		},
	},
	"github": {
		// github urls
		"github url - specific folder - *.tf": {
			Input:             "github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account//*.tf",
			ExpectedFilePaths: []string{"variables.tf"},
		},
		"github url - specific folder - **/*.tf": {
			Input:             "github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests//**/*.tf",
			ExpectedFilePaths: []string{"alicloud_account/variables.tf", "alicloud_action_trail/variables.tf", "alicloud_cas_certificate/variables.tf", "alicloud_cms_monitor_host/variables.tf", "alicloud_cs_kubernetes_cluster/variables.tf", "alicloud_cs_kubernetes_cluster_node/variables.tf", "alicloud_ecs_auto_provisioning_group/variables.tf", "alicloud_ecs_key_pair/variables.tf", "alicloud_ecs_launch_template/variables.tf", "alicloud_ecs_network_interface/variables.tf", "alicloud_ecs_region/variables.tf", "alicloud_ecs_zone/variables.tf", "alicloud_kms_key/variables.tf", "alicloud_kms_secret/variables.tf", "alicloud_oss_bucket/variables.tf", "alicloud_ram_policy/variables.tf", "alicloud_ram_user/variables.tf", "alicloud_rds_instance/variables.tf", "alicloud_vpc_dhcp_options_set/variables.tf", "alicloud_vpc_nat_gateway/variables.tf", "alicloud_vpc_network_acl/variables.tf", "alicloud_vpc_route_entry/variables.tf", "alicloud_vpc_route_table/variables.tf", "alicloud_vpc_vpn_customer_gateway/variables.tf", "alicloud_vpc_vpn_gateway/variables.tf"},
		},
		"github url - specific file - no glob": {
			Input:             "github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account/variables.tf",
			ExpectedFilePaths: []string{"alicloud-test/tests/alicloud_account/variables.tf"},
		},
		"github force protocol - specific folder - *.tf": {
			Input:             "git::github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests/alicloud_account//*.tf",
			ExpectedFilePaths: []string{"variables.tf"},
		},
		"github force protocol - specific folder - **/*.tf": {
			Input:             "git::github.com/turbot/steampipe-plugin-alicloud//alicloud-test/tests//**/*.tf",
			ExpectedFilePaths: []string{"alicloud_account/variables.tf", "alicloud_action_trail/variables.tf", "alicloud_cas_certificate/variables.tf", "alicloud_cms_monitor_host/variables.tf", "alicloud_cs_kubernetes_cluster/variables.tf", "alicloud_cs_kubernetes_cluster_node/variables.tf", "alicloud_ecs_auto_provisioning_group/variables.tf", "alicloud_ecs_key_pair/variables.tf", "alicloud_ecs_launch_template/variables.tf", "alicloud_ecs_network_interface/variables.tf", "alicloud_ecs_region/variables.tf", "alicloud_ecs_zone/variables.tf", "alicloud_kms_key/variables.tf", "alicloud_kms_secret/variables.tf", "alicloud_oss_bucket/variables.tf", "alicloud_ram_policy/variables.tf", "alicloud_ram_user/variables.tf", "alicloud_rds_instance/variables.tf", "alicloud_vpc_dhcp_options_set/variables.tf", "alicloud_vpc_nat_gateway/variables.tf", "alicloud_vpc_network_acl/variables.tf", "alicloud_vpc_route_entry/variables.tf", "alicloud_vpc_route_table/variables.tf", "alicloud_vpc_vpn_customer_gateway/variables.tf", "alicloud_vpc_vpn_gateway/variables.tf"},
		},
		"github force protocol with https url - specific folder - *.tf": {
			Input:             "git::https://github.com/turbot/steampipe-plugin-alicloud.git//alicloud-test/tests/alicloud_account//*.tf",
			ExpectedFilePaths: []string{"variables.tf"},
		},
		"github force protocol with https url - specific file - no glob": {
			Input:             "git::https://github.com/turbot/steampipe-plugin-alicloud.git//alicloud-test/tests/alicloud_account/variables.tf",
			ExpectedFilePaths: []string{"alicloud-test/tests/alicloud_account/variables.tf"},
		},
	},
	"bitbucket": {
		// bitbucket urls
		"bitbucket url - **/*.tf": {
			Input:             "bitbucket.org/benturrell/terraform-arcgis-portal//**/*.tf",
			ExpectedFilePaths: []string{"_outputs.tf", "_variables.tf", "main.tf", "modules/app_layer/_outputs.tf", "modules/app_layer/_variables.tf", "modules/app_layer/app-sg.tf", "modules/app_layer/asg.tf", "modules/app_layer/asg_policies.tf", "modules/app_layer/efs_file_system.tf", "modules/app_layer/iam_instance_profile.tf", "modules/app_layer/launch_configuration.tf", "modules/app_layer/private-subnets.tf", "modules/public_layer/_outputs.tf", "modules/public_layer/_variables.tf", "modules/public_layer/elb-sg.tf", "modules/public_layer/elb.tf", "modules/public_layer/jump-sg.tf", "modules/public_layer/jumpbox.tf", "modules/public_layer/public-subnets.tf", "modules/public_layer/route53.tf", "modules/shared/_outputs.tf", "modules/shared/_variables.tf", "modules/shared/vpc.tf"},
		},
		"bitbucket url - specific folder - *.tf": {
			Input:             "bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
			ExpectedFilePaths: []string{"_outputs.tf", "_variables.tf", "vpc.tf"},
		},
		"bitbucket force protocol - specific folder - *.tf": {
			Input:             "git::bitbucket.org/benturrell/terraform-arcgis-portal//modules/shared//*.tf",
			ExpectedFilePaths: []string{"_outputs.tf", "_variables.tf", "vpc.tf"},
		},
	},
	"gitlab": {
		// gitlab urls
		"gitlab url - **/*.tf": {
			Input:             "gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
			ExpectedFilePaths: []string{"account.tf"},
		},
		"gitlab url - specific folder - *.tf": {
			Input:             "gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
			ExpectedFilePaths: []string{"account.tf"},
		},
		"gitlab force protocol - specific folder - *.tf": {
			Input:             "git::gitlab.com/subhajit7/example-files//terraform-examples//*.tf",
			ExpectedFilePaths: []string{"account.tf"},
		},
	},
	"s3": {
		// s3 urls
		"s3 force protocol - top-level - *.json": {
			Input:             "s3::https://cloudformation-templates-ap-southeast-1.s3.ap-southeast-1.amazonaws.com///*.json",
			ExpectedFilePaths: []string{"Managed_EC2_Batch_Environment.json", "Managed_EC2_and_Spot_Batch_Environment.json", "cc1-cluster.json", "cc2-cluster.json"},
		},
		"s3 force protocol - specific file - no glob": {
			Input:             "s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/ghost/Dockerfile",
			ExpectedFilePaths: []string{"Dockerfile"},
		},
		"s3 force protocol - specific folder - Dockerfile*": {
			Input:             "s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/ghost//Dockerfile*",
			ExpectedFilePaths: []string{"ghost/Dockerfile"},
		},
		"s3 force protocol with quey params - specific folder - Dockerfile*": {
			Input:             "s3::https://demo-integrated-2022.s3.ap-southeast-1.amazonaws.com/ghost//Dockerfile*",
			ExpectedFilePaths: []string{"ghost/Dockerfile"},
		},
		"s3 url with quey params - specific folder - Dockerfile*": {
			Input:             "demo-integrated-2022.s3-ap-southeast-1.amazonaws.com/ghost//Dockerfile",
			ExpectedFilePaths: []string{"ghost/Dockerfile"},
		},
		"s3 url with query params - top-level - *.json": {
			Input:             "cloudformation-templates-ap-southeast-1.s3-ap-southeast-1.amazonaws.com///*.json",
			ExpectedFilePaths: []string{"Managed_EC2_Batch_Environment.json", "Managed_EC2_and_Spot_Batch_Environment.json", "cc1-cluster.json", "cc2-cluster.json"},
		},
		"s3 url with query params - specific file - no glob": {
			Input:             "demo-integrated-2022.s3-ap-southeast-1.amazonaws.com/ghost/Dockerfile",
			ExpectedFilePaths: []string{"Dockerfile"},
		},
		"s3 url with query params - **/*.json": {
			Input:             "cloudformation-templates-ap-southeast-1.s3-ap-southeast-1.amazonaws.com///**/*.json",
			ExpectedFilePaths: []string{"Managed_EC2_Batch_Environment.json", "Managed_EC2_and_Spot_Batch_Environment.json", "cc1-cluster.json", "cc2-cluster.json"},
		},
	},
	"http": {
		"virtual-host style without http protocol prefix - *.json": {
			Input:             "s3-ap-southeast-1.amazonaws.com/cloudformation-templates-ap-southeast-1///*.json",
			ExpectedFilePaths: []string{"cloudformation-templates-ap-southeast-1/Managed_EC2_Batch_Environment.json", "cloudformation-templates-ap-southeast-1/Managed_EC2_and_Spot_Batch_Environment.json", "cloudformation-templates-ap-southeast-1/cc1-cluster.json", "cloudformation-templates-ap-southeast-1/cc2-cluster.json"},
		},
		"virtual-host style without http protocol prefix - **/*.json": {
			Input:             "s3-ap-southeast-1.amazonaws.com/cloudformation-templates-ap-southeast-1///**/*.json",
			ExpectedFilePaths: []string{"cloudformation-templates-ap-southeast-1/Managed_EC2_Batch_Environment.json", "cloudformation-templates-ap-southeast-1/Managed_EC2_and_Spot_Batch_Environment.json", "cloudformation-templates-ap-southeast-1/cc1-cluster.json", "cloudformation-templates-ap-southeast-1/cc2-cluster.json"},
		},
		"virtual-host style without http protocol prefix - no extra / - **/*.json": {
			Input:             "s3-ap-southeast-1.amazonaws.com/cloudformation-templates-ap-southeast-1//**/*.json",
			ExpectedFilePaths: []string{"cloudformation-templates-ap-southeast-1/Managed_EC2_Batch_Environment.json", "cloudformation-templates-ap-southeast-1/Managed_EC2_and_Spot_Batch_Environment.json", "cloudformation-templates-ap-southeast-1/cc1-cluster.json", "cloudformation-templates-ap-southeast-1/cc2-cluster.json"},
		},
		"virtual-host style without http protocol prefix - specific file - no glob": {
			Input:             "s3-ap-southeast-1.amazonaws.com/cloudformation-templates-ap-southeast-1/Managed_EC2_Batch_Environment.json",
			ExpectedFilePaths: []string{"Managed_EC2_Batch_Environment.json"},
		},
		"https URL - specific file - no glob": {
			Input:             "https://www.cisa.gov/sites/default/files/csv/known_exploited_vulnerabilities.csv",
			ExpectedFilePaths: []string{"known_exploited_vulnerabilities.csv/known_exploited_vulnerabilities.csv"},
		},
	},
}

func TestGetSourceFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "testGetSourceFiles")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	// required to list the bucket mentioned above in the test
	os.Setenv("AWS_REGION", "ap-southeast-1")

	q := &QueryData{tempDir: tmpDir}

	prefixDividerCount := 4

	ignoreBlocks := []string{"github", "bitbucket", "gitlab", "s3", "http"}

	for block, cases := range getSourceFilesTestCases {
		if slices.Contains(ignoreBlocks, block) {
			continue
		}
		for name, test := range cases {
			fmt.Printf(" %s\n", name)
			filePaths, err := q.GetSourceFiles(test.Input)
			if err != nil {
				if strings.Contains(err.Error(), "NoCredentialProviders") {
					t.Skip(`Skipping test`)
				}
				t.Errorf(`Test: '%s' ERROR : %v`, name, err)
			}

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
				t.Errorf(`Test: '%s' FAILED : expected %v, got %v`, name, test.ExpectedFilePaths, filePaths)
			}
		}
	}
}

var getSourceFilesWithFolderJailTestCases = map[string]getSourceFilesWithFolderJailTest{
	// local files
	"local - *.tf": {
		Input:              filepath.Join(wd, "test_data/get_source_files_test/*.tf"),
		ExpectedFilePaths:  []string{"test_data/get_source_files_test/steampipe.tf", "test_data/get_source_files_test/steampipe1.tf", "test_data/get_source_files_test/steampunk.tf"},
		PermittedFileRoots: []string{"test_data/get_source_files_test"},
	},
	"local - *.tf, NOT PERMITTED": {
		Input:              filepath.Join(wd, "test_data/get_source_files_test/*.tf"),
		ExpectedFilePaths:  []string{},
		PermittedFileRoots: []string{"some_other_folder"},
	},
	"local - **/*.tf": {
		Input:              filepath.Join(wd, "test_data/get_source_files_test/**/*.tf"),
		ExpectedFilePaths:  []string{"test_data/get_source_files_test/aws/accessanalyzer.tf", "test_data/get_source_files_test/aws/account.tf", "test_data/get_source_files_test/gcp/compute_instance_actual_earwig.tf", "test_data/get_source_files_test/gcp/compute_instance_native_rodent.tf", "test_data/get_source_files_test/gcp/main.tf", "test_data/get_source_files_test/gcp/vars.tf", "test_data/get_source_files_test/steampipe.tf", "test_data/get_source_files_test/steampipe1.tf", "test_data/get_source_files_test/steampunk.tf"},
		PermittedFileRoots: []string{"test_data/get_source_files_test"},
	},
	"local - steampipe*.tf": {
		Input:              filepath.Join(wd, "test_data/get_source_files_test/steampipe*.tf"),
		ExpectedFilePaths:  []string{"test_data/get_source_files_test/steampipe.tf", "test_data/get_source_files_test/steampipe1.tf"},
		PermittedFileRoots: []string{"test_data/get_source_files_test"},
	},
	"local - steampipe.tf - permitted paths not set": {
		Input:             filepath.Join(wd, "test_data/get_source_files_test/steampipe.tf"),
		ExpectedFilePaths: []string{"test_data/get_source_files_test/steampipe.tf"},
	},
}

func TestGetSourceFilesWithFolderJail(t *testing.T) {
	q := &QueryData{}

	for name, test := range getSourceFilesWithFolderJailTestCases {
		permittedAbsPaths := []string{}
		var envPermittedPaths string

		fmt.Printf("\n >>> %s\n", name)

		// convert into absolute paths
		for _, v := range test.PermittedFileRoots {
			permittedAbsPaths = append(permittedAbsPaths, filepath.Join(wd, v))
		}

		// create a comma separated string of permitted paths
		envPermittedPaths = strings.Join(permittedAbsPaths, ",")
		fmt.Printf("Permitted absolute paths: %s", envPermittedPaths)

		// set the env for STEAMPIPE_SDK_PERMITTED_ROOT_PATHS
		err := os.Setenv(getter.EnvPermittedFileRoots, envPermittedPaths)
		if err != nil {
			t.Errorf(`Test: '%s' ERROR : %v`, name, err)
		}

		filePaths, err := q.GetSourceFiles(test.Input)
		if err != nil {
			t.Errorf(`Test: '%s' ERROR : %v`, name, err)
		}

		// to compare [] vs nil
		if filePaths == nil {
			filePaths = []string{}
		}

		for i, filePath := range filePaths {
			var splitPath []string
			// remove the working directory prefix for the filepath
			if strings.Contains(filePath, wd) {
				splitPath = strings.Split(filePath, wd+"/")
				filePaths[i] = path.Join(splitPath[1:]...)
				continue
			}
		}

		if !reflect.DeepEqual(test.ExpectedFilePaths, filePaths) {
			t.Errorf(`Test: '%s' FAILED : expected %v, got %v`, name, test.ExpectedFilePaths, filePaths)
		}
	}
}
