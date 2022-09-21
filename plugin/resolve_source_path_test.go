package plugin

import (
	"reflect"
	"testing"
)

type sourceTest struct {
	Name   string
	Input  string
	Source string
	Glob   string
}

var sourcePaths = []sourceTest{
	{
		"matches top level tf files",
		"/Users/subhajit/Desktop/terraform/*.tf",
		"/Users/subhajit/Desktop/terraform",
		"*.tf",
	},
	{
		"recursive tf files",
		"/Users/subhajit/Desktop/terraform/**/*.tf",
		"/Users/subhajit/Desktop/terraform",
		"**/*.tf",
	},
	{
		"home dir (~) tf files",
		"~/*.tf",
		"/Users/subhajit",
		"*.tf",
	},
	{
		"home dir (~) specific tf file",
		"~/Desktop/terraform/example.tf",
		"/Users/subhajit/Desktop/terraform/example.tf",
		"",
	},
	{
		"CWD (.) no file pattern",
		".",
		"/Users/subhajit/turbot-prod/steampipeio/sdks/steampipe-plugin-sdk/plugin",
		"",
	},
	{
		"CWD (.) tf files",
		"./*.tf",
		"/Users/subhajit/turbot-prod/steampipeio/sdks/steampipe-plugin-sdk/plugin",
		"*.tf",
	},
}

func TestResolveSourcePath(t *testing.T) {
	for _, test := range sourcePaths {
		source, glob, _ := ResolveSourcePath(test.Input, "")
		if !reflect.DeepEqual(source, test.Source) {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, test.Name, test.Source, source)
		}
		if !reflect.DeepEqual(glob, test.Glob) {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, test.Name, test.Glob, glob)
		}
	}
}
