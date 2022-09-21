package plugin

import (
	"os"
	"path"
	"reflect"
	"testing"
)

var fileTestCurrentDir, _ = os.Getwd()
var fileTestHomeDir, _ = os.UserHomeDir()

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
		path.Join(fileTestHomeDir),
		"*.tf",
	},
	{
		"home dir (~) specific tf file",
		"~/Desktop/terraform/example.tf",
		path.Join(fileTestHomeDir, "/Desktop/terraform/example.tf"),
		"",
	},
	{
		"CWD (.) no file pattern",
		".",
		fileTestCurrentDir,
		"",
	},
	{
		"CWD (.) tf files",
		"./*.tf",
		fileTestCurrentDir,
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
