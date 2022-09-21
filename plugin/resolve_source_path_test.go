package plugin

import (
	"os"
	"path"
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
		"/Users/subhajit/Desktop/terraform/*.tf",
	},
	{
		"recursive tf files",
		"/Users/subhajit/Desktop/terraform/**/*.tf",
		"/Users/subhajit/Desktop/terraform",
		"/Users/subhajit/Desktop/terraform/**/*.tf",
	},
	{
		"home dir (~) tf files",
		"~/*.tf",
		fileTestHomeDir,
		path.Join(fileTestHomeDir, "*.tf"),
	},
	{
		"home dir (~) specific tf file",
		"~/Desktop/terraform/example.tf",
		path.Join(fileTestHomeDir, "/Desktop/terraform"),
		path.Join(fileTestHomeDir, "/Desktop/terraform/example.tf"),
	},
	{
		"CWD (.) no file pattern",
		".",
		fileTestCurrentDir,
		fileTestCurrentDir,
	},
	{
		"CWD (.) tf files",
		"./*.tf",
		fileTestCurrentDir,
		path.Join(fileTestCurrentDir, "*.tf"),
	},
}

func TestResolveSourcePath(t *testing.T) {
	for _, test := range sourcePaths {
		source, glob, _ := ResolveSourcePath(test.Input, "/tmp")
		if source != test.Source {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, test.Name, test.Source, source)
		}
		if glob != test.Glob {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, test.Name, test.Glob, glob)
		}
	}
}
