package version

import (
	"runtime/debug"
	"testing"

	goVersion "github.com/hashicorp/go-version"
)

func sdkDep(version string, replace *debug.Module) *debug.Module {
	return &debug.Module{Path: sdkModulePath, Version: version, Replace: replace}
}

func TestVersionFromBuildInfo(t *testing.T) {
	other := &debug.Module{Path: "github.com/other/module", Version: "v1.2.3"}

	tests := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{"tagged release", &debug.BuildInfo{Deps: []*debug.Module{other, sdkDep("v6.3.0", nil)}}, true, "6.3.0"},
		{"tagged pre-release", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("v6.3.0-rc.1", nil)}}, true, "6.3.0-rc.1"},
		{"untagged commit pseudo-version", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("v6.2.3-0.20261123120000-abcdef123456", nil)}}, true, "6.2.3-0.20261123120000-abcdef123456"},
		{"replaced by another released version", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("v6.3.0", &debug.Module{Path: sdkModulePath, Version: "v6.1.0"})}}, true, "6.1.0"},
		{"replaced by a fork at a version", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("v6.3.0", &debug.Module{Path: "github.com/fork/sdk/v6", Version: "v6.9.0"})}}, true, "6.9.0"},
		{"local replace has no version", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("v6.3.0", &debug.Module{Path: "../sdk"})}}, true, devVersion},
		{"local replace reports (devel)", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("v6.3.0", &debug.Module{Path: "../sdk", Version: "(devel)"})}}, true, devVersion},
		{"sdk version is (devel)", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("(devel)", nil)}}, true, devVersion},
		{"sdk version is empty", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("", nil)}}, true, devVersion},
		{"sdk version is not semver", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("vnonsense", nil)}}, true, devVersion},
		{"nil dependency entry is skipped", &debug.BuildInfo{Deps: []*debug.Module{nil, sdkDep("v6.3.0", nil)}}, true, "6.3.0"},
		{"sdk is not a dependency (its own tests)", &debug.BuildInfo{Main: debug.Module{Path: sdkModulePath, Version: "(devel)"}, Deps: []*debug.Module{other}}, true, devVersion},
		{"no dependencies", &debug.BuildInfo{}, true, devVersion},
		{"no build information", nil, false, devVersion},
		{"build information not available but non-nil", &debug.BuildInfo{Deps: []*debug.Module{sdkDep("v6.3.0", nil)}}, false, devVersion},
		{"nil build information reported ok", nil, true, devVersion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := versionFromBuildInfo(tt.info, tt.ok)
			if got != tt.want {
				t.Fatalf("versionFromBuildInfo() = %q, want %q", got, tt.want)
			}
			if _, err := goVersion.NewVersion(got); err != nil {
				t.Fatalf("%q is not a semantic version: %v", got, err)
			}
		})
	}
}
