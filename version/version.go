// Package version defines the SDK version
package version

import (
	"runtime/debug"
	"strings"

	goVersion "github.com/hashicorp/go-version"
)

// ProtocolVersion marks the protobuf interface version
// // Update this if breaking changes are made to the protobuf spec (for example column types)
var ProtocolVersion int64 = 20220201

// sdkModulePath is the module path of the SDK, as it appears in the dependency list of a plugin binary
const sdkModulePath = "github.com/turbot/steampipe-plugin-sdk/v6"

// devVersion is reported when the build carries no module version for the SDK:
// the SDK's own tests, a replace with a local path, or a binary with no build information.
const devVersion = "0.0.0-dev"

// sdkVersion is the version this binary reports. It is read from the Go build information
// when the package is loaded, and is always a valid semantic version.
var sdkVersion = func() string {
	info, ok := debug.ReadBuildInfo()
	return versionFromBuildInfo(info, ok)
}()

// versionFromBuildInfo returns the SDK module's version from the dependencies of the build,
// with the leading "v" dropped and a pseudo-version kept as it is.
// A replacement of the SDK takes the place of the SDK: its version is the one reported.
func versionFromBuildInfo(info *debug.BuildInfo, ok bool) string {
	if !ok || info == nil {
		return devVersion
	}
	for _, dep := range info.Deps {
		if dep == nil || dep.Path != sdkModulePath {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		return cleanVersion(dep.Version)
	}
	return devVersion
}

// cleanVersion drops the leading "v" of a module version. A version that is empty, "(devel)"
// (a local path) or not a semantic version gives devVersion.
func cleanVersion(v string) string {
	v = strings.TrimPrefix(v, "v")
	if v == "" {
		return devVersion
	}
	if _, err := goVersion.NewVersion(v); err != nil {
		return devVersion
	}
	return v
}

// String returns the complete version string, including any pre-release part
func String() string {
	return sdkVersion
}
