package version

import (
	"fmt"

	version "github.com/hashicorp/go-version"
)

// ProtocolVersion :: update this if breaking changes are made to the protobuf spec
// (for example column types)
var ProtocolVersion int64 = 20210505

// Version :: the main version number that is being run at the moment.
var Version = "0.2.8"

// Prerelease :: a pre-release marker for the version
// if this is "" (empty string) then it means that it is a final release
// otherwise, this is a pre-release such as "dev" (in development), "beta", "rc1", etc.
var Prerelease = ""

// SemVer is an instance of version.Version. This has the secondary
// benefit of verifying during tests and init time that our version is a
// proper semantic version, which should always be the case.
var SemVer *version.Version

func init() {
	str := Version
	if Prerelease != "" {
		str = fmt.Sprintf("%s-%s", Version, Prerelease)
	}
	SemVer = version.Must(version.NewVersion(str))
}

// String :: return the complete version string, including prerelease
func String() string {
	return SemVer.String()
}
