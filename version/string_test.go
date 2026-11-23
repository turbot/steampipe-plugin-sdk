package version_test

import (
	"testing"

	goVersion "github.com/hashicorp/go-version"

	"github.com/turbot/steampipe-plugin-sdk/v6/version"
)

// The SDK's own test binary has no module version for the SDK, so it reports the
// fixed development version, which still parses as semver.
func TestStringInSdkTests(t *testing.T) {
	got := version.String()
	if got != "0.0.0-dev" {
		t.Fatalf("String() = %q, want %q", got, "0.0.0-dev")
	}
	if _, err := goVersion.NewVersion(got); err != nil {
		t.Fatalf("%q is not a semantic version: %v", got, err)
	}
}
