package plugin

import (
	"encoding/json"
	"testing"

	goVersion "github.com/hashicorp/go-version"
)

// The SDK's own test binary has no module version for the SDK, so the version in a
// row's _ctx is the fixed development version, which still parses as semver.
func TestRowCtxReportsSdkVersion(t *testing.T) {
	t.Setenv(EnvDiagnosticsLevel, "")
	rd := &rowData{queryData: &QueryData{Connection: &Connection{Name: "test"}}}

	ctx := newRowCtxData(rd)
	const want = "0.0.0-dev"
	if ctx.Steampipe.SdkVersion != want {
		t.Fatalf("sdk version = %q, want %q", ctx.Steampipe.SdkVersion, want)
	}
	if _, err := goVersion.NewVersion(ctx.Steampipe.SdkVersion); err != nil {
		t.Fatalf("%q is not a semantic version: %v", ctx.Steampipe.SdkVersion, err)
	}

	b, err := json.Marshal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Steampipe struct {
			SdkVersion string `json:"sdk_version"`
		} `json:"steampipe"`
	}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Steampipe.SdkVersion != want {
		t.Fatalf("_ctx sdk_version = %q, want %q", decoded.Steampipe.SdkVersion, want)
	}
}
