## Kind: design

# PR 965: "Run chaos acceptance tests" fails because steampipe and pipe-fittings still use tablewriter 0.x

**Feedback:** the required check "Run chaos acceptance tests" is red on head 0faa2a4. It builds
turbot/steampipe `develop` with this SDK replaced in, then `go mod tidy` and `go build`. The build fails in
`github.com/turbot/pipe-fittings/v2@v2.9.1/utils/timing.go` (`table.SetHeader`, `SetBorder`,
`SetReflowDuringAutoWrap`, `SetAutoWrapText`, `AppendBulk` undefined on `*tablewriter.Table`).

**Why I cannot action it:** the SDK's own code is already ported to tablewriter v1 and its Build job and
tests pass. The break is in other modules. `steampipe` develop and `pipe-fittings` develop both still
require `github.com/olekukonko/tablewriter v0.0.5` and call the removed v0 API. tablewriter v1 keeps the
same import path, so once the SDK requires v1.1.4, Go's minimum version selection raises the whole
build to v1 and pipe-fittings no longer compiles. Nothing in this repo can change that: a `replace` or
`exclude` here is ignored by modules that depend on the SDK. The fix lies in pipe-fittings (port
`utils/timing.go`), then a steampipe bump.

**Recommendation:** do not merge this bump yet. Either (a) hold it (`agent:skip`) until pipe-fittings and
steampipe are on tablewriter v1, then let the fleet re-sync it; or (b) close it with
`@dependabot ignore this major version` and reopen when the dependants have moved. Merging it as is
would leave the SDK unbuildable inside steampipe.
