package plugin

import "github.com/turbot/steampipe-plugin-sdk/v5/version"

type hydrateMetadata struct {
	Type         string            `json:"type"`
	FuncName     string            `json:"function_name"`
	ScopeValues  map[string]string `json:"scope_values"`
	RateLimiters []string          `json:"rate_limiters"`
	DelayMs      int64             `json:"rate_limiter_delay_ms"`
}

type SteampipeMetadata struct {
	SdkVersion string `json:"sdk_version"`
}

type rowCtxData struct {
	Connection string            `json:"connection_name"`
	Steampipe  SteampipeMetadata `json:"steampipe"`

	Diagnostics *rowCtxDiagnostics `json:"diagnostics,omitempty"`
}
type rowCtxDiagnostics struct {
	Calls []*hydrateMetadata `json:"calls"`
}

func newRowCtxData(rd *rowData) *rowCtxData {
	d := rd.queryData
	res := &rowCtxData{
		Connection: d.Connection.Name,
		Steampipe: SteampipeMetadata{
			SdkVersion: version.String(),
		},
	}

	if loadDiagnosticsEnvVar() == DiagnosticsAll {
		calls := append([]*hydrateMetadata{d.fetchMetadata}, rd.hydrateMetadata...)
		if d.parentHydrateMetadata != nil {
			calls = append([]*hydrateMetadata{d.parentHydrateMetadata}, calls...)
		}

		res.Diagnostics = &rowCtxDiagnostics{
			Calls: calls,
		}
	}
	return res
}
