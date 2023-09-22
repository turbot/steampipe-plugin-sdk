package plugin

type hydrateMetadata struct {
	Type         string            `json:"type"`
	ScopeValues  map[string]string `json:"scope_values"`
	RateLimiters []string          `json:"rate_limiters"`
	DelayMs      int64             `json:"rate_limiter_delay_ms"`
}

type rowCtxData struct {
	Connection  string             `json:"connection_name"`
	Diagnostics *rowCtxDiagnostics `json:"diagnostics,omitempty"`
}
type rowCtxDiagnostics struct {
	Calls []*hydrateMetadata `json:"calls"`
}

func newRowCtxData(rd *rowData) *rowCtxData {
	d := rd.queryData
	res := &rowCtxData{
		Connection: d.Connection.Name,
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
