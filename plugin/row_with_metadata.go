package plugin

type hydrateMetadata struct {
	FuncName     string            `json:"func_name"`
	ScopeValues  map[string]string `json:"scope_values,omitempty"`
	RateLimiters []string          `json:"rate_limiters,omitempty"`
	DelayMs      int64             `json:"rate_limiter_delay"`
}

type rowCtxData struct {
	Connection    string             `json:"connection"`
	FetchType     fetchType          `json:"fetch_type"`
	FetchCall     *hydrateMetadata   `json:"fetch_call"`
	ChildListCall *hydrateMetadata   `json:"child_list_call,omitempty"`
	HydrateCalls  []*hydrateMetadata `json:"hydrate_calls,omitempty"`
}

func newRowCtxData(d *QueryData, rd *rowData) *rowCtxData {
	return &rowCtxData{
		Connection:    d.Connection.Name,
		FetchType:     d.FetchType,
		FetchCall:     d.fetchMetadata,
		ChildListCall: d.childListMetadata,
		HydrateCalls:  rd.hydrateMetadata,
	}
}
