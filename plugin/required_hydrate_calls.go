package plugin

import (
	"github.com/turbot/go-kit/helpers"
)

// helper class to build list of required hydrate calls
type requiredHydrateCallBuilder struct {
	fetchCallName        string
	requiredHydrateCalls map[string]*hydrateCall
	queryData            *QueryData
}

func newRequiredHydrateCallBuilder(d *QueryData, fetchCallName string) *requiredHydrateCallBuilder {
	return &requiredHydrateCallBuilder{
		queryData:            d,
		fetchCallName:        fetchCallName,
		requiredHydrateCalls: make(map[string]*hydrateCall),
	}
}

func (c requiredHydrateCallBuilder) Add(hydrateFunc HydrateFunc, callId string) {
	hydrateName := helpers.GetFunctionName(hydrateFunc)

	// if the resolved hydrate call is NOT the same as the fetch call, add to the map of hydrate functions to call
	if hydrateName != c.fetchCallName {
		if _, ok := c.requiredHydrateCalls[hydrateName]; ok {
			return
		}

		// get the config for this hydrate function
		config := c.queryData.Table.hydrateConfigMap[hydrateName]

		c.requiredHydrateCalls[hydrateName] = newHydrateCall(config, c.queryData)

		// now add dependencies (we have already checked for circular dependencies so recursion is fine
		for _, dep := range config.Depends {
			c.Add(dep, callId)
		}
	}
}

func (c requiredHydrateCallBuilder) Get() []*hydrateCall {
	var res []*hydrateCall
	for _, call := range c.requiredHydrateCalls {
		res = append(res, call)
	}
	return res
}
