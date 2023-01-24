package plugin

import (
	"github.com/turbot/go-kit/helpers"
)

// helper class to build list of required hydrate calls
type requiredHydrateCallBuilder struct {
	fetchCallName        string
	requiredHydrateCalls map[string]*HydrateCall
	table                *Table
}

func newRequiredHydrateCallBuilder(t *Table, fetchCallName string) *requiredHydrateCallBuilder {
	return &requiredHydrateCallBuilder{
		table:                t,
		fetchCallName:        fetchCallName,
		requiredHydrateCalls: make(map[string]*HydrateCall),
	}
}

func (c requiredHydrateCallBuilder) Add(hydrateFunc HydrateFunc) {
	hydrateName := helpers.GetFunctionName(hydrateFunc)

	// if the resolved hydrate call is NOT the same as the fetch call, add to the map of hydrate functions to call
	if hydrateName != c.fetchCallName {
		if _, ok := c.requiredHydrateCalls[hydrateName]; ok {
			return
		}

		// get the config for this hydrate function
		config := c.table.hydrateConfigMap[hydrateName]

		c.requiredHydrateCalls[hydrateName] = newHydrateCall(config)

		// now add dependencies (we have already checked for circular dependencies so recursion is fine
		for _, dep := range config.Depends {
			c.Add(dep)
		}
	}
}

func (c requiredHydrateCallBuilder) Get() []*HydrateCall {
	var res []*HydrateCall
	for _, call := range c.requiredHydrateCalls {
		res = append(res, call)
	}
	return res
}
