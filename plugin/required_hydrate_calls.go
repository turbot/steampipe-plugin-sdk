package plugin

import (
	"log"
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

func (c requiredHydrateCallBuilder) Add(hydrateFunc namedHydrateFunc, callId string) error {
	hydrateName := hydrateFunc.Name

	// if the resolved hydrate call is NOT the same as the fetch call, add to the map of hydrate functions to call
	if hydrateName != c.fetchCallName {
		if _, ok := c.requiredHydrateCalls[hydrateName]; ok {
			return nil
		}

		// get the config for this hydrate function
		config := getHydrateConfig(c, hydrateName)

		call, err := newHydrateCall(config, c.queryData)
		if err != nil {
			log.Printf("[WARN] failed to add a hydrate call for %s: %s", hydrateName, err.Error())
			return err
		}
		c.requiredHydrateCalls[hydrateName] = call

		// now add dependencies (we have already checked for circular dependencies so recursion is fine
		for _, dep := range config.Depends {
			namedDep := newNamedHydrateFunc(dep)
			if err := c.Add(namedDep, callId); err != nil {
				log.Printf("[WARN] failed to add a hydrate call for %s, which is a dependency of %s: %s", namedDep, hydrateName, err.Error())
				return err
			}
		}
	}
	return nil
}

func getHydrateConfig(c requiredHydrateCallBuilder, hydrateName string) *HydrateConfig {
	// first try plugin
	config := c.queryData.plugin.hydrateConfigMap[hydrateName]
	// fall back on table - we know there will at least be implicit config
	if config == nil {
		config = c.queryData.Table.hydrateConfigMap[hydrateName]
	}
	return config
}

func (c requiredHydrateCallBuilder) Get() []*hydrateCall {
	var res []*hydrateCall
	for _, call := range c.requiredHydrateCalls {
		res = append(res, call)
	}

	return res
}
