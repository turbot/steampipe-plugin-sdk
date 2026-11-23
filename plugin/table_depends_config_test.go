package plugin

import (
	"testing"

	"github.com/hashicorp/go-hclog"
)

// a function named only in a Depends list gets a hydrate config, one which already has config keeps it (issue #969)
func TestHydrateConfigMapIncludesDependencies(t *testing.T) {
	tests := map[string]struct {
		pluginConfig []HydrateConfig
		table        *Table
		// names which must have a config in the table's hydrate config map
		wantConfigs []string
		// names whose config must be the one declared on the table (same pointer as its HydrateConfig entry)
		wantKept []string
		// names which must have no config (fetch calls)
		wantAbsent []string
	}{
		"dependency named only in Depends": {
			table: &Table{
				Name:          "t",
				List:          &ListConfig{Hydrate: listHydrate},
				Columns:       []*Column{{Name: "c1", Hydrate: hydrate2}},
				HydrateConfig: []HydrateConfig{{Func: hydrate2, Depends: []HydrateFunc{hydrate1}}},
			},
			wantConfigs: []string{"hydrate1", "hydrate2"},
		},
		"dependency named only in legacy HydrateDependencies": {
			table: &Table{
				Name:                "t",
				List:                &ListConfig{Hydrate: listHydrate},
				Columns:             []*Column{{Name: "c1", Hydrate: hydrate2}},
				HydrateDependencies: []HydrateDependencies{{Func: hydrate2, Depends: []HydrateFunc{hydrate1}}},
			},
			wantConfigs: []string{"hydrate1", "hydrate2"},
		},
		"chain of dependencies, none with config": {
			table: &Table{
				Name:    "t",
				List:    &ListConfig{Hydrate: listHydrate},
				Columns: []*Column{{Name: "c1", Hydrate: hydrate3}},
				HydrateConfig: []HydrateConfig{
					{Func: hydrate3, Depends: []HydrateFunc{hydrate2}},
					{Func: hydrate2, Depends: []HydrateFunc{hydrate1}},
				},
			},
			wantConfigs: []string{"hydrate1", "hydrate2", "hydrate3"},
			wantKept:    []string{"hydrate2", "hydrate3"},
		},
		"dependency with its own config keeps it": {
			table: &Table{
				Name:    "t",
				List:    &ListConfig{Hydrate: listHydrate},
				Columns: []*Column{{Name: "c1", Hydrate: hydrate2}},
				HydrateConfig: []HydrateConfig{
					{Func: hydrate2, Depends: []HydrateFunc{hydrate1}},
					{Func: hydrate1, Tags: map[string]string{"own": "config"}},
				},
			},
			wantConfigs: []string{"hydrate1", "hydrate2"},
			wantKept:    []string{"hydrate1", "hydrate2"},
		},
		"dependency which is a column's hydrate keeps its config": {
			table: &Table{
				Name:          "t",
				List:          &ListConfig{Hydrate: listHydrate},
				Columns:       []*Column{{Name: "c1", Hydrate: hydrate2}, {Name: "c2", Hydrate: hydrate1}},
				HydrateConfig: []HydrateConfig{{Func: hydrate2, Depends: []HydrateFunc{hydrate1}}},
			},
			wantConfigs: []string{"hydrate1", "hydrate2"},
			wantKept:    []string{"hydrate2"},
		},
		"dependency of a plugin level config": {
			pluginConfig: []HydrateConfig{{Func: hydrate2, Depends: []HydrateFunc{hydrate1}}},
			table: &Table{
				Name:    "t",
				List:    &ListConfig{Hydrate: listHydrate},
				Columns: []*Column{{Name: "c1", Hydrate: hydrate2}},
			},
			wantConfigs: []string{"hydrate1", "hydrate2"},
		},
		"dependency which is the list call gets no config": {
			table: &Table{
				Name:          "t",
				List:          &ListConfig{Hydrate: listHydrate},
				Columns:       []*Column{{Name: "c1", Hydrate: hydrate2}},
				HydrateConfig: []HydrateConfig{{Func: hydrate2, Depends: []HydrateFunc{listHydrate}}},
			},
			wantConfigs: []string{"hydrate2"},
			wantAbsent:  []string{"listHydrate"},
		},
		"dependency which is the parent list call gets no config": {
			table: &Table{
				Name:          "t",
				List:          &ListConfig{Hydrate: listHydrate, ParentHydrate: hydrate3},
				Columns:       []*Column{{Name: "c1", Hydrate: hydrate2}},
				HydrateConfig: []HydrateConfig{{Func: hydrate2, Depends: []HydrateFunc{hydrate3}}},
			},
			wantConfigs: []string{"hydrate2"},
			wantAbsent:  []string{"hydrate3"},
		},
		"dependency with a plugin level config": {
			pluginConfig: []HydrateConfig{{Func: hydrate1, Tags: map[string]string{"own": "config"}}},
			table: &Table{
				Name:          "t",
				List:          &ListConfig{Hydrate: listHydrate},
				Columns:       []*Column{{Name: "c1", Hydrate: hydrate2}},
				HydrateConfig: []HydrateConfig{{Func: hydrate2, Depends: []HydrateFunc{hydrate1}}},
			},
			wantConfigs: []string{"hydrate2"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			p := &Plugin{HydrateConfig: test.pluginConfig}
			p.initialise(hclog.NewNullLogger())
			p.buildHydrateConfigMap()
			test.table.initialise(p)

			for _, n := range test.wantConfigs {
				if test.table.hydrateConfigMap[n] == nil {
					t.Errorf("no hydrate config for %s", n)
				}
			}
			for _, n := range test.wantAbsent {
				if test.table.hydrateConfigMap[n] != nil {
					t.Errorf("unexpected hydrate config for %s", n)
				}
			}
			for _, n := range test.wantKept {
				var declared *HydrateConfig
				for i := range test.table.HydrateConfig {
					if test.table.HydrateConfig[i].namedHydrate.Name == n {
						declared = &test.table.HydrateConfig[i]
					}
				}
				if declared == nil || test.table.hydrateConfigMap[n] != declared {
					t.Errorf("hydrate config for %s was replaced", n)
				}
			}
			// the dependency can be found when a query needs it
			d := &QueryData{Table: test.table, plugin: p}
			b := requiredHydrateCallBuilder{queryData: d}
			for _, n := range []string{"hydrate1", "hydrate2"} {
				if getHydrateConfig(b, n) == nil && containsString(test.wantConfigs, n) {
					t.Errorf("getHydrateConfig found nothing for %s", n)
				}
			}
			if errs := test.table.validateHydrateDependencies(); len(errs) > 0 {
				t.Errorf("unexpected validation errors: %v", errs)
			}
		})
	}
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
