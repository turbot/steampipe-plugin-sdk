package plugin

import "testing"

// initialise is called with a nil table for plugin-level (as opposed to
// table-level) HydrateConfig entries - see Plugin.buildHydrateConfigMap.
// It must not dereference table.
func TestHydrateConfigInitialiseNilTable(t *testing.T) {
	c := &HydrateConfig{Func: hydrate1}
	c.initialise(nil)

	if c.RetryConfig == nil {
		t.Error("expected RetryConfig to be defaulted")
	}
	if c.IgnoreConfig == nil {
		t.Error("expected IgnoreConfig to be defaulted")
	}
	if c.namedHydrate.Name == "" {
		t.Error("expected namedHydrate to be populated")
	}
}
