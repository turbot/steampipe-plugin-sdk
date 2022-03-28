package plugin

// HydrateDependencies defines the hydrate function dependencies - other hydrate functions which must be run first
// Deprecated: used HydrateConfig
type HydrateDependencies struct {
	Func    HydrateFunc
	Depends []HydrateFunc
}
