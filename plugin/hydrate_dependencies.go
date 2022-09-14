package plugin

/*
HydrateDependencies defines the hydrate function dependencies - other hydrate functions
which must be run first.

This function is deprecated and the functionality is moved into [HydrateConfig].
*/
type HydrateDependencies struct {
	Func    HydrateFunc
	Depends []HydrateFunc
}
