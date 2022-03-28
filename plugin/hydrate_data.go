package plugin

// HydrateData contains the input data passed to every hydrate function
type HydrateData struct {
	// if there was a parent-child list call, store the parent list item
	ParentItem     interface{}
	Item           interface{}
	HydrateResults map[string]interface{}
}
