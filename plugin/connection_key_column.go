package plugin

// ConnectionKeyColumn is a struct that defines a column that has a value which maps 1-1 to a Steampipe connection
// and so can be used to filter connections when executing an aggregator query
//
// These columns are treated as (optional) KeyColumns. This means they are taken into account in the query planning
// -  so it is more likely that the scans will be planned to use these as quals where possible.
//
// When executing an aggregator query, if one of these columns is used as a qual, the execution code
// will filter the list of connections by comparing the qual value with the column value returned by the
// [ConnectionKeyColumn] Hydrate function.
//
// The Hydrate function is a function that returns the value fo the column for a given connection.
type ConnectionKeyColumn struct {
	Hydrate HydrateFunc
	Name    string
}
