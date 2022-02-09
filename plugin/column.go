package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

// Column is a struct representing a column defintion
// it is not mutated and contains column data, in a format compatible with proto ColumnDefinition
type Column struct {
	// column name
	Name string
	// column type
	Type proto.ColumnType
	// column description
	Description string
	// explicitly specify the function which populates this data
	// - this is only needed if any of the default hydrate functions wil NOT return this column
	Hydrate HydrateFunc
	// the default column value
	Default interface{}
	//  a list of transforms to generate the column value
	Transform *transform.ColumnTransforms
}

// QueryColumn is struct storing column name and resolved hydrate name
// this is used in the query data when the hydrate funciton has been resolved
type QueryColumn struct {
	*Column
	// the name of the hydrate function which will be used to populate this column
	// - this may be a default hydrate function
	hydrateName string
}

func NewQueryColumn(column *Column, hydrateName string) *QueryColumn {
	return &QueryColumn{column, hydrateName}
}
