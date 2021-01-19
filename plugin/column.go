package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

// Column :: column data, in format compatible with proto ColumnDefinition
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
	// the name of the hydrate function which will be used to populate this column
	// - this may be a default hydrate function
	resolvedHydrateName string
	// the default column value
	Default interface{}
	//  a list of transforms to generate the column value
	Transform *transform.ColumnTransforms
}
