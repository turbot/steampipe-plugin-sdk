package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

// Column is a struct representing a column defintion
// it is not mutated and contains column data, in a format compatible with proto ColumnDefinition

/* 
Column is a struct representing a column definition.
It is not mutated and contains column data in a format compatible with [proto.ColumnDefinition].

Example from [hackernews]:

[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/item.go#L14

func itemCols() []*plugin.Column {
    return []*plugin.Column{
        {Name: "id", Type: proto.ColumnType_INT, Description: "The item's unique id."},
        {Name: "title", Type: proto.ColumnType_STRING, Hydrate: getItem, Description: "The title of the story, poll or job. HTML."},
    }
}

—

*/
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
