package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"

	"github.com/turbot/go-kit/helpers"
)

/*
KeyColumnQuals defines all [qualifiers] for a column.

Use it in a [table definition], by way of the [plugin.QueryData] object.

The query writer must specify the qualifiers, in a WHERE or a JOIN clause, in order to limit the number of API calls that Steampipe makes to satisfy the query.

	func listUser(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
		var item User
		var id string

		if h.Item != nil {
			user := h.Item.(*User)
			id = user.ID
		} else {
			quals := d.KeyColumnQuals
			id = quals["id"].GetStringValue()
		}
		...
	}

Examples:

  - [hackernews]

[qualifiers]: https://steampipe.io/docs/develop/writing-plugins#qualifiers
[table definition]: https://steampipe.io/docs/develop/writing-plugins#table-definition
[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/bbfbb12751ad43a2ca0ab70901cde6a88e92cf44/hackernews/table_hackernews_user.go#L40
*/
type KeyColumnQuals struct {
	Name  string
	Quals quals.QualSlice
}

func (k KeyColumnQuals) SatisfiesKeyColumn(keyColumn *KeyColumn) bool {
	if keyColumn.Name != k.Name {
		return false
	}
	for _, q := range k.Quals {
		if helpers.StringSliceContains(keyColumn.Operators, q.Operator) {
			return true
		}
	}
	return false
}

func (k KeyColumnQuals) SingleEqualsQual() bool {
	return k.Quals.SingleEqualsQual()
}
