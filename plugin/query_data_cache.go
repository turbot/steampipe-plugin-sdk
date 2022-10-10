package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"sort"
)

// inspect the result row to build a full list of columns
func (d *QueryData) buildColumnsFromRow(row *proto.Row, columns []string) []string {
	for col := range row.Columns {
		if !helpers.StringSliceContains(columns, col) {
			columns = append(columns, col)
		}
	}
	sort.Strings(columns)
	return columns
}
