package plugin

import (
	"slices"
	"sort"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// inspect the result row to build a full list of columns
func (d *QueryData) buildColumnsFromRow(row *proto.Row, columns []string) []string {
	for col := range row.Columns {
		if !slices.Contains(columns, col) {
			columns = append(columns, col)
		}
	}
	sort.Strings(columns)
	return columns
}
