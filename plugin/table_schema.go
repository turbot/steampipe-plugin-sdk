package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"strings"
)

const contextColumnName = "_ctx"
const spcReservedColumnPrefix = "_spc_"

func IsReservedColumnName(columnName string) bool {
	return columnName == contextColumnName || strings.HasPrefix(columnName, spcReservedColumnPrefix)
}

func contextColumnDefinition() *proto.ColumnDefinition {
	return &proto.ColumnDefinition{
		Name:        contextColumnName,
		Type:        proto.ColumnType_JSON,
		Description: "Steampipe context in JSON form, e.g. connection_name.",
	}
}

// GetSchema returns the [proto.TableSchema], which defines the columns returned by the table.
//
// Note: an additional '_ctx' column is added to all table schemas. This contains Steampipe specific data.
// (Currently this is populated with the connection name.)
func (t *Table) GetSchema() (*proto.TableSchema, error) {
	schema := &proto.TableSchema{
		Columns:     make([]*proto.ColumnDefinition, 0, len(t.Columns)+1),
		Description: t.Description,
	}

	// NOTE: we add a column "_ctx" to all tables.
	// This is therefore a reserved column name
	// column schema
	for _, column := range t.Columns {
		// if this is NOT a reserved name, add
		if !IsReservedColumnName(column.Name) {
			columnDef := &proto.ColumnDefinition{
				Name:        column.Name,
				Type:        column.Type,
				Description: column.Description,
			}
			schema.Columns = append(schema.Columns, columnDef)
		}
	}
	// add _ctx column
	schema.Columns = append(schema.Columns, contextColumnDefinition())

	// key columns
	if t.Get != nil && len(t.Get.KeyColumns) > 0 {
		schema.GetCallKeyColumnList = t.Get.KeyColumns.ToProtobuf()
	}
	if t.List != nil {
		if len(t.List.KeyColumns) > 0 {
			schema.ListCallKeyColumnList = t.List.KeyColumns.ToProtobuf()
		}
	}

	return schema, nil
}
