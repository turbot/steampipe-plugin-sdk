package plugin

import (
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
)

const ContextColumnName = "_ctx"

// GetSchema returns the table schema
// Note: an additional '_ctx' column is added to all table schemas. This contains Steampipe specific data.
// (currently this is populated with the connection name)
func (t Table) GetSchema() (*proto.TableSchema, error) {
	schema := &proto.TableSchema{
		Columns:     make([]*proto.ColumnDefinition, len(t.Columns)+1),
		Description: t.Description,
	}

	// NOTE: we add a column "_ctx" to all tables.
	// This is therefore a reserved column name
	// column schema
	for i, column := range t.Columns {
		if column.Name == ContextColumnName {
			return nil, fmt.Errorf("column '%s' is reserved and may not be used within a plugin schema", ContextColumnName)
		}
		schema.Columns[i] = &proto.ColumnDefinition{
			Name:        column.Name,
			Type:        column.Type,
			Description: column.Description,
		}
	}
	// add _ctx column
	schema.Columns[len(t.Columns)] = &proto.ColumnDefinition{
		Name:        ContextColumnName,
		Type:        proto.ColumnType_JSON,
		Description: "Steampipe context in JSON form, e.g. connection_name.",
	}

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
