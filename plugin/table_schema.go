package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"strings"
)

const (
	deprecatedContextColumnName = "_ctx"
	contextColumnName           = "sp_ctx"
	connectionNameColumnName    = "sp_connection_name"
)

var spcReservedColumnPrefixes = []string{"_spc_", "sp_"}

func IsReservedColumnName(columnName string) bool {
	if columnName == deprecatedContextColumnName {
		return true
	}
	for _, prefix := range spcReservedColumnPrefixes {
		if strings.HasPrefix(columnName, prefix) {
			return true
		}
	}
	return false
}

func contextColumnDefinition(name string) *proto.ColumnDefinition {
	return &proto.ColumnDefinition{
		Name:        name,
		Type:        proto.ColumnType_JSON,
		Description: "Steampipe context in JSON form.",
	}
}
func connectionNameColumnDefinition() *proto.ColumnDefinition {
	return &proto.ColumnDefinition{
		Name:        connectionNameColumnName,
		Type:        proto.ColumnType_STRING,
		Description: "Steampipe connection name.",
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
			if column.Hydrate != nil {
				columnDef.Hydrate = column.namedHydrate.Name
			}
			if !helpers.IsNil(column.Default) {
				// to convert the column default to a proto.Column, call ToColumnValue with a nil value
				// - we will get the default
				def, err := column.ToColumnValue(nil)
				if err != nil {
					return nil, err
				}
				columnDef.Default = def

			}
			schema.Columns = append(schema.Columns, columnDef)
		}
	}
	// add _ctx, sp_ctx and sp_connection_name columns
	schema.Columns = append(schema.Columns, connectionNameColumnDefinition())
	schema.Columns = append(schema.Columns, contextColumnDefinition(contextColumnName))
	schema.Columns = append(schema.Columns, contextColumnDefinition(deprecatedContextColumnName))

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
