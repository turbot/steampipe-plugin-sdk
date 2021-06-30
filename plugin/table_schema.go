package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func (t Table) GetSchema() *proto.TableSchema {
	schema := &proto.TableSchema{
		Columns:     make([]*proto.ColumnDefinition, len(t.Columns)),
		Description: t.Description,
	}

	// column schema
	for i, column := range t.Columns {
		schema.Columns[i] = &proto.ColumnDefinition{
			Name:        column.Name,
			Type:        column.Type,
			Description: column.Description,
		}
	}
	// key columns
	if t.Get != nil && t.Get.KeyColumns != nil {
		schema.GetCallKeyColumns = t.Get.KeyColumns.ToProtobuf()
	}
	if t.List != nil {
		if t.List.KeyColumns != nil {
			schema.ListCallKeyColumns = t.List.KeyColumns.ToProtobuf()
		}

	}

	return schema
}
