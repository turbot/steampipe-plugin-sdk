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
		schema.GetCallKeyColumns = &proto.KeyColumnsSet{
			Single: t.Get.KeyColumns.Single,
			All:    t.Get.KeyColumns.All,
			Any:    t.Get.KeyColumns.Any,
		}
	}
	if t.List != nil {
		if t.List.KeyColumns != nil {
			schema.ListCallKeyColumns = &proto.KeyColumnsSet{
				Single: t.List.KeyColumns.Single,
				All:    t.List.KeyColumns.All,
				Any:    t.List.KeyColumns.Any,
			}
		}
		if t.List.OptionalKeyColumns != nil {
			schema.ListCallOptionalKeyColumns = &proto.KeyColumnsSet{
				Single: t.List.OptionalKeyColumns.Single,
				All:    t.List.OptionalKeyColumns.All,
				Any:    t.List.OptionalKeyColumns.Any,
			}

		}
	}

	return schema
}
