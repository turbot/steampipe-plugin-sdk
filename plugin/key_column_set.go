package plugin

import (
	"fmt"
	"strings"

	pb "github.com/turbotio/steampipe-plugin-sdk/grpc/proto"
)

// KeyColumnSet :: a set of columns which form the key of a table (i.e. may be used to get a single item)
// may specify:
// - a Single column
// - a set of columns which together All form the key
// - a set of columns Any of which which form the key
type KeyColumnSet struct {
	Single string
	All    []string
	Any    []string
}

func (k *KeyColumnSet) ToString() string {
	if k.Single != "" {
		return fmt.Sprintf("column: %s", k.Single)
	}
	if k.All != nil {
		return fmt.Sprintf("all columns: %s", strings.Join(k.All, ","))
	}
	if k.Any != nil {
		return fmt.Sprintf("one of columns: %s", strings.Join(k.Any, ","))
	}
	return ""
}

func (t Table) GetSchema() *pb.TableSchema {
	schema := &pb.TableSchema{
		Columns:     make([]*pb.ColumnDefinition, len(t.Columns)),
		Description: t.Description,
	}
	for i, column := range t.Columns {
		schema.Columns[i] = &pb.ColumnDefinition{
			Name:        column.Name,
			Type:        column.Type,
			Description: column.Description,
		}
	}

	return schema
}

func SingleColumn(column string) *KeyColumnSet {
	return &KeyColumnSet{Single: column}
}

func AllColumns(columns []string) *KeyColumnSet {
	return &KeyColumnSet{All: columns}
}

func AnyColumn(columns []string) *KeyColumnSet {
	return &KeyColumnSet{Any: columns}
}
