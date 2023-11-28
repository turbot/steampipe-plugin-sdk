package plugin

import (
	"context"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"log"
)

// get the column object with the given name
func (t *Table) getColumn(columnName string) *Column {
	for _, c := range t.Columns {
		if c.Name == columnName {
			return c
		}
	}
	return nil
}

// get the type of the column the given name
func (t *Table) getColumnType(columnName string) proto.ColumnType {
	if column := t.getColumn(columnName); column != nil {
		return column.Type
	}
	return proto.ColumnType_UNKNOWN
}

// take the raw value returned by the get/list/hydrate call, apply transforms and convert to protobuf value
func (t *Table) getColumnValue(ctx context.Context, rowData *rowData, column *QueryColumn) (*proto.Column, error) {
	hydrateItem, err := rowData.GetColumnData(column)
	if err != nil {
		log.Printf("[ERROR] table '%s' failed to get column data: %s (%s)", t.Name, err.Error(), rowData.queryData.connectionCallId)
		return nil, err
	}

	var value interface{} = nil
	// only call transforms if the hydrate item is non nil
	if !helpers.IsNil(hydrateItem) {
		// are there any generate transforms defined? if not apply default generate
		// NOTE: we must call getColumnTransforms to ensure the default is used if none is defined
		columnTransforms := t.getColumnTransforms(column)

		qualValueMap := rowData.queryData.Quals.ToQualMap()
		transformData := &transform.TransformData{
			HydrateItem:    hydrateItem,
			HydrateResults: rowData.hydrateResults,
			ColumnName:     column.Name,
			KeyColumnQuals: qualValueMap,
		}
		value, err = columnTransforms.Execute(ctx, transformData)
		if err != nil {
			log.Printf("[ERROR] failed to populate column '%s': %v\n", column.Name, err)
			return nil, fmt.Errorf("failed to populate column '%s': %v", column.Name, err)
		}
	}
	// now convert the value to a protobuf column value
	c, err := column.ToColumnValue(value)
	//c, err := proto.InterfaceToColumnValue(column.Column.Name, column.Column.Type, column.Column.Default, value)
	return c, err
}

// if there are any column transforms defined return them
// otherwise return either the table default (if it exists) or the base default Transform function
func (t *Table) getColumnTransforms(column *QueryColumn) *transform.ColumnTransforms {
	columnTransform := column.Transform
	if columnTransform == nil {
		columnTransform = t.getDefaultColumnTransform(column)
	}
	return columnTransform
}

func (t *Table) getDefaultColumnTransform(column *QueryColumn) *transform.ColumnTransforms {
	var columnTransform *transform.ColumnTransforms
	if defaultTransform := t.DefaultTransform; defaultTransform != nil {
		//did the table define a default transform
		columnTransform = defaultTransform
	} else if defaultTransform = t.Plugin.DefaultTransform; defaultTransform != nil {
		// maybe the plugin defined a default transform
		columnTransform = defaultTransform
	} else {
		// no table or plugin defined default transform - use the base default implementation
		// (just returning the field corresponding to the column name)
		columnTransform = &transform.ColumnTransforms{Transforms: []*transform.TransformCall{{Transform: transform.FieldValue, Param: column.Name}}}
	}
	return columnTransform
}
