package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/golang/protobuf/ptypes"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
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
func (t *Table) getColumnValue(ctx context.Context, rowData *RowData, column *QueryColumn) (*proto.Column, error) {
	hydrateItem, err := rowData.GetColumnData(column)
	if err != nil {
		log.Printf("[ERROR] table '%s' failed to get column data, callId %s: %v", t.Name, rowData.queryData.callId, err)
		return nil, err
	}
	// are there any generate transforms defined? if not apply default generate
	// NOTE: we must call getColumnTransforms to ensure the default is used if none is defined
	columnTransforms := t.getColumnTransforms(column)
	defaultTransform := t.getDefaultColumnTransform(column)

	qualValueMap := rowData.queryData.Quals.ToQualMap()
	transformData := &transform.TransformData{
		HydrateItem:    hydrateItem,
		HydrateResults: rowData.hydrateResults,
		ColumnName:     column.Name,
		KeyColumnQuals: qualValueMap,
	}
	value, err := columnTransforms.Execute(ctx, transformData, defaultTransform)
	if err != nil {
		log.Printf("[ERROR] failed to populate column '%s': %v\n", column.Name, err)
		return nil, fmt.Errorf("failed to populate column '%s': %v", column.Name, err)
	}
	// now convert the value to a protobuf column value
	c, err := t.interfaceToColumnValue(column, value)
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

// convert a value of unknown type to a valid protobuf column value.type
func (t *Table) interfaceToColumnValue(column *QueryColumn, val interface{}) (*proto.Column, error) {
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Errorf("%s: %v", column.Name, r))
		}
	}()

	// if the value is a pointer, get its value and use that
	val = helpers.DereferencePointer(val)
	if val == nil {
		if column.Default != nil {
			val = column.Default
		} else {
			// return nil
			return &proto.Column{Value: &proto.Column_NullValue{}}, nil
		}
	}

	var columnValue *proto.Column

	switch column.Type {
	case proto.ColumnType_STRING:
		columnValue = &proto.Column{Value: &proto.Column_StringValue{StringValue: types.ToString(val)}}
		break
	case proto.ColumnType_BOOL:
		b, err := types.ToBool(val)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", column.Name, err)
		}
		columnValue = &proto.Column{Value: &proto.Column_BoolValue{BoolValue: b}}
		break
	case proto.ColumnType_INT:
		i, err := types.ToInt64(val)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", column.Name, err)
		}

		columnValue = &proto.Column{Value: &proto.Column_IntValue{IntValue: i}}
		break
	case proto.ColumnType_DOUBLE:
		d, err := types.ToFloat64(val)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", column.Name, err)
		}
		columnValue = &proto.Column{Value: &proto.Column_DoubleValue{DoubleValue: d}}
		break
	case proto.ColumnType_JSON:
		strValue, ok := val.(string)
		if ok {
			// NOTE: Strings are assumed to be raw JSON, so are passed through directly.
			// This is the most common case, but means it's currently impossible to
			// pass through a string and have it marshalled to be a JSON representation
			// of a string.
			columnValue = &proto.Column{Value: &proto.Column_JsonValue{JsonValue: []byte(strValue)}}
		} else {
			res, err := json.Marshal(val)
			if err != nil {
				log.Printf("[ERROR] failed to marshal value to json: %v\n", err)
				return nil, fmt.Errorf("%s: %v ", column.Name, err)
			}
			columnValue = &proto.Column{Value: &proto.Column_JsonValue{JsonValue: res}}
		}
	case proto.ColumnType_DATETIME, proto.ColumnType_TIMESTAMP:
		// cast val to time
		var timeVal, err = types.ToTime(val)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", column.Name, err)
		}
		// now convert time to protobuf timestamp
		timestamp, err := ptypes.TimestampProto(timeVal)
		if err != nil {
			return nil, fmt.Errorf("interfaceToColumnValue failed for column '%s': %v", column.Name, err)
		}
		columnValue = &proto.Column{Value: &proto.Column_TimestampValue{TimestampValue: timestamp}}
		break
	case proto.ColumnType_IPADDR:
		ipString := types.SafeString(val)
		// treat an empty string as a null ip address
		if ipString == "" {
			columnValue = &proto.Column{Value: &proto.Column_NullValue{}}
		} else {
			if ip := net.ParseIP(ipString); ip == nil {
				return nil, fmt.Errorf("%s: invalid ip address %s", column.Name, ipString)
			}
			columnValue = &proto.Column{Value: &proto.Column_IpAddrValue{IpAddrValue: ipString}}
		}
		break
	case proto.ColumnType_CIDR:
		cidrRangeString := types.SafeString(val)
		// treat an empty string as a null ip address
		if cidrRangeString == "" {
			columnValue = &proto.Column{Value: &proto.Column_NullValue{}}
		} else {
			if _, _, err := net.ParseCIDR(cidrRangeString); err != nil {
				return nil, fmt.Errorf("%s: invalid ip address %s", column.Name, cidrRangeString)
			}
			columnValue = &proto.Column{Value: &proto.Column_CidrRangeValue{CidrRangeValue: cidrRangeString}}
		}
		break
	case proto.ColumnType_LTREE:
		columnValue = &proto.Column{Value: &proto.Column_LtreeValue{LtreeValue: types.ToString(val)}}
		break

	default:
		return nil, fmt.Errorf("unrecognised columnValue type '%s'", column.Type)
	}

	return columnValue, nil

}
