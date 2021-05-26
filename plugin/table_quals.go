package plugin

import (
	"log"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// detect whether the query is requesting a single item by its key
// (e.g.  select * from aws_s3_bucket where name = 'turbot-166014743106-eu-west-2';)
// if so return the quals values
func (t *Table) getKeyColumnQuals(d *QueryData, keyColumns *KeyColumnSet) map[string]*proto.QualValue {
	for _, c := range t.Columns {
		if qual, ok := d.singleEqualsQual(c.Name); ok {
			d.equalsQuals[c.Name] = qual.GetValue()
		}
	}

	// KeyColumns is either string or []string
	if keyColumn := keyColumns.Single; keyColumn != "" {
		return t.singleKeyQuals(d, keyColumn)
	}
	if keyColumns := keyColumns.All; len(keyColumns) != 0 {
		return t.multiKeyQuals(d, keyColumns)
	}

	if keyColumns := keyColumns.Any; len(keyColumns) != 0 {
		for _, keyColumn := range keyColumns {
			if quals := t.singleKeyQuals(d, keyColumn); quals != nil {
				return quals
			}
		}
	}

	return nil
}

// return true is there is a single equals qual for the key column
// NOTE: the qual value for this qual may be an array of values
// - we still return 'true' in this case as for single key column tables,
// we can still treat this as a get call
//
func (t *Table) singleKeyQuals(d *QueryData, keyColumn string) map[string]*proto.QualValue {
	log.Printf("[TRACE] checking whether keyColumn: %s has a single '=' qual\n", keyColumn)

	if qualValue, ok := d.equalsQuals[keyColumn]; ok {
		log.Printf("[TRACE] singleKeyQuals TRUE, keyColumn: %s, qual: %v\n", keyColumn, qualValue)
		return map[string]*proto.QualValue{keyColumn: qualValue}
	}
	log.Printf("[TRACE] singleKeyQuals FALSE - not a get call")
	// this is not a get call
	return nil
}

func (t *Table) multiKeyQuals(d *QueryData, keyColumns []string) map[string]*proto.QualValue {
	// so a list of key column selections are specified.
	keyValues := make(map[string]*proto.QualValue)
	for _, keyColumn := range keyColumns {
		if qualValue, ok := d.equalsQuals[keyColumn]; ok {
			// NOTE: if there is a list of qual values for any of the key column, this is not a get
			// (we do not support lists of qual values for multiple key columns)
			if qualValue.GetListValue() != nil {
				return nil
			}
			keyValues[keyColumn] = qualValue

		} else {
			// quals not satisfied
			return nil
		}
	}
	// to get this far, all key columns must have a single qual so this is a get call
	return keyValues
}

// ColumnQualValue converts a qual value into the underlying value
// - based on out knowledge of the column type which the qual is applying to
func ColumnQualValue(qv *proto.QualValue, column *Column) interface{} {
	switch column.Type {
	case proto.ColumnType_BOOL:
		return qv.GetBoolValue()
	case proto.ColumnType_INT:
		return qv.GetInt64Value()
	case proto.ColumnType_DOUBLE:
		return qv.GetDoubleValue()
	case proto.ColumnType_STRING:
		return qv.GetStringValue()
	case proto.ColumnType_DATETIME, proto.ColumnType_TIMESTAMP:
		return qv.GetTimestampValue()
	case proto.ColumnType_JSON:
		return qv.GetJsonbValue()
	case proto.ColumnType_IPADDR, proto.ColumnType_CIDR: //proto.ColumnType_INET:
		return qv.GetInetValue()
	default:
		return nil
	}
}
