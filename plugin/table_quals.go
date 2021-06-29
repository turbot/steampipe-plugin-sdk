package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

//
//// detect whether the query is requesting a single item by its key
//// (e.g.  select * from aws_s3_bucket where name = 'turbot-166014743106-eu-west-2';)
//// if so return the quals values
//func (t *Table) buildGetKeyColumnsQuals(d *QueryData, keyColumns *KeyColumnSet) map[string]*proto.QualValue {
//	for _, c := range t.Columns {
//		if qual, ok := singleEqualsQual(c.Name, d.QueryContext.RawQuals); ok {
//			d.equalsQuals[c.Name] = qual.GetValue()
//		}
//	}
//	// KeyColumns is either string or []string
//	if keyColumn := keyColumns.Single; keyColumn != nil {
//		return t.singleGetKeyQual(d, keyColumn)
//	}
//	if keyColumns := keyColumns.All; len(keyColumns) != 0 {
//		return t.multiGetKeyQuals(d, keyColumns, true)
//	}
//	if keyColumns := keyColumns.Any; len(keyColumns) != 0 {
//		return t.multiGetKeyQuals(d, keyColumns, false)
//	}
//	return nil
//}
//
//// build a map of column name to list key column qual
//func (t *Table) buildListKeyColumnsQuals(d *QueryData, keyColumns *KeyColumnSet) KeyColumnQualValueMap {
//	log.Printf("[WARN] buildListKeyColumnsQuals")
//
//	// KeyColumns is either string or []string
//	if col := keyColumns.Single; col != nil {
//		log.Printf("[WARN] keyColumns.Single")
//		res := NewKeyColumnQualValueMap(d.QueryContext.RawQuals, []*KeyColumn{keyColumns.Single})
//
//		quals, ok := getMatchingQuals(col, d.QueryContext.RawQuals)
//		if quals != nil {
//			res := KeyColumnQualValueMap{
//				col.Name: &KeyColumnQuals{
//					KeyColumn: col,
//					Quals:     []*proto.QualValue{qual.Value},
//				},
//			}
//			log.Printf("[WARN] 'Single' key column qual exists  %s", res)
//			return res
//		}
//		log.Printf("[WARN] 'Single' key column qual not found")
//
//		return nil
//	}
//
//	if cols := keyColumns.All; len(cols) != 0 {
//		log.Printf("[WARN] keyColumns.All")
//		res := NewKeyColumnQualValueMap(d.QueryContext.RawQuals, cols)
//		// we must have all key columns
//		if len(res) == len(cols) {
//			log.Printf("[WARN] 'All' key column qual exists  %s", res)
//			return res
//		}
//		log.Printf("[WARN] 'All' key column qual not found")
//		return nil
//	}
//
//	cols -> ke
//	if cols := keyColumns.Any; len(cols) != 0 {
//		log.Printf("[WARN] keyColumns.Any")
//		// if the map has any entries, the Any requirement is satisfied
//		res := NewKeyColumnQualValueMap(d.QueryContext.RawQuals, cols)
//		if len(res) > 0 {
//			log.Printf("[WARN] 'Any' key column qual exists  %s", res)
//			return res
//		}
//		log.Printf("[WARN] 'All' key column qual not found")
//	}
//	return nil
//}
//
//// return true is there is a single equals qual for the key column
//// NOTE: the qual value for this qual may be an array of values
//// - we still return 'true' in this case as for single key column tables,
//// we can still treat this as a get call
////
//func (t *Table) singleGetKeyQual(d *QueryData, keyColumn *KeyColumn) map[string]*proto.QualValue {
//	log.Printf("[TRACE] checking whether keyColumn: %s has a single '=' qual\n", keyColumn)
//
//	if qualValue, ok := d.equalsQuals[keyColumn.Name]; ok {
//		log.Printf("[TRACE] singleGetKeyQual TRUE, keyColumn: %s, qual: %v\n", keyColumn, qualValue)
//		return map[string]*proto.QualValue{keyColumn.Name: qualValue}
//	}
//	log.Printf("[TRACE] singleGetKeyQual FALSE - not a get call")
//	// this is not a get call
//	return nil
//}
//
//func (t *Table) multiGetKeyQuals(d *QueryData, keyColumns KeyColumnSlice, allRequired bool) map[string]*proto.QualValue {
//	// so a list of key column selections are specified.
//	keyValues := make(map[string]*proto.QualValue)
//	for _, keyColumn := range keyColumns {
//		if qualValue, ok := d.equalsQuals[keyColumn.Name]; ok {
//			// NOTE: if there is a list of qual values for any of the key column, qual requirements are not satisfied
//			// (we do not support lists of qual values for multiple key columns)
//			if qualValue.GetListValue() != nil {
//				return nil
//			}
//			keyValues[keyColumn.Name] = qualValue
//		} else {
//			if allRequired {
//				// quals requirements are not satisfied
//				return nil
//			}
//		}
//	}
//	// to get this far, all key columns must have a single qual so qual requirements are satisfied
//	return keyValues
//}

// ColumnQualValue converts a qual value into the underlying value
// - based on out knowledge of the column type which the qual is applying to
// TODO is this doing basically the same thing as grpc.GetQualValue
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
