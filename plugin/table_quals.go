package plugin

import (
	"log"

	pb "github.com/turbotio/steampipe-plugin-sdk/grpc/proto"
)

// detect whether the query is requesting a single item by its key
// (e.g.  select * from aws_s3_bucket where name = 'turbot-166014743106-eu-west-2';)
// if so return the quals values
func (t *Table) getKeyColumnQuals(d *QueryData, keyColumns *KeyColumnSet) map[string]*pb.QualValue {
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
func (t *Table) singleKeyQuals(d *QueryData, keyColumn string) map[string]*pb.QualValue {
	log.Printf("[DEBUG] checking whether keyColumn: %s has a single '=' qual\n", keyColumn)

	if qual, ok := d.singleEqualsQual(keyColumn); ok {
		log.Printf("[DEBUG] singleKeyQuals TRUE, keyColumn: %s, qual: %v\n", keyColumn, qual)
		return map[string]*pb.QualValue{keyColumn: qual.GetValue()}
	}
	log.Printf("[DEBUG] singleKeyQuals FALSE - not a get call")
	// this is not a get call
	return nil
}

func (t *Table) multiKeyQuals(d *QueryData, keyColumns []string) map[string]*pb.QualValue {
	// so a list of key column selections are specified.
	keyValues := make(map[string]*pb.QualValue)
	for _, keyColumn := range keyColumns {
		if qual, ok := d.singleEqualsQual(keyColumn); ok {
			qualValue := qual.GetValue()
			// NOTE: if there is a list of qual values for any of the key column, this is not a get
			// (we do not support lists of qual values for multiple key columns)
			if qualValue.GetListValue() != nil {
				return nil
			}
			keyValues[keyColumn] = qual.GetValue()

		} else {
			// this is not a get call
			return nil
		}
	}
	// to get this far, all key columns must have a single qual so this is a get call
	return keyValues
}
