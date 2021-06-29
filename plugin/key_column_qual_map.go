package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin/quals"
)

// KeyColumnQualMap is a map of KeyColumnQuals keyed by column name
type KeyColumnQualMap map[string]*KeyColumnQuals

// ToEqualsQualValueMap converts a KeyColumnQualMap to a column-qual value map, including only the
func (m KeyColumnQualMap) ToEqualsQualValueMap() map[string]*proto.QualValue {
	res := make(map[string]*proto.QualValue, len(m))
	for k, v := range m {
		if v.SingleEqualsQual() {
			res[k] = v.Quals[0].Value
		}
	}
	return res
}

func (m KeyColumnQualMap) String() string {
	strs := make([]string, len(m))
	for _, k := range m {
		var values = make([]interface{}, len(k.Quals))
		for i, v := range k.Quals {
			values[i] = v.Value
		}
		strs = append(strs, fmt.Sprintf("%s - %v", k.Column, values))
	}
	return strings.Join(strs, "\n")
}

func (m KeyColumnQualMap) SatisfiesKeyColumns(keyColumns *KeyColumnSet) bool {
	log.Printf("[WARN] SatisfiesKeyColumns")
	if keyColumns == nil {
		return true
	}
	if keyColumns.Single != nil {
		log.Printf("[WARN] columns.Single")
		k := m[keyColumns.Single.Column]
		return k != nil && k.SatisfiesKeyColumn(keyColumns.Single)
	}
	if keyColumns.Any != nil {
		log.Printf("[WARN] columns.Any")
		for _, k := range keyColumns.Any {
			q := m[k.Column]
			if q != nil && q.SatisfiesKeyColumn(k) {
				return true
			}
		}
	}
	if keyColumns.All != nil {
		log.Printf("[WARN] columns.All")
		for _, k := range keyColumns.All {
			q := m[k.Column]
			if q == nil || !q.SatisfiesKeyColumn(k) {
				log.Printf("[WARN] quals %v does not satisfy key column %v", q, k)
				return false
			}
		}
	}
	log.Printf("[WARN] I'm satisfied")
	return true
}

// ToQualMap converts the map into a simpler map of column to []Quals
// this is needed to avoid the transform package needing to reference plugin
func (m KeyColumnQualMap) ToQualMap() map[string][]*quals.Qual {
	var res = make(map[string][]*quals.Qual)
	for k, v := range m {
		res[k] = v.Quals
	}
	return res
}

// NewKeyColumnQualValueMap creates a KeyColumnQualMap from one or more KeyColumnSets
func NewKeyColumnQualValueMap(qualMap map[string]*proto.Quals, keyColumnSets ...*KeyColumnSet) KeyColumnQualMap {
	res := KeyColumnQualMap{}
	for _, keyColumns := range keyColumnSets {
		// extract the key columns slice from the set
		slice := keyColumns.ToKeyColumnSlice()
		for _, col := range slice {
			matchingQuals := getMatchingQuals(col, qualMap)
			for _, q := range matchingQuals {
				// convert proto.Qual into a qual.Qual (which is easier to use)
				qual := quals.NewQual(q)

				// if there is already an entry for this column, add a value to the array
				if mapEntry, mapEntryExists := res[col.Column]; mapEntryExists {
					mapEntry.Quals = append(mapEntry.Quals, qual)
					res[col.Column] = mapEntry
				} else {
					// crate a new map entry for this column
					res[col.Column] = &KeyColumnQuals{
						Column: col.Column,
						Quals:  []*quals.Qual{qual},
					}
				}
			}

		}
	}
	return res
}

// look in a column-qual map for quals with column and operator matching the key column
func getMatchingQuals(keyColumn *KeyColumn, qualMap map[string]*proto.Quals) []*proto.Qual {
	log.Printf("[WARN] getMatchingQuals keyColumn %s qualMap %s", keyColumn, qualMap)

	quals, ok := qualMap[keyColumn.Column]
	if !ok {
		log.Printf("[WARN] getMatchingQuals returning false - qualMap does not contain any quals for colums %s", keyColumn.Column)
		return nil
	}

	var res []*proto.Qual
	for _, q := range quals.Quals {
		operator := q.GetStringValue()
		if helpers.StringSliceContains(keyColumn.Operators, operator) {
			res = append(res, q)
		}
	}
	if len(res) > 0 {
		log.Printf("[WARN] getMatchingQuals found %d quals matching key column %s", len(res), keyColumn)
	} else {
		log.Printf("[WARN] getMatchingQuals returning false - qualMap does not contain any matching quals for quals for key column %s", keyColumn)
	}

	return res
}
