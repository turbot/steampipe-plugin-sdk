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
		strs = append(strs, fmt.Sprintf("%s - %v", k.Name, values))
	}
	return strings.Join(strs, "\n")
}

func (m KeyColumnQualMap) SatisfiesKeyColumns(columns KeyColumnSlice) (bool, KeyColumnSlice) {
	log.Printf("[WARN] SatisfiesKeyColumns %v", columns)

	if columns == nil {
		return true, nil
	}
	var unsatisfiedKeyColumns KeyColumnSlice
	satisfiedCount := map[string]int{
		Required: 0,
		AnyOf:    0,
		Optional: 0,
	}
	unsatisfiedCount := map[string]int{
		Required: 0,
		AnyOf:    0,
		Optional: 0,
	}

	for _, keyColumn := range columns {
		// look for this key column in our map
		k := m[keyColumn.Name]
		satisfied := k != nil && k.SatisfiesKeyColumn(keyColumn)
		if satisfied {
			satisfiedCount[keyColumn.Require]++

			log.Printf("[TRACE] key column satisfied %v", keyColumn)

		} else {
			unsatisfiedCount[keyColumn.Require]++
			unsatisfiedKeyColumns = append(unsatisfiedKeyColumns, keyColumn)
			log.Printf("[TRACE] key column NOT satisfied %v", keyColumn)
			// if this was NOT an optional key column, we are not satisfied
		}
	}

	// we are satisfied if:
	// all Required key columns are satisfied
	// either there is at least 1 satisfied AnyOf key columns, or there are no AnyOf columns
	res := unsatisfiedCount[Required] == 0 && (satisfiedCount[AnyOf] > 0 || unsatisfiedCount[AnyOf] == 0)

	log.Printf("[WARN] SatisfiesKeyColumns result: %v, satisfiedCount %v, unsatisfiedCount %v, unsatisfiedKeyColumns %v", res, satisfiedCount, unsatisfiedCount, unsatisfiedKeyColumns)
	return res, unsatisfiedKeyColumns
}

// ToQualMap converts the map into a simpler map of column to []Quals
// this is needed to avoid the transform package needing to reference plugin
func (m KeyColumnQualMap) ToQualMap() map[string]quals.QualSlice {
	var res = make(map[string]quals.QualSlice)
	for k, v := range m {
		res[k] = v.Quals
	}
	return res
}

// NewKeyColumnQualValueMap creates a KeyColumnQualMap from a qual map and a KeyColumnSlice
func NewKeyColumnQualValueMap(qualMap map[string]*proto.Quals, keyColumns KeyColumnSlice) KeyColumnQualMap {
	res := KeyColumnQualMap{}

	for _, col := range keyColumns {
		matchingQuals := getMatchingQuals(col, qualMap)
		for _, q := range matchingQuals {
			// convert proto.Qual into a qual.Qual (which is easier to use)
			qual := quals.NewQual(q)

			// if there is already an entry for this column, add a value to the array
			if mapEntry, mapEntryExists := res[col.Name]; mapEntryExists {
				mapEntry.Quals = append(mapEntry.Quals, qual)
				res[col.Name] = mapEntry
			} else {
				// crate a new map entry for this column
				res[col.Name] = &KeyColumnQuals{
					Name:  col.Name,
					Quals: quals.QualSlice{qual},
				}
			}
		}
	}
	return res
}

// look in a column-qual map for quals with column and operator matching the key column
func getMatchingQuals(keyColumn *KeyColumn, qualMap map[string]*proto.Quals) []*proto.Qual {
	log.Printf("[TRACE] getMatchingQuals keyColumn %s qualMap %s", keyColumn, qualMap)

	quals, ok := qualMap[keyColumn.Name]
	if !ok {
		log.Printf("[TRACE] getMatchingQuals returning false - qualMap does not contain any quals for colums %s", keyColumn.Name)
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
		log.Printf("[TRACE] getMatchingQuals found %d quals matching key column %s", len(res), keyColumn)
	} else {
		log.Printf("[TRACE] getMatchingQuals returning false - qualMap does not contain any matching quals for quals for key column %s", keyColumn)
	}

	return res
}
