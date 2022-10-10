package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
)

// KeyColumnQualMap is a map of [KeyColumnQuals] keyed by column name
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
	if len(m) == 0 {
		return "{}"
	}
	var strs []string
	for _, k := range m {
		for _, q := range k.Quals {
			strs = append(strs, fmt.Sprintf("%s %s %v", k.Name, q.Operator, grpc.GetQualValue(q.Value)))
		}
	}
	return strings.Join(strs, "\n")
}

func (m KeyColumnQualMap) GetUnsatisfiedKeyColumns(columns KeyColumnSlice) KeyColumnSlice {
	log.Printf("[TRACE] GetUnsatisfiedKeyColumns for columns %v", columns)

	if columns == nil {
		return nil
	}
	var unsatisfiedKeyColumns KeyColumnSlice
	satisfiedMap := map[string]KeyColumnSlice{
		Required: {},
		AnyOf:    {},
		Optional: {},
	}
	unsatisfiedMap := map[string]KeyColumnSlice{
		Required: {},
		AnyOf:    {},
		Optional: {},
	}

	for _, keyColumn := range columns {
		// look for this key column in our map
		k := m[keyColumn.Name]
		satisfied := k != nil && k.SatisfiesKeyColumn(keyColumn)
		if satisfied {
			satisfiedMap[keyColumn.Require] = append(satisfiedMap[keyColumn.Require], keyColumn)
			log.Printf("[TRACE] key column satisfied %v", keyColumn)

		} else {
			unsatisfiedMap[keyColumn.Require] = append(unsatisfiedMap[keyColumn.Require], keyColumn)
			log.Printf("[TRACE] key column NOT satisfied %v", keyColumn)
		}
	}

	// we are satisfied if:
	// all Required key columns are satisfied
	// either there is at least 1 satisfied AnyOf key columns, or there are no AnyOf columns
	anyOfSatisfied := len(satisfiedMap[AnyOf]) > 0 || len(unsatisfiedMap[AnyOf]) == 0
	if !anyOfSatisfied {
		unsatisfiedKeyColumns = unsatisfiedMap[AnyOf]
	}
	// if any 'required' are unsatisfied, we are unsatisfied
	requiredSatisfied := len(unsatisfiedMap[Required]) == 0
	if !requiredSatisfied {
		unsatisfiedKeyColumns = append(unsatisfiedKeyColumns, unsatisfiedMap[Required]...)
	}

	log.Printf("[TRACE] satisfied: %v", satisfiedMap)
	log.Printf("[TRACE] unsatisfied: %v", unsatisfiedMap)
	log.Printf("[TRACE] unsatisfied required KeyColumns: %v", unsatisfiedKeyColumns)

	return unsatisfiedKeyColumns
}

// ToQualMap converts the map into a simpler map of column to []Quals
// this is used in the TraansformData
// (needed to avoid the transform package needing to reference plugin)
func (m KeyColumnQualMap) ToQualMap() map[string]quals.QualSlice {
	var res = make(map[string]quals.QualSlice)
	for k, v := range m {
		res[k] = v.Quals
	}
	return res
}

// ToQualMap converts the map into a  map of column to *proto.Quals
// used for cache indexes
func (m KeyColumnQualMap) ToProtoQualMap() map[string]*proto.Quals {
	var res = make(map[string]*proto.Quals)
	for k, v := range m {
		res[k] = v.Quals.ToProto()
	}
	return res
}

// GetListQualValues returns a slice of any quals we have which have a list value
func (m KeyColumnQualMap) GetListQualValues() quals.QualSlice {
	var res quals.QualSlice
	for _, qualsForColumn := range m {
		for _, qual := range qualsForColumn.Quals {
			if qualValueList := qual.Value.GetListValue(); qualValueList != nil {
				res = append(res, qual)
			}
		}
	}
	return res
}

// NewKeyColumnQualValueMap creates a KeyColumnQualMap from a qual map and a KeyColumnSlice
func NewKeyColumnQualValueMap(qualMap map[string]*proto.Quals, keyColumns KeyColumnSlice) KeyColumnQualMap {
	res := KeyColumnQualMap{}

	// find which of the provided quals match the key columns
	for _, keyColumn := range keyColumns {
		matchingQuals := getMatchingQuals(keyColumn, qualMap)

		for _, q := range matchingQuals {
			// convert proto.Qual into a qual.Qual (which is easier to use)
			qual := quals.NewQual(q)

			// if there is already an entry for this column, add a value to the array
			if mapEntry, mapEntryExists := res[keyColumn.Name]; mapEntryExists {
				log.Printf("[TRACE] NewKeyColumnQualValueMap entry exists for col %s", keyColumn.Name)
				// check whether we have this value in the list of quals yet
				if !mapEntry.Quals.Contains(qual) {
					log.Printf("[TRACE] this qual not found - adding %+v", qual)
					mapEntry.Quals = append(mapEntry.Quals, qual)
					res[keyColumn.Name] = mapEntry
				}
			} else {
				// create a new map entry for this column
				res[keyColumn.Name] = &KeyColumnQuals{
					Name:  keyColumn.Name,
					Quals: quals.QualSlice{qual},
				}
			}
		}
	}

	return res
}

// look in a column-qual map for quals with column and operator matching the key column
func getMatchingQuals(keyColumn *KeyColumn, qualMap map[string]*proto.Quals) []*proto.Qual {
	log.Printf("[TRACE] getMatchingQuals %s", keyColumn)

	columnQuals, ok := qualMap[keyColumn.Name]
	if !ok {
		log.Printf("[TRACE] getMatchingQuals returning false - qualMap does not contain any quals for column %s", keyColumn.Name)
		return nil
	}

	var res []*proto.Qual
	for _, q := range columnQuals.Quals {
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
