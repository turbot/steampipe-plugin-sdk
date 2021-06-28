package plugin

import (
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/grpc"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// KeyColumnQualValue is a struct representing a KeyColumQual with its specified value
type KeyColumnQualValue struct {
	KeyColumn *KeyColumn
	Values    []*proto.QualValue
}

// KeyColumnQualValueMap is a map of KeyColumnQualValue keyed by column name
type KeyColumnQualValueMap map[string]*KeyColumnQualValue

// ToValueMap converts a KeyColumnQualValueMap to a column-qual value map
func (m KeyColumnQualValueMap) ToValueMap() map[string]*proto.QualValue {
	res := make(map[string]*proto.QualValue, len(m))
	// TODO
	return res
}

func (m KeyColumnQualValueMap) String() string {
	strs := make([]string, len(m))
	for _, k := range m {
		var values = make([]interface{}, len(k.Values))
		for i, v := range k.Values {
			values[i] = grpc.GetQualValue(v)
		}
		strs = append(strs, fmt.Sprintf("%s - %v", k.KeyColumn, values))
	}
	return strings.Join(strs, "\n")
}

func NewKeyColumnQualValueMap(qualMap map[string]*proto.Quals, keyColumns KeyColumnSlice) KeyColumnQualValueMap {
	res := KeyColumnQualValueMap{}
	for _, col := range keyColumns {
		qual, foundQual := qualExists(col, qualMap)
		if foundQual {
			// if there is already an entry for this column, add a value to the array
			if mapEntry, mapEntryExists := res[col.Name]; mapEntryExists {
				mapEntry.Values = append(mapEntry.Values, qual.Value)
				res[col.Name] = mapEntry
			} else {
				// crate a new map entry for this column
				res[col.Name] = &KeyColumnQualValue{
					KeyColumn: col,
					Values:    []*proto.QualValue{qual.Value},
				}
			}
		}
	}
	return res
}
