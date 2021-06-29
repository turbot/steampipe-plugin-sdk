package plugin

import (
	"fmt"
	"strings"

	"github.com/turbot/go-kit/helpers"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// KeyColumnQuals is a struct representing all quals for a specific column
type KeyColumnQuals struct {
	Column string
	Quals  []*Qual
}

func (k KeyColumnQuals) SatisfiesKeyColumn(keyColumn *KeyColumn) bool {
	if keyColumn.Column != k.Column {
		return false
	}
	for _, q := range k.Quals {
		if helpers.StringSliceContains(keyColumn.Operators, q.Operator) {
			return true
		}
	}
	return false
}

func (k KeyColumnQuals) SingleEqualsQual() bool {
	return len(k.Quals) == 1 && k.Quals[0].Operator == "="
}

// KeyColumnQualValueMap is a map of KeyColumnQuals keyed by column name
type KeyColumnQualValueMap map[string]*KeyColumnQuals

// ToValueMap converts a KeyColumnQualValueMap to a column-qual value map
func (m KeyColumnQualValueMap) ToValueMap() map[string]*proto.QualValue {
	res := make(map[string]*proto.QualValue, len(m))
	// TODO
	return res
}

func (m KeyColumnQualValueMap) String() string {
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

func (m KeyColumnQualValueMap) SatisfiesKeyColumns(columns *KeyColumnSet) bool {
	if columns == nil {
		return true
	}
	if columns.Single != nil {
		k := m[columns.Single.Column]
		return k != nil && k.SatisfiesKeyColumn(columns.Single)
	}
	if columns.Any != nil {
		for _, c := range columns.Any {
			k := m[c.Column]
			if k != nil && k.SatisfiesKeyColumn(c) {
				return true
			}
		}
	}
	if columns.All != nil {
		for _, c := range columns.Any {
			k := m[c.Column]
			if k == nil || !k.SatisfiesKeyColumn(c) {
				return false
			}
		}
	}
	return true

}

// NewKeyColumnQualValueMap creates a KeyColumnQualValueMap from one or more KeyColumnSets
func NewKeyColumnQualValueMap(qualMap map[string]*proto.Quals, keyColumnSets ...*KeyColumnSet) KeyColumnQualValueMap {
	res := KeyColumnQualValueMap{}
	for _, keyColumns := range keyColumnSets {
		// extract the key columns slice from the set
		slice := keyColumns.ToKeyColumnSlice()
		for _, col := range slice {
			matchingQuals := getMatchingQuals(col, qualMap)
			for _, q := range matchingQuals {
				// convert protobuf qual into a plugin.Qual
				qual := NewQual(q)

				// if there is already an entry for this column, add a value to the array
				if mapEntry, mapEntryExists := res[col.Column]; mapEntryExists {
					mapEntry.Quals = append(mapEntry.Quals, qual)
					res[col.Column] = mapEntry
				} else {
					// crate a new map entry for this column
					res[col.Column] = &KeyColumnQuals{
						Column: col.Column,
						Quals:  []*Qual{qual},
					}
				}
			}

		}
	}
	return res
}
