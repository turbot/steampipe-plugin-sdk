package plugin

import (
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v2/grpc/proto"
)

type KeyColumnSlice []*KeyColumn

func (k KeyColumnSlice) String() string {
	return strings.Join(k.StringSlice(), "\n")
}

// StringSlice converts a KeyColumnSlice to a slice of strings
func (k KeyColumnSlice) StringSlice() []string {
	strs := make([]string, len(k))
	for i, c := range k {
		strs[i] = c.String()
	}
	return strs
}

// ToProtobuf converts the KeyColumnSlice to a slice of protobuf KeyColumns
func (k KeyColumnSlice) ToProtobuf() []*proto.KeyColumn {
	var res = make([]*proto.KeyColumn, len(k))
	for i, col := range k {
		res[i] = col.ToProtobuf()
	}
	return res
}

// AllEquals returns whether all KeyColumns only use equals operators
func (k KeyColumnSlice) AllEquals() bool {
	for _, col := range k {
		if !col.SingleEqualsQual() {
			return false
		}
	}

	return true
}

// IsAnyOf returns whether all key columns have Require == AnyOf
func (k KeyColumnSlice) IsAnyOf() bool {
	for _, kc := range k {
		if kc.Require != AnyOf {
			return false
		}
	}
	return true
}

// Validate validates all child columns
func (k KeyColumnSlice) Validate() []string {
	var res []string
	for _, col := range k {
		res = append(res, col.Validate()...)
	}
	return res
}

// Find looks for a key column with the given name and returns it if found
func (k *KeyColumnSlice) Find(name string) *KeyColumn {
	for _, keyColumn := range *k {
		if keyColumn.Name == name {
			return keyColumn
		}
	}
	return nil
}
