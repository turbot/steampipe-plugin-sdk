package plugin

import (
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
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

// SingleEqualsQual determines whether this key column slice has a single qual with a single = operator
// and if so returns it
func (k KeyColumnSlice) SingleEqualsQual() *KeyColumn {
	if len(k) == 1 && k[0].SingleEqualsQual() {
		return k[0]
	}
	return nil
}

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
