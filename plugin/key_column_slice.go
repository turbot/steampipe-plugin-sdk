package plugin

import (
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type KeyColumnSlice []*KeyColumn

// NewEqualsKeyColumnSlice creates a KeyColumnSlice from a list of column names.
// All KeyColumns default to use equals operator
func NewEqualsKeyColumnSlice(columns []string) KeyColumnSlice {
	var all = make([]*KeyColumn, len(columns))
	for i, c := range columns {
		all[i] = &KeyColumn{Column: c, Operators: []string{"="}}
	}
	return all
}

func (k KeyColumnSlice) String() string {
	return strings.Join(k.StringSlice(), "\n")
}

// StringSlice converts a KeyColumnSlice to a slice of strings
func (k KeyColumnSlice) StringSlice() []string {
	strs := make([]string, len(k))
	for i, s := range k {
		strs[i] = s.String()
	}
	return strs
}

func (k KeyColumnSlice) ToProtobuf() []*proto.KeyColumn {
	var res = make([]*proto.KeyColumn, len(k))
	for i, col := range k {
		res[i] = col.ToProtobuf()
	}
	return res
}

// AllEquals returns whether all child KeyColumns only use equals operators
func (k KeyColumnSlice) AllEquals() bool {
	for _, col := range k {
		if !col.SingleEqualsQual() {
			return false
		}
	}

	return true
}

func (k KeyColumnSlice) Validate() []string {
	var res []string
	for _, col := range k {
		res = append(res, col.Validate()...)
	}
	return res
}
