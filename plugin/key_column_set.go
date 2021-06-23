package plugin

import (
	"fmt"
	"strings"
)

// KeyColumnSet is a set of columns which form the key of a table (i.e. may be used to get a single item)
// may specify:
// - a Single column
// - a set of columns which together All form the key
// - a set of columns Any of which which form the key
type KeyColumnSet struct {
	Single *KeyColumn
	All    []*KeyColumn
	Any    []*KeyColumn
}

func (k *KeyColumnSet) ToString() string {
	if k.Single != nil {
		return fmt.Sprintf("column: %s", k.Single)
	}
	if k.All != nil {
		return fmt.Sprintf("all columns: %s", strings.Join(k.All, ","))
	}
	if k.Any != nil {
		return fmt.Sprintf("one of columns: %s", strings.Join(k.Any, ","))
	}
	return ""
}

func SingleColumn(column string) *KeyColumnSet {
	return &KeyColumnSet{Single: &KeyColumn{Name: column, Operators: []Operator{OperatorEq}}}
}

func AllColumns(columns []string) *KeyColumnSet {
	return &KeyColumnSet{All: columns}
}

func AnyColumn(columns []string) *KeyColumnSet {
	return &KeyColumnSet{Any: columns}
}

key_columns_should_allow_specifying_supported_operators_121