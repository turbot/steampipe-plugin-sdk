package plugin

import (
	"fmt"
	"strings"
)

const (
	OperatorEq = "=="
	OperatorNe = "<>"
	OperatorLt = "<"
	OperatorLe = "<="
	OperatorGt = ">"
	OperatorGe = ">="
)

type KeyColumn struct {
	Name      string
	Operators []string
}

func (k KeyColumn) String() string {
	return fmt.Sprintf("%s [%s]", k.Name, strings.Join(k.Operators, ","))
}

type KeyColumnSlice []*KeyColumn

func NewKeyColumnSlice(columns []string) KeyColumnSlice {
	var all = make([]*KeyColumn, len(columns))
	for i, c := range columns {
		all[i] = &KeyColumn{Name: c, Operators: []string{OperatorEq}}
	}
	return all
}

func (k KeyColumnSlice) String() string {

}
