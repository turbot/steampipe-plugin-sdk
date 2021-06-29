package plugin

import (
	"fmt"
	"strings"

	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// KeyColumn is a struct representing the definition of a KeyColumn used to filter and Get/List call
type KeyColumn struct {
	Column    string
	Operators []string
}

func (k KeyColumn) String() string {
	return fmt.Sprintf("column:'%s' %s: %s", k.Column, pluralize.NewClient().Pluralize("operator", len(k.Operators), false), strings.Join(k.Operators, ","))
}

// ToProtobuf converts the KeyColumn to a protobuf object
func (k *KeyColumn) ToProtobuf() *proto.KeyColumn {
	return &proto.KeyColumn{Name: k.Column}
}

// SingleEqualsQual returns whether this key column has a single = operator
func (k *KeyColumn) SingleEqualsQual() bool {
	return len(k.Operators) == 1 && k.Operators[0] == "="
}

func (k *KeyColumn) Validate() []string {
	// ensure operators are valid

	// map "!=" operator to "<>"
	validOperators := []string{"=", "<>", "<", "<=", ">", ">="}
	var res []string

	for _, op := range k.Operators {
		// convert "!=" to "<>"
		if op == "!=" {
			op = "<>"
		}
		if !helpers.StringSliceContains(validOperators, op) {
			res = append(res, fmt.Sprintf("operator %s is not valid, it must be one of: %s", op, strings.Join(validOperators, ",")))
		}
	}
	return res
}
