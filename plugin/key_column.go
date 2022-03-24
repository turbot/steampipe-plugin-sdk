package plugin

import (
	"fmt"
	"strings"

	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/cache"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/quals"
)

const (
	// Require values
	Required = "required"
	Optional = "optional"
	AnyOf    = "any_of"
)

// KeyColumn is a struct representing the definition of a KeyColumn used to filter and Get/List call
type KeyColumn struct {
	Name       string
	Operators  []string
	Require    string
	CacheMatch string
}

func (k KeyColumn) String() string {
	return fmt.Sprintf("column:'%s' %s: %s", k.Name, pluralize.NewClient().Pluralize("operator", len(k.Operators), false), strings.Join(k.Operators, ","))
}

// ToProtobuf converts the KeyColumn to a protobuf object
func (k *KeyColumn) ToProtobuf() *proto.KeyColumn {
	return &proto.KeyColumn{
		Name:       k.Name,
		Operators:  k.Operators,
		Require:    k.Require,
		CacheMatch: k.CacheMatch,
	}
}

// SingleEqualsQual returns whether this key column has a single = operator
func (k *KeyColumn) SingleEqualsQual() bool {
	return len(k.Operators) == 1 && k.Operators[0] == "="
}

// InitialiseOperators adds a default '=' operator is no operators are set, and converts "!=" to "<>"
func (k *KeyColumn) InitialiseOperators() {
	// if there are no operators, add a single equals operator
	if len(k.Operators) == 0 {
		k.Operators = []string{"="}
		return

	}
	for i, op := range k.Operators {
		// convert "!=" to "<>"
		if op == "!=" {
			k.Operators[i] = "<>"
		}
	}
}

// Validate ensures 'Operators' and 'Require' are valid
func (k *KeyColumn) Validate() []string {
	// first set default operator and convert "!=" to "<>"
	k.InitialiseOperators()
	// ensure operators are valid
	validOperators := []string{"=", "<>", "<", "<=", ">", ">=", quals.QualOperatorIsNull, quals.QualOperatorIsNotNull}
	validRequire := []string{Required, Optional, AnyOf}
	validCacheMatch := []string{cache.CacheMatchSubset, cache.CacheMatchExact, ""}
	var res []string

	for _, op := range k.Operators {
		if !helpers.StringSliceContains(validOperators, op) {
			res = append(res, fmt.Sprintf("operator %s is not valid, it must be one of: %s", op, strings.Join(validOperators, ",")))
		}
	}

	// default Require to Required
	if k.Require == "" {
		k.Require = Required
	}
	if !helpers.StringSliceContains(validRequire, k.Require) {
		res = append(res, fmt.Sprintf("Require value '%s' is not valid, it must be one of: %s", k.Require, strings.Join(validRequire, ",")))
	}

	// default CacheMatch to subset
	if k.CacheMatch == "" {
		k.CacheMatch = cache.CacheMatchSubset
	}
	if !helpers.StringSliceContains(validCacheMatch, k.CacheMatch) {
		res = append(res, fmt.Sprintf("CacheMatch value '%s' is not valid, it must be one of: %s", k.CacheMatch, strings.Join(validCacheMatch, ",")))
	}

	return res
}
