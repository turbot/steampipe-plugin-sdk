package plugin

import (
	"fmt"
	"strings"

	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
	"github.com/turbot/steampipe-plugin-sdk/v5/query_cache"
)

const (
	// Require values
	Required = "required"
	Optional = "optional"
	AnyOf    = "any_of"
)

// KeyColumn is a struct representing the definition of a column used to filter and Get and List calls.
//
// At least one key column must be defined for a Get call. They are optional for List calls.
//
// # Operators
//
// This property specifies the accepted operators (from a possible set: "=", "<>", "<", "<=", ">", ">=")
//
// # Require
//
// This property determines whether the column is required or optional. Possible values:
//
// "required"
//
// The key column must be provided as a query qualifier (i.e. in a where clause in the query).
//
// "optional"
//
// The key column is optional but if provided it will be used to filter the results.
//
// "any_of"
//
// Any one of the given columns must be provided.
//
// # CacheMatch
//
// This property determines the logic used by the query results cache to determine whether a cached value
// matches a given query. Possible values:
//
// "subset" [default value]
//
// A cached item is considered a match (i.e. a cache hit) if the qual for the
// query is a subset of the quals for the cached item.
//
// For example, is the cached qual is "val < 100", and the query qual is "val < 50",
// this would be considered a qual subset so would lead to a cache match.
//
// "exact"
//
// A cached item is considered a match ONLY if the qual for the cached item is the same as as the qual for the query.
//
// This is used for columns which are only populated if the qual for that column is passed.
// A common pattern is to provide a "filter" column, which is populated using the qual value provided.
// This filter value is used when making the API call the fetch the data. If no filter qual is provided,
// then the filter column returned by the plugin is empty.
//
// This breaks the subset logic as if there is a cached data with no qual for the filter column,
// this cached data would contain null values for the filter column.
// This data would be considered a superset of the data returned from a query which provides a filter qual,
// which is incorrect as the data returned if a filter qual is passed would include a non null filter column.
//
// The solution is to set CacheMatch="exact".
//
// Plugin examples:
//  - [salesforce]
//
// [salesforce]: https://github.com/turbot/steampipe-plugin-salesforce/blob/bd563985a57c6a5dc526d5e4c701f37e017270cd/salesforce/utils.go#L297-L319
type KeyColumn struct {
	Name       string
	Operators  []string
	Require    string
	CacheMatch string
}

func (k KeyColumn) String() string {
	return fmt.Sprintf("column:'%s' %s: %s", k.Name, pluralize.NewClient().Pluralize("operator", len(k.Operators), false), strings.Join(k.Operators, ","))
}

// ToProtobuf converts the KeyColumn to a protobuf object.
func (k *KeyColumn) ToProtobuf() *proto.KeyColumn {
	return &proto.KeyColumn{
		Name:       k.Name,
		Operators:  k.Operators,
		Require:    k.Require,
		CacheMatch: k.CacheMatch,
	}
}

// SingleEqualsQual returns whether this key column has a single = operator.
func (k *KeyColumn) SingleEqualsQual() bool {
	return len(k.Operators) == 1 && k.Operators[0] == "="
}

// InitialiseOperators adds a default '=' operator is no operators are set, and converts "!=" to "<>".
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

// Validate ensures 'Operators' and 'Require' are valid. It returns a list of validation error strings.
func (k *KeyColumn) Validate() []string {
	// first set default operator and convert "!=" to "<>"
	k.InitialiseOperators()
	// ensure operators are valid
	validOperators := []string{"=", "<>", "<", "<=", ">", ">=", quals.QualOperatorIsNull, quals.QualOperatorIsNotNull}
	validRequire := []string{Required, Optional, AnyOf}
	validCacheMatch := []string{query_cache.CacheMatchSubset, query_cache.CacheMatchExact, ""}
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
		k.CacheMatch = query_cache.CacheMatchSubset
	}
	if !helpers.StringSliceContains(validCacheMatch, k.CacheMatch) {
		res = append(res, fmt.Sprintf("CacheMatch value '%s' is not valid, it must be one of: %s", k.CacheMatch, strings.Join(validCacheMatch, ",")))
	}

	return res
}
