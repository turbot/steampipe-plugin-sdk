package plugin

import (
	"log"

	"github.com/turbot/steampipe-plugin-sdk/plugin/option"
)

// helper functions to create key column sets

// NewKeyColumnSet builds a KeyColumnSet from a list of KeyColumn and optional KeyColumnSetOptions
func NewKeyColumnSet(keyColumns []*KeyColumn, options ...option.KeyColumnSetOptions) *KeyColumnSet {
	res := &KeyColumnSet{Columns: keyColumns}

	// if options were passed, set them
	for _, option := range options {
		res.SetOption(option)
	}
	return res
}

// SingleColumn creates a KeyColumnSet based on a column name
// The created set has a single KeyColumn using a single equals operator and Optional=false
func SingleColumn(column string) *KeyColumnSet {
	res := &KeyColumnSet{
		Columns: []*KeyColumn{
			{
				Column:    column,
				Operators: []string{"="},
				Optional:  false,
			},
		}}
	log.Printf("[WARN] SingleColumn returning %v", res)
	return res
}

// AllColumns creates a KeyColumnSet based on a slice of column names,
// All columns have a single equals operator and Optional=false
func AllColumns(columns []string) *KeyColumnSet {
	res := &KeyColumnSet{Columns: NewEqualsKeyColumnSlice(columns, false)}
	log.Printf("[WARN] AllColumns returning %v", res)
	return res
}

// OptionalColumns Columns creates a KeyColumnSet based on a slice of column names,
// All columns have a single equals operator and Optional=false
func OptionalColumns(columns []string) *KeyColumnSet {
	return &KeyColumnSet{Columns: NewEqualsKeyColumnSlice(columns, false)}
}

// AnyColumn Columns creates a KeyColumnSet based on a slice of column names,
// All columns have a single equals operator and Optional=false
// The column set has Minimu=1, meaning at least one satisfied qual is required
func AnyColumn(columns []string) *KeyColumnSet {
	res := &KeyColumnSet{Columns: NewEqualsKeyColumnSlice(columns, true), Minimum: 1}
	log.Printf("[WARN] AnyColumn returning %v", res)
	return res
}
