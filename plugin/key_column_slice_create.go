package plugin

// helper functions to create key column slices

// SingleColumn creates a KeyColumnSlice based on a column name
// The created slice has a single KeyColumn using a single equals operator and Require=Required
func SingleColumn(column string) KeyColumnSlice {
	return []*KeyColumn{
		{
			Name:      column,
			Operators: []string{"="},
			Require:   Required,
		},
	}
}

// AllColumns creates a KeyColumnSlice based on a slice of column names,
// each with a single equals operator and Require=Required
func AllColumns(columns []string) KeyColumnSlice {
	return NewEqualsKeyColumnSlice(columns, Required)
}

// OptionalColumns Columns creates a KeyColumnSlice based on a slice of column names,
// with a single equals operator and Require=Optional
func OptionalColumns(columns []string) KeyColumnSlice {
	return NewEqualsKeyColumnSlice(columns, Optional)
}

// AnyColumn Columns creates a KeyColumnSlice based on a slice of column names,
// each with a single equals operator and Require=AnyOf
func AnyColumn(columns []string) KeyColumnSlice {
	return NewEqualsKeyColumnSlice(columns, AnyOf)
}

// NewEqualsKeyColumnSlice creates a KeyColumnSlice from a list of column names,
// each with a single equals operator
func NewEqualsKeyColumnSlice(columns []string, require string) KeyColumnSlice {
	var all = make([]*KeyColumn, len(columns))
	for i, c := range columns {
		all[i] = &KeyColumn{
			Name:      c,
			Operators: []string{"="},
			Require:   require,
		}
	}
	return all
}
