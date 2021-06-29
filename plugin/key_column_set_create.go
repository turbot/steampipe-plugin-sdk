package plugin

// helper functions to create key column sets

// SingleColumn creates a KeyColumnSet based on a column name
// The created set has a 'Single' KeyColumn using equals operator
func SingleColumn(column string) *KeyColumnSet {
	return &KeyColumnSet{Single: &KeyColumn{Column: column, Operators: []string{"="}}}
}

// AllColumns creates a KeyColumnSet based on a slice of column names,
// The created set has an 'All' KeyColumnSlice using equals operator
func AllColumns(columns []string) *KeyColumnSet {
	return &KeyColumnSet{All: NewEqualsKeyColumnSlice(columns)}
}

// AnyColumn creates a KeyColumnSet with an 'Any' KeyColumnSlice using equals operator
func AnyColumn(columns []string) *KeyColumnSet {
	return &KeyColumnSet{Any: NewEqualsKeyColumnSlice(columns)}
}

// SingleKeyColumn creates a 'Single' KeyColumnSet based on the passed in KeyColumn
func SingleKeyColumn(keyColumn *KeyColumn) *KeyColumnSet {
	return &KeyColumnSet{Single: keyColumn}
}

// AllKeyColumns creates a, 'All' KeyColumnSet based on the passed in KeyColumn
func AllKeyColumns(keyColumns KeyColumnSlice) *KeyColumnSet {
	return &KeyColumnSet{All: keyColumns}
}

// AnyKeyColumn creates a, 'All' KeyColumnSet based on the passed in KeyColumn
func AnyKeyColumn(keyColumns KeyColumnSlice) *KeyColumnSet {
	return &KeyColumnSet{Any: keyColumns}
}
